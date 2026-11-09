//! Running compiled PL/pgSQL functions (decision D13): components of the `ferrosa:plpgsql`
//! `routine` world (`src/wit/plpgsql.wit`), under the same sandbox as a CQL UDF.
//!
//! The component imports the host interface; each import is forwarded to a
//! [`ferrosa_plpgsql::Host`] the caller hands over for the duration of one call, so the
//! function's queries run inside the caller's transaction. Values are converted at the ABI
//! boundary only (`Arc<str>` text to the owned `String` the canonical ABI lifts into).
//!
//! Sandbox: the executor's engine (fuel, epoch interruption), `[udf] max_memory_bytes` via the
//! same [`GuestState`](crate::sandbox::GuestState) limiter, and per-call fuel `max_fuel`. The wall-clock limit counts
//! **guest** time only: the time spent inside host calls (the SQL the function runs) is
//! subtracted, so a slow query is not mistaken for a runaway function.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_plpgsql::{Call, Host, SqlError, TriggerCall, Value};
use wasmtime::component::types::ComponentFunc;
use wasmtime::component::{Component, Linker, Val};
use wasmtime::{Store, StoreContextMut, UpdateDeadline};

use crate::sandbox::GuestState;
use crate::{UdfError, UdfExecutor};

// The `routine` world is lowered and lifted through the dynamic `Val` API, the way the
// `udf` world's `cql-value` is (see `executor.rs`), rather than through
// `wasmtime::component::bindgen!`: the generated bindings are a macro expansion over
// wasmtime internals that nothing downstream can read or extract. The names below are the
// WIT contract in `src/wit/plpgsql.wit`; a component that disagrees fails to link.

/// The host interface the component imports.
const HOST_INTERFACE: &str = "ferrosa:plpgsql/host@0.1.0";
/// The world's one export.
const CALL_EXPORT: &str = "call";

/// A compiled PL/pgSQL component, ready to instantiate. Shared (`Arc`) across sessions: the
/// front end caches one per function source hash.
pub struct PlpgsqlComponent {
    component: Component,
}

/// The host a call runs against, boxed so it can live in the wasm store for one call and be
/// handed back afterwards.
pub type BoxedHost = Box<dyn Host + Send>;

struct CallState {
    guest: GuestState,
    host: BoxedHost,
    /// Time spent inside host calls, subtracted from the guest-time limit.
    host_time: Duration,
    /// When the call started, for the guest-time limit.
    started: Instant,
    /// The guest-time limit (`max_execution_time`).
    limit: Duration,
}

impl CallState {
    /// Count the time since `started` as host time (the SQL a call ran, not the guest).
    fn charge(&mut self, started: Instant) {
        self.host_time += started.elapsed();
    }
}

// The glue below is written without closures: each closure is an item the Aeneas extraction
// cannot translate, so plain matches and named functions keep this file inside the gap
// inventory instead of adding to it.

// ---------------------------------------------------------------- WIT values <-> Val

/// A value's text, lowered (ABI boundary: the canonical ABI lowers an owned String).
fn text_to_val(text: Arc<str>) -> Box<Val> {
    Box::new(Val::String(text.to_string()))
}

/// `record value { type-oid: u32, text: option<string> }`.
fn value_to_val(value: Value) -> Val {
    let text = value.text.map(text_to_val);
    Val::Record(vec![
        ("type-oid".to_string(), Val::U32(value.type_oid)),
        ("text".to_string(), Val::Option(text)),
    ])
}

fn values_to_val(values: Vec<Value>) -> Val {
    let mut items = Vec::with_capacity(values.len());
    for value in values {
        items.push(value_to_val(value));
    }
    Val::List(items)
}

/// `option<list<value>>`.
fn row_to_val(row: Option<Vec<Value>>) -> Val {
    match row {
        Some(values) => Val::Option(Some(Box::new(values_to_val(values)))),
        None => Val::Option(None),
    }
}

/// `record sql-error { sqlstate: string, message: string }`.
fn error_to_val(error: SqlError) -> Val {
    Val::Record(vec![
        ("sqlstate".to_string(), Val::String(error.sqlstate)),
        ("message".to_string(), Val::String(error.message)),
    ])
}

/// `result<T, sql-error>` from an already lowered success payload (`None` for `_`).
fn result_to_val(result: Result<Option<Val>, SqlError>) -> Val {
    match result {
        Ok(Some(value)) => Val::Result(Ok(Some(Box::new(value)))),
        Ok(None) => Val::Result(Ok(None)),
        Err(error) => Val::Result(Err(Some(Box::new(error_to_val(error))))),
    }
}

/// A lifted value that does not have the WIT shape. Wasmtime type-checks the component
/// against the import types, so this is a host/contract mismatch: it traps, loudly.
fn shape(expected: &str, got: &Val) -> wasmtime::Error {
    wasmtime::Error::msg(format!("PL/pgSQL ABI: expected {expected}, got {got:?}"))
}

fn field<'v>(fields: &'v [(String, Val)], name: &str) -> wasmtime::Result<&'v Val> {
    for (field_name, value) in fields {
        if field_name == name {
            return Ok(value);
        }
    }
    Err(wasmtime::Error::msg(format!(
        "PL/pgSQL ABI: record lacks `{name}`"
    )))
}

fn val_string(val: &Val) -> wasmtime::Result<String> {
    match val {
        Val::String(s) => Ok(s.clone()), // lifted values are borrowed from the call frame
        other => Err(shape("string", other)),
    }
}

fn val_u32(val: &Val) -> wasmtime::Result<u32> {
    match val {
        Val::U32(n) => Ok(*n),
        other => Err(shape("u32", other)),
    }
}

fn val_to_value(val: &Val) -> wasmtime::Result<Value> {
    let Val::Record(fields) = val else {
        return Err(shape("record value", val));
    };
    let text = match field(fields, "text")? {
        Val::Option(None) => None,
        Val::Option(Some(inner)) => Some(Arc::from(val_string(inner)?)),
        other => return Err(shape("option<string>", other)),
    };
    Ok(Value {
        type_oid: val_u32(field(fields, "type-oid")?)?,
        text,
    })
}

fn val_to_values(val: &Val) -> wasmtime::Result<Vec<Value>> {
    let Val::List(items) = val else {
        return Err(shape("list<value>", val));
    };
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        values.push(val_to_value(item)?);
    }
    Ok(values)
}

fn val_to_error(val: &Val) -> wasmtime::Result<SqlError> {
    let Val::Record(fields) = val else {
        return Err(shape("record sql-error", val));
    };
    Ok(SqlError::new(
        &val_string(field(fields, "sqlstate")?)?,
        val_string(field(fields, "message")?)?,
    ))
}

/// `option<trigger-data>`.
fn trigger_to_val(trigger: Option<TriggerCall>) -> Val {
    let Some(TriggerCall {
        op,
        new_row,
        old_row,
    }) = trigger
    else {
        return Val::Option(None);
    };
    Val::Option(Some(Box::new(Val::Record(vec![
        ("op".to_string(), Val::String(op)),
        ("new-row".to_string(), row_to_val(new_row)),
        ("old-row".to_string(), row_to_val(old_row)),
    ]))))
}

/// The export's `result<option<value>, sql-error>`.
fn val_to_outcome(val: &Val) -> wasmtime::Result<Result<Option<Value>, SqlError>> {
    match val {
        Val::Result(Ok(Some(inner))) => match inner.as_ref() {
            Val::Option(None) => Ok(Ok(None)),
            Val::Option(Some(value)) => Ok(Ok(Some(val_to_value(value)?))),
            other => Err(shape("option<value>", other)),
        },
        Val::Result(Err(Some(error))) => Ok(Err(val_to_error(error)?)),
        other => Err(shape("result<option<value>, sql-error>", other)),
    }
}

// ---------------------------------------------------------------- the host interface

/// Store `val` as the single result of a host function.
fn set_result(results: &mut [Val], val: Val) -> wasmtime::Result<()> {
    match results.first_mut() {
        Some(slot) => {
            *slot = val;
            Ok(())
        }
        None => Err(wasmtime::Error::msg("PL/pgSQL ABI: no result slot")),
    }
}

fn param(params: &[Val], index: usize) -> wasmtime::Result<&Val> {
    match params.get(index) {
        Some(val) => Ok(val),
        None => Err(wasmtime::Error::msg(format!(
            "PL/pgSQL ABI: missing argument {index}"
        ))),
    }
}

/// `query: func(sql: string, params: list<value>) -> result<u32, sql-error>`.
fn host_query(
    mut store: StoreContextMut<'_, CallState>,
    _ty: ComponentFunc,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let sql = val_string(param(params, 0)?)?;
    let args = val_to_values(param(params, 1)?)?;
    let state = store.data_mut();
    let started = Instant::now();
    let outcome = state.host.query(&sql, &args);
    state.charge(started);
    let lowered = match outcome {
        Ok(cursor) => Ok(Some(Val::U32(cursor))),
        Err(error) => Err(error),
    };
    set_result(results, result_to_val(lowered))
}

/// `fetch: func(cursor: u32, max: u32) -> result<list<list<value>>, sql-error>`.
fn host_fetch(
    mut store: StoreContextMut<'_, CallState>,
    _ty: ComponentFunc,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let cursor = val_u32(param(params, 0)?)?;
    let max = val_u32(param(params, 1)?)?;
    let state = store.data_mut();
    let started = Instant::now();
    let outcome = state.host.fetch(cursor, max);
    state.charge(started);
    let lowered = match outcome {
        Ok(rows) => {
            let mut items = Vec::with_capacity(rows.len());
            for row in rows {
                items.push(values_to_val(row));
            }
            Ok(Some(Val::List(items)))
        }
        Err(error) => Err(error),
    };
    set_result(results, result_to_val(lowered))
}

/// `close: func(cursor: u32)`.
fn host_close(
    mut store: StoreContextMut<'_, CallState>,
    _ty: ComponentFunc,
    params: &[Val],
    _results: &mut [Val],
) -> wasmtime::Result<()> {
    let cursor = val_u32(param(params, 0)?)?;
    let state = store.data_mut();
    let started = Instant::now();
    state.host.close(cursor);
    state.charge(started);
    Ok(())
}

/// `execute: func(sql: string, params: list<value>) -> result<u64, sql-error>`.
fn host_execute(
    mut store: StoreContextMut<'_, CallState>,
    _ty: ComponentFunc,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let sql = val_string(param(params, 0)?)?;
    let args = val_to_values(param(params, 1)?)?;
    let state = store.data_mut();
    let started = Instant::now();
    let outcome = state.host.execute(&sql, &args);
    state.charge(started);
    let lowered = match outcome {
        Ok(count) => Ok(Some(Val::U64(count))),
        Err(error) => Err(error),
    };
    set_result(results, result_to_val(lowered))
}

/// `notify: func(channel: string, payload: string) -> result<_, sql-error>`.
fn host_notify(
    mut store: StoreContextMut<'_, CallState>,
    _ty: ComponentFunc,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let channel = val_string(param(params, 0)?)?;
    let payload = val_string(param(params, 1)?)?;
    let state = store.data_mut();
    let started = Instant::now();
    let outcome = state.host.notify(&channel, &payload);
    state.charge(started);
    let lowered = match outcome {
        Ok(()) => Ok(None),
        Err(error) => Err(error),
    };
    set_result(results, result_to_val(lowered))
}

/// Define the `host` interface's five functions.
fn add_host(linker: &mut Linker<CallState>) -> wasmtime::Result<()> {
    let mut instance = linker.instance(HOST_INTERFACE)?;
    instance.func_new("query", host_query)?;
    instance.func_new("fetch", host_fetch)?;
    instance.func_new("close", host_close)?;
    instance.func_new("execute", host_execute)?;
    instance.func_new("notify", host_notify)?;
    Ok(())
}

/// The store's resource limiter: the same guest limits a CQL UDF runs under.
fn guest_limiter(state: &mut CallState) -> &mut dyn wasmtime::ResourceLimiter {
    &mut state.guest
}

/// The epoch callback: stop the call once its **guest** time (wall time minus the time spent
/// in host calls) passes the limit.
fn guest_deadline(ctx: StoreContextMut<'_, CallState>) -> wasmtime::Result<UpdateDeadline> {
    let state = ctx.data();
    let guest_time = state.started.elapsed().saturating_sub(state.host_time);
    if guest_time < state.limit {
        return Ok(UpdateDeadline::Continue(1));
    }
    let limit = state.limit;
    tracing::warn!(
        ?guest_time,
        ?limit,
        "PL/pgSQL function exceeded its guest time limit"
    );
    Err(wasmtime::Error::msg(format!(
        "PL/pgSQL function ran for {guest_time:?} of guest time, over the {limit:?} limit"
    )))
}

impl UdfExecutor {
    /// Validate and compile component bytes of the `routine` world.
    ///
    /// # Errors
    ///
    /// [`UdfError::BinaryTooLarge`] over `max_wasm_size`; [`UdfError::CompilationFailed`] when
    /// the bytes are not a component wasmtime accepts.
    pub fn load_plpgsql(&self, bytes: &[u8]) -> Result<Arc<PlpgsqlComponent>, UdfError> {
        let max = self.sandbox().max_wasm_size;
        if bytes.len() > max {
            return Err(UdfError::BinaryTooLarge {
                size: bytes.len(),
                max,
            });
        }
        match Component::new(self.engine(), bytes) {
            Ok(component) => Ok(Arc::new(PlpgsqlComponent { component })),
            Err(e) => Err(UdfError::CompilationFailed(format!(
                "PL/pgSQL component: {e:#}"
            ))),
        }
    }

    /// Call a compiled PL/pgSQL function with `host` as its database. The host is moved into
    /// the call and handed back with the result, whatever the outcome.
    ///
    /// A trap (fuel, memory, guest time, or a wasm fault) is reported as SQLSTATE `54000`
    /// (`program_limit_exceeded`) for resource limits and `XX000` otherwise; never as success.
    pub fn call_plpgsql(
        &self,
        function: &PlpgsqlComponent,
        call: Call,
        host: BoxedHost,
    ) -> (Result<Option<Value>, SqlError>, BoxedHost) {
        let mut store = match self.plpgsql_store(host) {
            Ok(store) => store,
            Err((error, host)) => return (Err(error), host),
        };
        let result = run_call(&mut store, &function.component, call);
        let state = store.into_data();
        (result, state.host)
    }

    fn plpgsql_store(&self, host: BoxedHost) -> Result<Store<CallState>, (SqlError, BoxedHost)> {
        let state = CallState {
            guest: GuestState::new(self.sandbox()),
            host,
            host_time: Duration::ZERO,
            started: Instant::now(),
            limit: self.sandbox().max_execution_time,
        };
        let mut store = Store::new(self.engine(), state);
        store.limiter(guest_limiter);
        if let Err(e) = store.set_fuel(self.sandbox().max_fuel) {
            let host = store.into_data().host;
            return Err((SqlError::new("XX000", format!("setting fuel: {e}")), host));
        }
        store.epoch_deadline_callback(guest_deadline);
        store.set_epoch_deadline(1);
        Ok(store)
    }
}

fn run_call(
    store: &mut Store<CallState>,
    component: &Component,
    call: Call,
) -> Result<Option<Value>, SqlError> {
    let mut linker: Linker<CallState> = Linker::new(store.engine());
    if let Err(e) = add_host(&mut linker) {
        return Err(SqlError::new(
            "XX000",
            format!("linking the PL/pgSQL host: {e:#}"),
        ));
    }
    let instance = match linker.instantiate(&mut *store, component) {
        Ok(instance) => instance,
        Err(e) => return Err(trap_error(&e, "instantiating")),
    };
    let Some(func) = instance.get_func(&mut *store, CALL_EXPORT) else {
        return Err(SqlError::new(
            "XX000",
            "PL/pgSQL component does not export `call`",
        ));
    };
    let Call { args, trigger } = call;
    let params = [values_to_val(args), trigger_to_val(trigger)];
    let mut results = [Val::Bool(false)]; // overwritten by the call's one result
    if let Err(e) = func.call(&mut *store, &params, &mut results) {
        return Err(trap_error(&e, "running"));
    }
    match val_to_outcome(&results[0]) {
        Ok(outcome) => outcome,
        Err(e) => Err(trap_error(&e, "reading the result")),
    }
}

/// A trap as a SQL error: resource limits are `54000`, anything else `XX000`. Logged at the
/// point of failure.
fn trap_error(error: &wasmtime::Error, during: &str) -> SqlError {
    let text = format!("{error:#}");
    let limit = error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::OutOfFuel)
        || error
            .downcast_ref::<crate::sandbox::MemoryLimitTrap>()
            .is_some()
        || text.contains("guest time");
    tracing::error!(error = %text, during, "PL/pgSQL component trapped");
    let state = if limit { "54000" } else { "XX000" };
    SqlError::new(
        state,
        format!("PL/pgSQL function trapped while {during}: {text}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SandboxConfig;

    #[test]
    fn bytes_that_are_not_a_component_are_refused() {
        let executor = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        let error = executor.load_plpgsql(b"not wasm").err().expect("refused");
        assert!(matches!(error, UdfError::CompilationFailed(_)), "{error}");
    }

    #[test]
    fn an_oversized_component_is_refused_before_compiling() {
        let config = SandboxConfig {
            max_wasm_size: 4,
            ..SandboxConfig::default()
        };
        let executor = UdfExecutor::new(config).expect("executor");
        let error = executor.load_plpgsql(b"12345").err().expect("refused");
        assert!(matches!(error, UdfError::BinaryTooLarge { .. }), "{error}");
    }
}
