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
use wasmtime::component::{Component, Linker, LinkerInstance, Val};
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
    host_time: Duration,
}

impl CallState {
    /// Run `f` against the host, counting its time as host time.
    fn timed<T>(&mut self, f: impl FnOnce(&mut dyn Host) -> T) -> T {
        let started = Instant::now();
        let out = f(&mut *self.host);
        self.host_time += started.elapsed();
        out
    }
}

// ---------------------------------------------------------------- WIT values <-> Val

/// `record value { type-oid: u32, text: option<string> }`.
fn value_to_val(value: Value) -> Val {
    // ABI boundary: the canonical ABI lowers an owned String.
    let text = value.text.map(|t| Box::new(Val::String(t.to_string())));
    Val::Record(vec![
        ("type-oid".to_string(), Val::U32(value.type_oid)),
        ("text".to_string(), Val::Option(text)),
    ])
}

fn values_to_val(values: Vec<Value>) -> Val {
    Val::List(values.into_iter().map(value_to_val).collect())
}

/// `record sql-error { sqlstate: string, message: string }`.
fn error_to_val(error: SqlError) -> Val {
    Val::Record(vec![
        ("sqlstate".to_string(), Val::String(error.sqlstate)),
        ("message".to_string(), Val::String(error.message)),
    ])
}

/// `result<T, sql-error>`, with `ok` lowering the success payload (`None` for `_`).
fn result_to_val<T>(result: Result<T, SqlError>, ok: impl FnOnce(T) -> Option<Val>) -> Val {
    Val::Result(match result {
        Ok(value) => Ok(ok(value).map(Box::new)),
        Err(error) => Err(Some(Box::new(error_to_val(error)))),
    })
}

/// A lifted value that does not have the WIT shape. Wasmtime type-checks the component
/// against the import types, so this is a host/contract mismatch: it traps, loudly.
fn shape(expected: &str, got: &Val) -> wasmtime::Error {
    wasmtime::Error::msg(format!("PL/pgSQL ABI: expected {expected}, got {got:?}"))
}

fn field<'v>(fields: &'v [(String, Val)], name: &str) -> wasmtime::Result<&'v Val> {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
        .ok_or_else(|| wasmtime::Error::msg(format!("PL/pgSQL ABI: record lacks `{name}`")))
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

fn val_list(val: &Val) -> wasmtime::Result<&[Val]> {
    match val {
        Val::List(items) => Ok(items),
        other => Err(shape("list", other)),
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
    val_list(val)?.iter().map(val_to_value).collect()
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
    Val::Option(trigger.map(|trigger| {
        let TriggerCall {
            op,
            new_row,
            old_row,
        } = trigger;
        Box::new(Val::Record(vec![
            ("op".to_string(), Val::String(op)),
            (
                "new-row".to_string(),
                Val::Option(new_row.map(|r| Box::new(values_to_val(r)))),
            ),
            (
                "old-row".to_string(),
                Val::Option(old_row.map(|r| Box::new(values_to_val(r)))),
            ),
        ]))
    }))
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
    let slot = results
        .first_mut()
        .ok_or_else(|| wasmtime::Error::msg("PL/pgSQL ABI: no result slot"))?;
    *slot = val;
    Ok(())
}

fn param(params: &[Val], index: usize) -> wasmtime::Result<&Val> {
    params
        .get(index)
        .ok_or_else(|| wasmtime::Error::msg(format!("PL/pgSQL ABI: missing argument {index}")))
}

/// `query: func(sql: string, params: list<value>) -> result<u32, sql-error>`.
fn host_query(
    mut store: StoreContextMut<'_, CallState>,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let sql = val_string(param(params, 0)?)?;
    let args = val_to_values(param(params, 1)?)?;
    let outcome = store.data_mut().timed(|h| h.query(&sql, &args));
    set_result(
        results,
        result_to_val(outcome, |cursor| Some(Val::U32(cursor))),
    )
}

/// `fetch: func(cursor: u32, max: u32) -> result<list<list<value>>, sql-error>`.
fn host_fetch(
    mut store: StoreContextMut<'_, CallState>,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let cursor = val_u32(param(params, 0)?)?;
    let max = val_u32(param(params, 1)?)?;
    let outcome = store.data_mut().timed(|h| h.fetch(cursor, max));
    set_result(
        results,
        result_to_val(outcome, |rows| {
            Some(Val::List(rows.into_iter().map(values_to_val).collect()))
        }),
    )
}

/// `close: func(cursor: u32)`.
fn host_close(
    mut store: StoreContextMut<'_, CallState>,
    params: &[Val],
    _results: &mut [Val],
) -> wasmtime::Result<()> {
    let cursor = val_u32(param(params, 0)?)?;
    store.data_mut().timed(|h| h.close(cursor));
    Ok(())
}

/// `execute: func(sql: string, params: list<value>) -> result<u64, sql-error>`.
fn host_execute(
    mut store: StoreContextMut<'_, CallState>,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let sql = val_string(param(params, 0)?)?;
    let args = val_to_values(param(params, 1)?)?;
    let outcome = store.data_mut().timed(|h| h.execute(&sql, &args));
    set_result(
        results,
        result_to_val(outcome, |count| Some(Val::U64(count))),
    )
}

/// `notify: func(channel: string, payload: string) -> result<_, sql-error>`.
fn host_notify(
    mut store: StoreContextMut<'_, CallState>,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let channel = val_string(param(params, 0)?)?;
    let payload = val_string(param(params, 1)?)?;
    let outcome = store.data_mut().timed(|h| h.notify(&channel, &payload));
    set_result(results, result_to_val(outcome, |()| None))
}

/// Define the `host` interface's five functions in `instance`.
fn add_host(instance: &mut LinkerInstance<'_, CallState>) -> wasmtime::Result<()> {
    instance.func_new("query", |s, _ty, p, r| host_query(s, p, r))?;
    instance.func_new("fetch", |s, _ty, p, r| host_fetch(s, p, r))?;
    instance.func_new("close", |s, _ty, p, r| host_close(s, p, r))?;
    instance.func_new("execute", |s, _ty, p, r| host_execute(s, p, r))?;
    instance.func_new("notify", |s, _ty, p, r| host_notify(s, p, r))?;
    Ok(())
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
        let component = Component::new(self.engine(), bytes)
            .map_err(|e| UdfError::CompilationFailed(format!("PL/pgSQL component: {e:#}")))?;
        Ok(Arc::new(PlpgsqlComponent { component }))
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
        };
        let mut store = Store::new(self.engine(), state);
        store.limiter(|s| &mut s.guest);
        if let Err(e) = store.set_fuel(self.sandbox().max_fuel) {
            let host = store.into_data().host;
            return Err((SqlError::new("XX000", format!("setting fuel: {e}")), host));
        }
        let limit = self.sandbox().max_execution_time;
        let started = Instant::now();
        store.epoch_deadline_callback(move |ctx| {
            let guest_time = started.elapsed().saturating_sub(ctx.data().host_time);
            if guest_time < limit {
                return Ok(UpdateDeadline::Continue(1));
            }
            tracing::warn!(
                ?guest_time,
                ?limit,
                "PL/pgSQL function exceeded its guest time limit"
            );
            Err(wasmtime::Error::msg(format!(
                "PL/pgSQL function ran for {guest_time:?} of guest time, over the {limit:?} limit"
            )))
        });
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
    linker
        .instance(HOST_INTERFACE)
        .and_then(|mut instance| add_host(&mut instance))
        .map_err(|e| SqlError::new("XX000", format!("linking the PL/pgSQL host: {e:#}")))?;
    let instance = linker
        .instantiate(&mut *store, component)
        .map_err(|e| trap_error(&e, "instantiating"))?;
    let func = instance
        .get_func(&mut *store, CALL_EXPORT)
        .ok_or_else(|| SqlError::new("XX000", "PL/pgSQL component does not export `call`"))?;
    let Call { args, trigger } = call;
    let params = [values_to_val(args), trigger_to_val(trigger)];
    let mut results = [Val::Bool(false)]; // overwritten by the call's one result
    func.call(&mut *store, &params, &mut results)
        .map_err(|e| trap_error(&e, "running"))?;
    val_to_outcome(&results[0]).map_err(|e| trap_error(&e, "reading the result"))?
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
