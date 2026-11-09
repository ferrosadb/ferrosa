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
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Store, UpdateDeadline};

use crate::sandbox::GuestState;
use crate::{UdfError, UdfExecutor};

mod bindings {
    wasmtime::component::bindgen!({
        path: "src/wit/plpgsql.wit",
        world: "routine",
    });
}

use bindings::ferrosa::plpgsql::types as wit;

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

fn to_wit(value: Value) -> wit::Value {
    wit::Value {
        type_oid: value.type_oid,
        text: value.text.map(|t| t.to_string()), // ABI boundary: the canonical ABI lowers an owned String
    }
}

fn from_wit(value: wit::Value) -> Value {
    Value {
        type_oid: value.type_oid,
        text: value.text.map(Arc::from),
    }
}

fn err_to_wit(error: SqlError) -> wit::SqlError {
    wit::SqlError {
        sqlstate: error.sqlstate,
        message: error.message,
    }
}

fn params_from_wit(params: Vec<wit::Value>) -> Vec<Value> {
    params.into_iter().map(from_wit).collect()
}

impl bindings::ferrosa::plpgsql::types::Host for CallState {}

impl bindings::ferrosa::plpgsql::host::Host for CallState {
    fn query(&mut self, sql: String, params: Vec<wit::Value>) -> Result<u32, wit::SqlError> {
        let params = params_from_wit(params);
        self.timed(|h| h.query(&sql, &params)).map_err(err_to_wit)
    }

    fn fetch(&mut self, cursor: u32, max: u32) -> Result<Vec<Vec<wit::Value>>, wit::SqlError> {
        let rows = self.timed(|h| h.fetch(cursor, max)).map_err(err_to_wit)?;
        Ok(rows
            .into_iter()
            .map(|row| row.into_iter().map(to_wit).collect())
            .collect())
    }

    fn close(&mut self, cursor: u32) {
        self.timed(|h| h.close(cursor));
    }

    fn execute(&mut self, sql: String, params: Vec<wit::Value>) -> Result<u64, wit::SqlError> {
        let params = params_from_wit(params);
        self.timed(|h| h.execute(&sql, &params)).map_err(err_to_wit)
    }

    fn notify(&mut self, channel: String, payload: String) -> Result<(), wit::SqlError> {
        self.timed(|h| h.notify(&channel, &payload))
            .map_err(err_to_wit)
    }
}

fn trigger_to_wit(trigger: TriggerCall) -> wit::TriggerData {
    let TriggerCall {
        op,
        new_row,
        old_row,
    } = trigger;
    wit::TriggerData {
        op,
        new_row: new_row.map(|r| r.into_iter().map(to_wit).collect()),
        old_row: old_row.map(|r| r.into_iter().map(to_wit).collect()),
    }
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
    bindings::Routine::add_to_linker::<CallState, HasSelf<CallState>>(&mut linker, |s| s)
        .map_err(|e| SqlError::new("XX000", format!("linking the PL/pgSQL host: {e:#}")))?;
    let routine = bindings::Routine::instantiate(&mut *store, component, &linker)
        .map_err(|e| trap_error(&e, "instantiating"))?;
    let Call { args, trigger } = call;
    let args: Vec<wit::Value> = args.into_iter().map(to_wit).collect();
    let trigger = trigger.map(trigger_to_wit);
    let outcome = routine
        .call_call(&mut *store, &args, trigger.as_ref())
        .map_err(|e| trap_error(&e, "running"))?;
    outcome
        .map(|value| value.map(from_wit))
        .map_err(|e| SqlError::new(&e.sqlstate, e.message))
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
