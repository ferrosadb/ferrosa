//! WASM function executor with SlotMap-backed function registry and instance pooling.
//!
//! Provides real Wasmtime Component Model compilation and invocation for
//! CQL User-Defined Functions. The executor validates WASM at compile time
//! and invokes the `invoke` export at call time with fuel-based metering.
//!
//! Instance pooling amortises the per-call instantiation overhead: a pool
//! of pre-instantiated `(Store, Instance)` pairs is kept per function key.
//! On `call()` the executor tries the pool first; on miss it falls back to
//! fresh instantiation. After a call the instance is returned to the pool
//! (if the pool is not full).
//!
//! The WIT `cql-value` variant is a recursive type (list/set/map/tuple/udt
//! variants contain `cql-value`), which prevents use of `wasmtime::component::bindgen!`
//! (the Component Model type system does not support recursive types in bindgen).
//! Instead, we use the dynamic `Val` API with `Val::Variant` to correctly encode
//! the `cql-value` discriminant names and payloads matching the WIT contract.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use ferrosa_common::{CqlType, CqlValue};
use slotmap::{DefaultKey, SlotMap};
use wasmtime::component::{Component, Instance, Linker, Val};
use wasmtime::{Engine, Store};

use crate::convert::{cql_to_wit, wit_to_cql, WitCqlValue};
use crate::error::UdfError;
use crate::sandbox::SandboxConfig;

/// Opaque handle to a compiled function. O(1) array-index lookup on hot path.
/// Ephemeral: per-process only, never serialized or sent over the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FunctionKey(DefaultKey);

// Maximum number of pooled instances kept per function.
// Using a fixed constant avoids a `num_cpus` dependency.
const POOL_MAX_PER_FUNCTION: usize = 8;

/// Compiled WASM component ready for instantiation.
struct CompiledFunction {
    component: Component,
}

/// Registry mapping (keyspace, name) -> compiled function via SlotMap.
///
/// The `slots` SlotMap provides O(1) generational-index lookup on the hot path.
/// The `index` HashMap maps the logical function name to its SlotMap key.
struct FunctionRegistry {
    slots: SlotMap<DefaultKey, Arc<CompiledFunction>>,
    index: HashMap<(String, String), FunctionKey>,
}

impl FunctionRegistry {
    fn new() -> Self {
        Self {
            slots: SlotMap::new(),
            index: HashMap::new(),
        }
    }
}

/// A pre-instantiated WASM component instance with its associated store.
struct PooledInstance {
    store: Store<()>,
    instance: Instance,
}

/// Per-function pool of warm `(Store, Instance)` pairs.
///
/// Each function key maps to a `Mutex<Vec<PooledInstance>>`. Acquire pops
/// from the vec; release pushes back (up to `max_per_function`).
struct InstancePool {
    pools: HashMap<(String, String), Mutex<Vec<PooledInstance>>>,
    max_per_function: usize,
}

impl InstancePool {
    fn new(max_per_function: usize) -> Self {
        Self {
            pools: HashMap::new(),
            max_per_function,
        }
    }

    /// Ensure a pool entry exists for `key`. No-op if already present.
    fn ensure_pool(&mut self, key: (String, String)) {
        self.pools
            .entry(key)
            .or_insert_with(|| Mutex::new(Vec::new()));
    }

    /// Try to take a warm instance from the pool. Returns `None` on miss.
    fn acquire(&self, key: &(String, String)) -> Option<PooledInstance> {
        let pool = self.pools.get(key)?;
        let mut guard = pool.lock().ok()?;
        guard.pop()
    }

    /// Return an instance to the pool. Drops the instance if the pool is full.
    fn release(&self, key: &(String, String), instance: PooledInstance) {
        if let Some(pool) = self.pools.get(key) {
            if let Ok(mut guard) = pool.lock() {
                if guard.len() < self.max_per_function {
                    guard.push(instance);
                }
            }
        }
    }

    /// Remove and drop all pooled instances for `key` (called on invalidate).
    fn drain(&self, key: &(String, String)) {
        if let Some(pool) = self.pools.get(key) {
            if let Ok(mut guard) = pool.lock() {
                guard.clear();
            }
        }
    }
}

/// Executor for WASM-based User-Defined Functions.
///
/// Manages the Wasmtime engine, function registry, instance pool, and sandbox
/// policy. Thread-safe — can be shared across async tasks via `Arc`.
pub struct UdfExecutor {
    engine: Engine,
    config: SandboxConfig,
    registry: RwLock<FunctionRegistry>,
    pool: RwLock<InstancePool>,
}

impl UdfExecutor {
    /// Create a new executor with the given sandbox configuration.
    ///
    /// Spawns a background thread (`udf-epoch-ticker`) that increments the
    /// engine epoch once per `config.max_execution_time` interval. This is
    /// required for epoch-based interruption to trigger correctly.
    pub fn new(config: SandboxConfig) -> Result<Self, UdfError> {
        let mut engine_config = wasmtime::Config::new();
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true);
        engine_config.wasm_component_model(true);

        let engine = Engine::new(&engine_config)
            .map_err(|e| UdfError::CompilationFailed(format!("engine creation failed: {e}")))?;

        // Spawn the epoch ticker thread so epoch interruption actually fires.
        let engine_for_ticker = engine.clone();
        let tick_interval = config.max_execution_time;
        std::thread::Builder::new()
            .name("udf-epoch-ticker".into())
            .spawn(move || loop {
                std::thread::sleep(tick_interval);
                engine_for_ticker.increment_epoch();
            })
            .map_err(|e| {
                UdfError::CompilationFailed(format!("failed to spawn epoch ticker: {e}"))
            })?;

        let pool = RwLock::new(InstancePool::new(POOL_MAX_PER_FUNCTION));

        Ok(Self {
            engine,
            config,
            registry: RwLock::new(FunctionRegistry::new()),
            pool,
        })
    }

    /// Pre-compile a WASM binary. Called on INSERT into wasm_binaries.
    ///
    /// Validates the WASM component bytes using the Wasmtime compiler.
    /// The compiled component is registered for later invocation and a pool
    /// slot is pre-allocated for the function key.
    /// If an entry already exists for (keyspace, name) it is replaced (CREATE OR REPLACE).
    pub fn compile(&self, keyspace: &str, name: &str, wasm_bytes: &[u8]) -> Result<(), UdfError> {
        if wasm_bytes.len() > self.config.max_wasm_size {
            return Err(UdfError::BinaryTooLarge {
                size: wasm_bytes.len(),
                max: self.config.max_wasm_size,
            });
        }

        let component = Component::new(&self.engine, wasm_bytes)
            .map_err(|e| UdfError::CompilationFailed(format!("{e}")))?;

        tracing::info!(
            keyspace,
            name,
            size = wasm_bytes.len(),
            "compiled WASM function"
        );

        let key = (keyspace.to_string(), name.to_string());
        let compiled = Arc::new(CompiledFunction { component });

        let mut reg = self.registry.write().expect("registry lock poisoned");
        // Remove old slot if replacing an existing entry.
        if let Some(existing) = reg.index.remove(&key) {
            reg.slots.remove(existing.0);
        }
        let slot_key = reg.slots.insert(compiled);
        reg.index.insert(key.clone(), FunctionKey(slot_key));
        drop(reg);

        // Ensure the pool has an entry for this key.
        if let Ok(mut pool) = self.pool.write() {
            pool.ensure_pool(key);
        }

        Ok(())
    }

    /// Invalidate a registered function (on DROP or CREATE OR REPLACE pre-clear).
    ///
    /// Removes the compiled function from the registry and drains any pooled
    /// instances so stale code is not reused.
    pub fn invalidate(&self, keyspace: &str, name: &str) {
        let key = (keyspace.to_string(), name.to_string());
        let mut reg = self.registry.write().expect("registry lock poisoned");
        if let Some(fk) = reg.index.remove(&key) {
            reg.slots.remove(fk.0);
        }
        drop(reg);
        if let Ok(pool) = self.pool.read() {
            pool.drain(&key);
        }
    }

    /// Resolve a function name to its opaque key and kind for query planning.
    ///
    /// Acquires a read lock and returns immediately. The returned `FunctionKey`
    /// can be passed to `call_by_key` on the hot path without another name lookup.
    pub fn resolve(
        &self,
        keyspace: &str,
        name: &str,
    ) -> Result<(FunctionKey, FunctionKind), UdfError> {
        let reg = self.registry.read().expect("registry lock poisoned");
        let fk = *reg
            .index
            .get(&(keyspace.to_string(), name.to_string()))
            .ok_or_else(|| UdfError::NotFound {
                keyspace: keyspace.to_string(),
                name: name.to_string(),
            })?;
        // All functions registered via compile() are scalar UDFs.
        // UDA support will extend this when UDA metadata is tracked.
        Ok((fk, FunctionKind::Scalar))
    }

    /// Look up the kind of a compiled function by name.
    ///
    /// Returns `NotFound` if the function has not been compiled.
    pub fn get_kind(&self, keyspace: &str, name: &str) -> Result<FunctionKind, UdfError> {
        let reg = self.registry.read().expect("registry lock poisoned");
        reg.index
            .get(&(keyspace.to_string(), name.to_string()))
            .map(|_| FunctionKind::Scalar)
            .ok_or_else(|| UdfError::NotFound {
                keyspace: keyspace.to_string(),
                name: name.to_string(),
            })
    }

    /// Invoke a UDF. Returns the function's result.
    ///
    /// Tries to acquire a warm `(Store, Instance)` from the instance pool.
    /// On a pool miss a fresh store and instance are created. After the call
    /// the instance is returned to the pool for reuse.
    ///
    /// The conversion chain is:
    /// 1. `CqlValue` -> `WitCqlValue` via `cql_to_wit` (preserves all 26 types)
    /// 2. `WitCqlValue` -> `Val::Variant` via `wit_cql_value_to_val` (WIT encoding)
    /// 3. Call the WASM component's `invoke` export
    /// 4. `Val::Result` -> `Val::Variant` -> `WitCqlValue` via `val_to_wit_cql_value`
    /// 5. `WitCqlValue` -> `CqlValue` via `wit_to_cql` (type-directed reconstruction)
    pub fn call(
        &self,
        keyspace: &str,
        func_name: &str,
        args: Vec<CqlValue>,
        _arg_types: &[CqlType],
        return_type: &CqlType,
    ) -> Result<CqlValue, UdfError> {
        let key = (keyspace.to_string(), func_name.to_string());

        let compiled = {
            let reg = self.registry.read().expect("registry lock poisoned");
            let fk = reg.index.get(&key).ok_or_else(|| UdfError::NotFound {
                keyspace: keyspace.to_string(),
                name: func_name.to_string(),
            })?;
            Arc::clone(reg.slots.get(fk.0).expect("index/slots out of sync"))
        };

        // Try to acquire a warm instance from the pool.
        let pooled = self.pool.read().ok().and_then(|p| p.acquire(&key));

        let (mut store, instance) = if let Some(warm) = pooled {
            // Reset resource limits on the warm store before reuse.
            let mut s = warm.store;
            s.set_fuel(self.config.max_fuel)
                .map_err(|e| UdfError::ExecutionFailed(format!("failed to reset fuel: {e}")))?;
            s.epoch_deadline_trap();
            s.set_epoch_deadline(1);
            (s, warm.instance)
        } else {
            // Pool miss — create a fresh store and instantiate.
            let mut s = Store::new(&self.engine, ());
            s.set_fuel(self.config.max_fuel)
                .map_err(|e| UdfError::ExecutionFailed(format!("failed to set fuel: {e}")))?;
            s.epoch_deadline_trap();
            s.set_epoch_deadline(1);

            let linker = Linker::<()>::new(&self.engine);
            let inst = linker
                .instantiate(&mut s, &compiled.component)
                .map_err(|e| UdfError::ExecutionFailed(format!("instantiation failed: {e}")))?;
            (s, inst)
        };

        // Look up the "invoke" export
        let invoke_func = instance.get_func(&mut store, "invoke").ok_or_else(|| {
            UdfError::ExecutionFailed("component does not export 'invoke' function".into())
        })?;

        // Convert CQL args to WIT variant Vals
        let wit_args: Vec<WitCqlValue> = args.iter().map(cql_to_wit).collect();
        let args_val = wit_cql_list_to_val(&wit_args);

        // Call the function with dynamic args/results
        let mut results = vec![Val::Bool(false)]; // placeholder for result slot
        let call_result = invoke_func
            .call(&mut store, &[args_val], &mut results)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("fuel") {
                    UdfError::ResourceExhausted(format!("out of fuel: {msg}"))
                } else if msg.contains("epoch") {
                    UdfError::ResourceExhausted(format!("execution timeout: {msg}"))
                } else {
                    UdfError::ExecutionFailed(msg)
                }
            });

        // Propagate call errors without returning the instance to the pool.
        call_result?;

        // Post-call cleanup required by wasmtime component model
        invoke_func
            .post_return(&mut store)
            .map_err(|e| UdfError::ExecutionFailed(format!("post_return failed: {e}")))?;

        // Convert result Val back to CqlValue before returning the instance.
        let result_val = results
            .into_iter()
            .next()
            .ok_or_else(|| UdfError::ExecutionFailed("invoke returned no result".into()))?;

        let cql_result = val_to_cql_result(&result_val, return_type);

        // Return the instance to the pool (only on success; discard on error).
        if cql_result.is_ok() {
            if let Ok(pool) = self.pool.read() {
                pool.release(&key, PooledInstance { store, instance });
            }
        }

        cql_result
    }

    /// Invoke a UDF by opaque key (hot path — O(1) SlotMap lookup).
    ///
    /// The caller must have obtained `key` from `resolve()` at query-plan time.
    /// Acquires a read lock, clones the `Arc`, releases the lock, then proceeds
    /// identically to `call()`.
    pub fn call_by_key(
        &self,
        key: FunctionKey,
        args: Vec<CqlValue>,
        _arg_types: &[CqlType],
        return_type: &CqlType,
    ) -> Result<CqlValue, UdfError> {
        let compiled = {
            let reg = self.registry.read().expect("registry lock poisoned");
            Arc::clone(reg.slots.get(key.0).ok_or(UdfError::KeyInvalid)?)
        };

        let mut store = Store::new(&self.engine, ());
        store
            .set_fuel(self.config.max_fuel)
            .map_err(|e| UdfError::ExecutionFailed(format!("failed to set fuel: {e}")))?;

        store.epoch_deadline_trap();
        store.set_epoch_deadline(1);

        let linker = Linker::<()>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &compiled.component)
            .map_err(|e| UdfError::ExecutionFailed(format!("instantiation failed: {e}")))?;

        let invoke_func = instance.get_func(&mut store, "invoke").ok_or_else(|| {
            UdfError::ExecutionFailed("component does not export 'invoke' function".into())
        })?;

        let wit_args: Vec<WitCqlValue> = args.iter().map(cql_to_wit).collect();
        let args_val = wit_cql_list_to_val(&wit_args);

        let mut results = vec![Val::Bool(false)];
        invoke_func
            .call(&mut store, &[args_val], &mut results)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("fuel") {
                    UdfError::ResourceExhausted(format!("out of fuel: {msg}"))
                } else if msg.contains("epoch") {
                    UdfError::ResourceExhausted(format!("execution timeout: {msg}"))
                } else {
                    UdfError::ExecutionFailed(msg)
                }
            })?;

        invoke_func
            .post_return(&mut store)
            .map_err(|e| UdfError::ExecutionFailed(format!("post_return failed: {e}")))?;

        let result_val = results
            .into_iter()
            .next()
            .ok_or_else(|| UdfError::ExecutionFailed("invoke returned no result".into()))?;

        val_to_cql_result(&result_val, return_type)
    }

    /// Create a fresh UDA instance by opaque key (hot path — O(1) SlotMap lookup).
    ///
    /// Returns a `(Store, Instance)` pair ready for UDA state accumulation.
    /// The caller must have obtained `key` from `resolve()` at query-plan time.
    pub fn create_uda_instance_by_key(
        &self,
        key: FunctionKey,
    ) -> Result<(Store<()>, wasmtime::component::Instance), UdfError> {
        let compiled = {
            let reg = self.registry.read().expect("registry lock poisoned");
            Arc::clone(reg.slots.get(key.0).ok_or(UdfError::KeyInvalid)?)
        };

        let mut store = Store::new(&self.engine, ());
        store
            .set_fuel(self.config.max_aggregate_fuel)
            .map_err(|e| UdfError::ExecutionFailed(format!("failed to set fuel: {e}")))?;

        store.epoch_deadline_trap();
        store.set_epoch_deadline(1);

        let linker = Linker::<()>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &compiled.component)
            .map_err(|e| UdfError::ExecutionFailed(format!("instantiation failed: {e}")))?;

        Ok((store, instance))
    }

    /// Create a fresh UDA instance by name.
    ///
    /// Acquires a read lock, clones the `Arc`, releases the lock, then
    /// creates a `Store` with the aggregate fuel cap and instantiates the component.
    pub fn create_uda_instance(
        &self,
        keyspace: &str,
        name: &str,
    ) -> Result<(Store<()>, wasmtime::component::Instance), UdfError> {
        let compiled = {
            let reg = self.registry.read().expect("registry lock poisoned");
            let fk = reg
                .index
                .get(&(keyspace.to_string(), name.to_string()))
                .ok_or_else(|| UdfError::NotFound {
                    keyspace: keyspace.to_string(),
                    name: name.to_string(),
                })?;
            Arc::clone(reg.slots.get(fk.0).expect("index/slots out of sync"))
        };

        let mut store = Store::new(&self.engine, ());
        store
            .set_fuel(self.config.max_aggregate_fuel)
            .map_err(|e| UdfError::ExecutionFailed(format!("failed to set fuel: {e}")))?;

        store.epoch_deadline_trap();
        store.set_epoch_deadline(1);

        let linker = Linker::<()>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &compiled.component)
            .map_err(|e| UdfError::ExecutionFailed(format!("instantiation failed: {e}")))?;

        Ok((store, instance))
    }
}

/// The kind of a compiled function — scalar UDF or aggregate UDA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    /// User-Defined Function: maps arguments -> return value.
    Scalar,
    /// User-Defined Aggregate: accumulates state across rows.
    Aggregate,
}

// ---------------------------------------------------------------------------
// Val encoding: WitCqlValue -> Val (for passing args to WASM)
// ---------------------------------------------------------------------------

/// Convert a list of WitCqlValue to a `Val::List` of variant-encoded values.
///
/// Each element is encoded as `Val::Variant` with the discriminant name
/// matching the WIT `cql-value` variant case (e.g. `"null"`, `"int-val"`, etc.).
fn wit_cql_list_to_val(values: &[WitCqlValue]) -> Val {
    let items: Vec<Val> = values.iter().map(wit_cql_value_to_val).collect();
    Val::List(items)
}

/// Convert a single `WitCqlValue` to its WIT `cql-value` variant encoding.
///
/// Uses `Val::Variant(discriminant, payload)` where the discriminant name
/// matches the WIT definition exactly (kebab-case). The payload types match
/// the WIT parameter types:
///
/// | WIT case               | Discriminant      | Payload type                        |
/// |------------------------|-------------------|-------------------------------------|
/// | `null`                 | `"null"`          | None                                |
/// | `int-val(s32)`         | `"int-val"`       | `Val::S32`                          |
/// | `bigint-val(s64)`      | `"bigint-val"`    | `Val::S64`                          |
/// | `float-val(f32)`       | `"float-val"`     | `Val::Float32`                      |
/// | `double-val(f64)`      | `"double-val"`    | `Val::Float64`                      |
/// | `boolean-val(bool)`    | `"boolean-val"`   | `Val::Bool`                         |
/// | `text-val(string)`     | `"text-val"`      | `Val::String`                       |
/// | `blob-val(list<u8>)`   | `"blob-val"`      | `Val::List(Vec<Val::U8>)`           |
/// | `uuid-val(string)`     | `"uuid-val"`      | `Val::String`                       |
/// | `timestamp-val(s64)`   | `"timestamp-val"` | `Val::S64`                          |
/// | `date-val(s32)`        | `"date-val"`      | `Val::S32`                          |
/// | `time-val(s64)`        | `"time-val"`      | `Val::S64`                          |
/// | `smallint-val(s16)`    | `"smallint-val"`  | `Val::S16`                          |
/// | `tinyint-val(s8)`      | `"tinyint-val"`   | `Val::S8`                           |
/// | `inet-val(string)`     | `"inet-val"`      | `Val::String`                       |
/// | `decimal-val(t<…>)`    | `"decimal-val"`   | `Val::Tuple([list<u8>, s32])`       |
/// | `varint-val(list<u8>)` | `"varint-val"`    | `Val::List(Vec<Val::U8>)`           |
/// | `duration-val(t<…>)`   | `"duration-val"`  | `Val::Tuple([s32, s32, s64])`       |
/// | `ascii-val(string)`    | `"ascii-val"`     | `Val::String`                       |
/// | `timeuuid-val(string)` | `"timeuuid-val"`  | `Val::String`                       |
/// | `list-val(list<…>)`    | `"list-val"`      | `Val::List(Vec<cql-value variant>)` |
/// | `set-val(list<…>)`     | `"set-val"`       | `Val::List(Vec<cql-value variant>)` |
/// | `map-val(list<…>)`     | `"map-val"`       | `Val::List(Vec<Tuple<cv, cv>>)`     |
/// | `tuple-val(list<…>)`   | `"tuple-val"`     | `Val::List(Vec<cql-value variant>)` |
/// | `udt-val(list<…>)`     | `"udt-val"`       | `Val::List(Vec<Tuple<str, cv>>)`    |
/// | `counter-val(s64)`     | `"counter-val"`   | `Val::S64`                          |
fn wit_cql_value_to_val(v: &WitCqlValue) -> Val {
    match v {
        WitCqlValue::Null => Val::Variant("null".into(), None),
        WitCqlValue::IntVal(i) => variant_with("int-val", Val::S32(*i)),
        WitCqlValue::BigintVal(i) => variant_with("bigint-val", Val::S64(*i)),
        WitCqlValue::FloatVal(f) => variant_with("float-val", Val::Float32(*f)),
        WitCqlValue::DoubleVal(f) => variant_with("double-val", Val::Float64(*f)),
        WitCqlValue::BooleanVal(b) => variant_with("boolean-val", Val::Bool(*b)),
        WitCqlValue::TextVal(s) => variant_with("text-val", Val::String(s.clone())),
        WitCqlValue::BlobVal(b) => variant_with("blob-val", bytes_to_val(b)),
        WitCqlValue::UuidVal(s) => variant_with("uuid-val", Val::String(s.clone())),
        WitCqlValue::TimestampVal(i) => variant_with("timestamp-val", Val::S64(*i)),
        WitCqlValue::DateVal(i) => variant_with("date-val", Val::S32(*i)),
        WitCqlValue::TimeVal(i) => variant_with("time-val", Val::S64(*i)),
        WitCqlValue::SmallintVal(i) => variant_with("smallint-val", Val::S16(*i)),
        WitCqlValue::TinyintVal(i) => variant_with("tinyint-val", Val::S8(*i)),
        WitCqlValue::InetVal(s) => variant_with("inet-val", Val::String(s.clone())),
        WitCqlValue::DecimalVal(bytes, scale) => variant_with(
            "decimal-val",
            Val::Tuple(vec![bytes_to_val(bytes), Val::S32(*scale)]),
        ),
        WitCqlValue::VarintVal(bytes) => variant_with("varint-val", bytes_to_val(bytes)),
        WitCqlValue::DurationVal(m, d, n) => variant_with(
            "duration-val",
            Val::Tuple(vec![Val::S32(*m), Val::S32(*d), Val::S64(*n)]),
        ),
        WitCqlValue::AsciiVal(s) => variant_with("ascii-val", Val::String(s.clone())),
        WitCqlValue::TimeuuidVal(s) => variant_with("timeuuid-val", Val::String(s.clone())),
        WitCqlValue::ListVal(items) => variant_with(
            "list-val",
            Val::List(items.iter().map(wit_cql_value_to_val).collect()),
        ),
        WitCqlValue::SetVal(items) => variant_with(
            "set-val",
            Val::List(items.iter().map(wit_cql_value_to_val).collect()),
        ),
        WitCqlValue::MapVal(entries) => {
            let tuples: Vec<Val> = entries
                .iter()
                .map(|(k, v)| Val::Tuple(vec![wit_cql_value_to_val(k), wit_cql_value_to_val(v)]))
                .collect();
            variant_with("map-val", Val::List(tuples))
        }
        WitCqlValue::TupleVal(items) => variant_with(
            "tuple-val",
            Val::List(items.iter().map(wit_cql_value_to_val).collect()),
        ),
        WitCqlValue::UdtVal(fields) => {
            let tuples: Vec<Val> = fields
                .iter()
                .map(|(name, v)| {
                    Val::Tuple(vec![Val::String(name.clone()), wit_cql_value_to_val(v)])
                })
                .collect();
            variant_with("udt-val", Val::List(tuples))
        }
        WitCqlValue::CounterVal(i) => variant_with("counter-val", Val::S64(*i)),
    }
}

/// Helper: construct `Val::Variant(name, Some(Box::new(payload)))`.
#[inline]
fn variant_with(name: &str, payload: Val) -> Val {
    Val::Variant(name.into(), Some(Box::new(payload)))
}

/// Helper: convert a byte slice to `Val::List(Vec<Val::U8>)`.
#[inline]
fn bytes_to_val(bytes: &[u8]) -> Val {
    Val::List(bytes.iter().map(|b| Val::U8(*b)).collect())
}

// ---------------------------------------------------------------------------
// Val decoding: Val -> WitCqlValue (for reading results from WASM)
// ---------------------------------------------------------------------------

/// Convert a result `Val` from the `invoke` export back to `CqlValue`.
///
/// The WIT contract specifies: `result<cql-value, string>`.
/// The component model encodes this as `Val::Result(Ok(Some(variant)) | Err(Some(string)))`.
fn val_to_cql_result(val: &Val, return_type: &CqlType) -> Result<CqlValue, UdfError> {
    match val {
        Val::Result(result) => match result.as_ref() {
            Ok(Some(inner)) => {
                let wit_val = val_to_wit_cql_value(inner)?;
                wit_to_cql(&wit_val, return_type)
            }
            Ok(None) => Ok(CqlValue::Null),
            Err(Some(err_val)) => {
                let msg = match &**err_val {
                    Val::String(s) => s.clone(),
                    other => format!("UDF error: {other:?}"),
                };
                Err(UdfError::ExecutionFailed(msg))
            }
            Err(None) => Err(UdfError::ExecutionFailed("UDF returned error".into())),
        },
        // If the component returns the variant directly (no result wrapper),
        // try to decode it as a cql-value variant.
        other => {
            let wit_val = val_to_wit_cql_value(other)?;
            wit_to_cql(&wit_val, return_type)
        }
    }
}

/// Decode a `Val::Variant` (cql-value encoding) back to `WitCqlValue`.
///
/// Matches the discriminant name and extracts the typed payload.
/// Handles all 26 variant cases from the WIT contract.
fn val_to_wit_cql_value(val: &Val) -> Result<WitCqlValue, UdfError> {
    match val {
        Val::Variant(disc, payload) => match disc.as_str() {
            "null" => Ok(WitCqlValue::Null),
            "int-val" => {
                let v = extract_s32(payload, "int-val")?;
                Ok(WitCqlValue::IntVal(v))
            }
            "bigint-val" => {
                let v = extract_s64(payload, "bigint-val")?;
                Ok(WitCqlValue::BigintVal(v))
            }
            "float-val" => {
                let v = extract_float32(payload, "float-val")?;
                Ok(WitCqlValue::FloatVal(v))
            }
            "double-val" => {
                let v = extract_float64(payload, "double-val")?;
                Ok(WitCqlValue::DoubleVal(v))
            }
            "boolean-val" => {
                let v = extract_bool(payload, "boolean-val")?;
                Ok(WitCqlValue::BooleanVal(v))
            }
            "text-val" => {
                let v = extract_string(payload, "text-val")?;
                Ok(WitCqlValue::TextVal(v))
            }
            "blob-val" => {
                let v = extract_bytes(payload, "blob-val")?;
                Ok(WitCqlValue::BlobVal(v))
            }
            "uuid-val" => {
                let v = extract_string(payload, "uuid-val")?;
                Ok(WitCqlValue::UuidVal(v))
            }
            "timestamp-val" => {
                let v = extract_s64(payload, "timestamp-val")?;
                Ok(WitCqlValue::TimestampVal(v))
            }
            "date-val" => {
                let v = extract_s32(payload, "date-val")?;
                Ok(WitCqlValue::DateVal(v))
            }
            "time-val" => {
                let v = extract_s64(payload, "time-val")?;
                Ok(WitCqlValue::TimeVal(v))
            }
            "smallint-val" => {
                let v = extract_s16(payload, "smallint-val")?;
                Ok(WitCqlValue::SmallintVal(v))
            }
            "tinyint-val" => {
                let v = extract_s8(payload, "tinyint-val")?;
                Ok(WitCqlValue::TinyintVal(v))
            }
            "inet-val" => {
                let v = extract_string(payload, "inet-val")?;
                Ok(WitCqlValue::InetVal(v))
            }
            "decimal-val" => {
                let (bytes, scale) = extract_decimal(payload)?;
                Ok(WitCqlValue::DecimalVal(bytes, scale))
            }
            "varint-val" => {
                let v = extract_bytes(payload, "varint-val")?;
                Ok(WitCqlValue::VarintVal(v))
            }
            "duration-val" => {
                let (m, d, n) = extract_duration(payload)?;
                Ok(WitCqlValue::DurationVal(m, d, n))
            }
            "ascii-val" => {
                let v = extract_string(payload, "ascii-val")?;
                Ok(WitCqlValue::AsciiVal(v))
            }
            "timeuuid-val" => {
                let v = extract_string(payload, "timeuuid-val")?;
                Ok(WitCqlValue::TimeuuidVal(v))
            }
            "list-val" => {
                let items = extract_cql_value_list(payload, "list-val")?;
                Ok(WitCqlValue::ListVal(items))
            }
            "set-val" => {
                let items = extract_cql_value_list(payload, "set-val")?;
                Ok(WitCqlValue::SetVal(items))
            }
            "map-val" => {
                let entries = extract_cql_value_map(payload)?;
                Ok(WitCqlValue::MapVal(entries))
            }
            "tuple-val" => {
                let items = extract_cql_value_list(payload, "tuple-val")?;
                Ok(WitCqlValue::TupleVal(items))
            }
            "udt-val" => {
                let fields = extract_cql_value_udt(payload)?;
                Ok(WitCqlValue::UdtVal(fields))
            }
            "counter-val" => {
                let v = extract_s64(payload, "counter-val")?;
                Ok(WitCqlValue::CounterVal(v))
            }
            unknown => Err(UdfError::TypeMismatch(format!(
                "unknown cql-value variant discriminant: {unknown}"
            ))),
        },
        other => Err(UdfError::TypeMismatch(format!(
            "expected Val::Variant for cql-value, got: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Payload extraction helpers (for decoding Val payloads)
// ---------------------------------------------------------------------------

fn require_payload<'a>(
    payload: &'a Option<Box<Val>>,
    variant_name: &str,
) -> Result<&'a Val, UdfError> {
    payload
        .as_ref()
        .map(|b| b.as_ref())
        .ok_or_else(|| UdfError::TypeMismatch(format!("{variant_name}: missing payload")))
}

fn extract_bool(payload: &Option<Box<Val>>, name: &str) -> Result<bool, UdfError> {
    match require_payload(payload, name)? {
        Val::Bool(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected Bool, got {other:?}"
        ))),
    }
}

fn extract_s8(payload: &Option<Box<Val>>, name: &str) -> Result<i8, UdfError> {
    match require_payload(payload, name)? {
        Val::S8(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected S8, got {other:?}"
        ))),
    }
}

fn extract_s16(payload: &Option<Box<Val>>, name: &str) -> Result<i16, UdfError> {
    match require_payload(payload, name)? {
        Val::S16(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected S16, got {other:?}"
        ))),
    }
}

fn extract_s32(payload: &Option<Box<Val>>, name: &str) -> Result<i32, UdfError> {
    match require_payload(payload, name)? {
        Val::S32(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected S32, got {other:?}"
        ))),
    }
}

fn extract_s64(payload: &Option<Box<Val>>, name: &str) -> Result<i64, UdfError> {
    match require_payload(payload, name)? {
        Val::S64(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected S64, got {other:?}"
        ))),
    }
}

fn extract_float32(payload: &Option<Box<Val>>, name: &str) -> Result<f32, UdfError> {
    match require_payload(payload, name)? {
        Val::Float32(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected Float32, got {other:?}"
        ))),
    }
}

fn extract_float64(payload: &Option<Box<Val>>, name: &str) -> Result<f64, UdfError> {
    match require_payload(payload, name)? {
        Val::Float64(v) => Ok(*v),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected Float64, got {other:?}"
        ))),
    }
}

fn extract_string(payload: &Option<Box<Val>>, name: &str) -> Result<String, UdfError> {
    match require_payload(payload, name)? {
        Val::String(v) => Ok(v.clone()),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected String, got {other:?}"
        ))),
    }
}

/// Extract `list<u8>` payload as `Vec<u8>`.
fn extract_bytes(payload: &Option<Box<Val>>, name: &str) -> Result<Vec<u8>, UdfError> {
    match require_payload(payload, name)? {
        Val::List(items) => items
            .iter()
            .map(|v| match v {
                Val::U8(b) => Ok(*b),
                other => Err(UdfError::TypeMismatch(format!(
                    "{name}: expected U8 in list, got {other:?}"
                ))),
            })
            .collect(),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected List, got {other:?}"
        ))),
    }
}

/// Extract `tuple<list<u8>, s32>` for decimal-val.
fn extract_decimal(payload: &Option<Box<Val>>) -> Result<(Vec<u8>, i32), UdfError> {
    match require_payload(payload, "decimal-val")? {
        Val::Tuple(fields) if fields.len() == 2 => {
            let bytes = match &fields[0] {
                Val::List(items) => items
                    .iter()
                    .map(|v| match v {
                        Val::U8(b) => Ok(*b),
                        other => Err(UdfError::TypeMismatch(format!(
                            "decimal-val: expected U8 in bytes, got {other:?}"
                        ))),
                    })
                    .collect::<Result<Vec<u8>, _>>()?,
                other => {
                    return Err(UdfError::TypeMismatch(format!(
                        "decimal-val: expected List for bytes, got {other:?}"
                    )));
                }
            };
            let scale = match &fields[1] {
                Val::S32(v) => *v,
                other => {
                    return Err(UdfError::TypeMismatch(format!(
                        "decimal-val: expected S32 for scale, got {other:?}"
                    )));
                }
            };
            Ok((bytes, scale))
        }
        other => Err(UdfError::TypeMismatch(format!(
            "decimal-val: expected Tuple(2), got {other:?}"
        ))),
    }
}

/// Extract `tuple<s32, s32, s64>` for duration-val.
fn extract_duration(payload: &Option<Box<Val>>) -> Result<(i32, i32, i64), UdfError> {
    match require_payload(payload, "duration-val")? {
        Val::Tuple(fields) if fields.len() == 3 => {
            let m = match &fields[0] {
                Val::S32(v) => *v,
                other => {
                    return Err(UdfError::TypeMismatch(format!(
                        "duration-val: expected S32 for months, got {other:?}"
                    )));
                }
            };
            let d = match &fields[1] {
                Val::S32(v) => *v,
                other => {
                    return Err(UdfError::TypeMismatch(format!(
                        "duration-val: expected S32 for days, got {other:?}"
                    )));
                }
            };
            let n = match &fields[2] {
                Val::S64(v) => *v,
                other => {
                    return Err(UdfError::TypeMismatch(format!(
                        "duration-val: expected S64 for nanos, got {other:?}"
                    )));
                }
            };
            Ok((m, d, n))
        }
        other => Err(UdfError::TypeMismatch(format!(
            "duration-val: expected Tuple(3), got {other:?}"
        ))),
    }
}

/// Extract a `list<cql-value>` payload, recursively decoding each element.
fn extract_cql_value_list(
    payload: &Option<Box<Val>>,
    name: &str,
) -> Result<Vec<WitCqlValue>, UdfError> {
    match require_payload(payload, name)? {
        Val::List(items) => items.iter().map(val_to_wit_cql_value).collect(),
        other => Err(UdfError::TypeMismatch(format!(
            "{name}: expected List, got {other:?}"
        ))),
    }
}

/// Extract `list<tuple<cql-value, cql-value>>` for map-val.
fn extract_cql_value_map(
    payload: &Option<Box<Val>>,
) -> Result<Vec<(WitCqlValue, WitCqlValue)>, UdfError> {
    match require_payload(payload, "map-val")? {
        Val::List(items) => items
            .iter()
            .map(|item| match item {
                Val::Tuple(fields) if fields.len() == 2 => {
                    let k = val_to_wit_cql_value(&fields[0])?;
                    let v = val_to_wit_cql_value(&fields[1])?;
                    Ok((k, v))
                }
                other => Err(UdfError::TypeMismatch(format!(
                    "map-val: expected Tuple(2) entry, got {other:?}"
                ))),
            })
            .collect(),
        other => Err(UdfError::TypeMismatch(format!(
            "map-val: expected List, got {other:?}"
        ))),
    }
}

/// Extract `list<tuple<string, cql-value>>` for udt-val.
fn extract_cql_value_udt(
    payload: &Option<Box<Val>>,
) -> Result<Vec<(String, WitCqlValue)>, UdfError> {
    match require_payload(payload, "udt-val")? {
        Val::List(items) => items
            .iter()
            .map(|item| match item {
                Val::Tuple(fields) if fields.len() == 2 => {
                    let name = match &fields[0] {
                        Val::String(s) => s.clone(),
                        other => {
                            return Err(UdfError::TypeMismatch(format!(
                                "udt-val: expected String for field name, got {other:?}"
                            )));
                        }
                    };
                    let v = val_to_wit_cql_value(&fields[1])?;
                    Ok((name, v))
                }
                other => Err(UdfError::TypeMismatch(format!(
                    "udt-val: expected Tuple(2) entry, got {other:?}"
                ))),
            })
            .collect(),
        other => Err(UdfError::TypeMismatch(format!(
            "udt-val: expected List, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_rejects_oversized_binary() {
        let config = SandboxConfig {
            max_wasm_size: 100,
            ..Default::default()
        };
        let executor = UdfExecutor::new(config).unwrap();
        let err = executor.compile("ks", "func", &[0u8; 200]).unwrap_err();
        assert!(matches!(err, UdfError::BinaryTooLarge { .. }));
    }

    #[test]
    fn compile_rejects_invalid_wasm() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let err = executor
            .compile("ks", "bad", b"not valid wasm")
            .unwrap_err();
        assert!(
            matches!(err, UdfError::CompilationFailed(..)),
            "expected CompilationFailed, got: {err:?}"
        );
    }

    #[test]
    fn call_unknown_function_returns_not_found() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let err = executor
            .call("ks", "missing", vec![], &[], &CqlType::Int)
            .unwrap_err();
        assert!(matches!(err, UdfError::NotFound { .. }));
    }

    #[test]
    fn invalidate_removes_cached_function() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        // Insert via compile() so the registry is populated.
        executor
            .compile("ks", "func", &minimal_component_bytes())
            .unwrap();
        // Confirm it is present.
        assert!(executor.resolve("ks", "func").is_ok());
        executor.invalidate("ks", "func");
        // Confirm it is gone.
        assert!(executor.resolve("ks", "func").is_err());
    }

    #[test]
    fn engine_has_fuel_and_epoch_enabled() {
        // Verify the engine was configured correctly by creating a store
        // and checking that fuel operations succeed.
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let mut store = Store::new(&executor.engine, ());
        // set_fuel should succeed when fuel consumption is enabled
        store.set_fuel(1000).expect("fuel should be enabled");
        // epoch deadline should be settable when epoch interruption is enabled
        store.set_epoch_deadline(1);
    }

    // ---- Val encoding round-trip tests ----
    // Verify: WitCqlValue -> Val::Variant -> WitCqlValue is lossless

    #[test]
    fn val_roundtrip_null() {
        let orig = WitCqlValue::Null;
        let val = wit_cql_value_to_val(&orig);
        assert_eq!(val, Val::Variant("null".into(), None));
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_scalars() {
        let cases = vec![
            WitCqlValue::IntVal(42),
            WitCqlValue::BigintVal(i64::MAX),
            WitCqlValue::FloatVal(1.5),
            WitCqlValue::DoubleVal(2.5),
            WitCqlValue::BooleanVal(true),
            WitCqlValue::BooleanVal(false),
            WitCqlValue::TextVal("hello world".into()),
            WitCqlValue::BlobVal(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            WitCqlValue::UuidVal("550e8400-e29b-41d4-a716-446655440000".into()),
            WitCqlValue::TimestampVal(1_700_000_000_000),
            WitCqlValue::DateVal(100),
            WitCqlValue::TimeVal(43_200_000_000_000),
            WitCqlValue::SmallintVal(-1234),
            WitCqlValue::TinyintVal(-42),
            WitCqlValue::InetVal("192.168.1.1".into()),
            WitCqlValue::AsciiVal("ASCII".into()),
            WitCqlValue::TimeuuidVal("550e8400-e29b-41d4-a716-446655440000".into()),
            WitCqlValue::CounterVal(999),
        ];
        for case in cases {
            let val = wit_cql_value_to_val(&case);
            let back = val_to_wit_cql_value(&val).unwrap();
            assert_eq!(back, case, "roundtrip failed for {case:?}");
        }
    }

    #[test]
    fn val_roundtrip_decimal() {
        let orig = WitCqlValue::DecimalVal(vec![1, 2, 3], 5);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_varint() {
        let orig = WitCqlValue::VarintVal(vec![0xFF, 0x01]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_duration() {
        let orig = WitCqlValue::DurationVal(1, 15, 3_600_000_000_000);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_list() {
        let orig = WitCqlValue::ListVal(vec![
            WitCqlValue::IntVal(1),
            WitCqlValue::IntVal(2),
            WitCqlValue::IntVal(3),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_set() {
        let orig = WitCqlValue::SetVal(vec![
            WitCqlValue::TextVal("a".into()),
            WitCqlValue::TextVal("b".into()),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_map() {
        let orig = WitCqlValue::MapVal(vec![
            (WitCqlValue::IntVal(1), WitCqlValue::TextVal("one".into())),
            (WitCqlValue::IntVal(2), WitCqlValue::TextVal("two".into())),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_tuple() {
        let orig = WitCqlValue::TupleVal(vec![
            WitCqlValue::IntVal(42),
            WitCqlValue::Null,
            WitCqlValue::TextVal("hello".into()),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_udt() {
        let orig = WitCqlValue::UdtVal(vec![
            ("street".into(), WitCqlValue::TextVal("123 Main".into())),
            ("zip".into(), WitCqlValue::IntVal(62701)),
            ("apt".into(), WitCqlValue::Null),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_nested_collections() {
        // list<list<int>> — tests recursive encoding
        let orig = WitCqlValue::ListVal(vec![
            WitCqlValue::ListVal(vec![WitCqlValue::IntVal(1), WitCqlValue::IntVal(2)]),
            WitCqlValue::ListVal(vec![WitCqlValue::IntVal(3)]),
        ]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_map_with_list_values() {
        // map<text, list<int>> — tests nested collections in map values
        let orig = WitCqlValue::MapVal(vec![(
            WitCqlValue::TextVal("key".into()),
            WitCqlValue::ListVal(vec![WitCqlValue::IntVal(1), WitCqlValue::IntVal(2)]),
        )]);
        let val = wit_cql_value_to_val(&orig);
        let back = val_to_wit_cql_value(&val).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn val_roundtrip_empty_collections() {
        let cases = vec![
            WitCqlValue::ListVal(vec![]),
            WitCqlValue::SetVal(vec![]),
            WitCqlValue::MapVal(vec![]),
            WitCqlValue::TupleVal(vec![]),
            WitCqlValue::UdtVal(vec![]),
        ];
        for case in cases {
            let val = wit_cql_value_to_val(&case);
            let back = val_to_wit_cql_value(&val).unwrap();
            assert_eq!(back, case, "empty collection roundtrip failed for {case:?}");
        }
    }

    #[test]
    fn val_result_ok_decodes() {
        // Simulate what a WASM component returns: Result<cql-value, string>
        let inner = wit_cql_value_to_val(&WitCqlValue::IntVal(42));
        let result_val = Val::Result(Ok(Some(Box::new(inner))));
        let cql = val_to_cql_result(&result_val, &CqlType::Int).unwrap();
        assert_eq!(cql, CqlValue::Int(42));
    }

    #[test]
    fn val_result_ok_null_decodes() {
        let result_val = Val::Result(Ok(None));
        let cql = val_to_cql_result(&result_val, &CqlType::Int).unwrap();
        assert_eq!(cql, CqlValue::Null);
    }

    #[test]
    fn val_result_err_decodes() {
        let result_val = Val::Result(Err(Some(Box::new(Val::String("division by zero".into())))));
        let err = val_to_cql_result(&result_val, &CqlType::Int).unwrap_err();
        match err {
            UdfError::ExecutionFailed(msg) => assert_eq!(msg, "division by zero"),
            other => panic!("expected ExecutionFailed, got {other:?}"),
        }
    }

    #[test]
    fn val_to_wit_rejects_non_variant() {
        // Raw scalars should fail since cql-value must be a Variant
        let err = val_to_wit_cql_value(&Val::S32(42)).unwrap_err();
        assert!(matches!(err, UdfError::TypeMismatch(..)));
    }

    #[test]
    fn val_to_wit_rejects_unknown_discriminant() {
        let err = val_to_wit_cql_value(&Val::Variant("bogus".into(), None)).unwrap_err();
        assert!(matches!(err, UdfError::TypeMismatch(ref msg) if msg.contains("unknown")));
    }

    #[test]
    fn wit_cql_list_to_val_encodes_args() {
        let args = vec![WitCqlValue::IntVal(1), WitCqlValue::TextVal("hi".into())];
        let val = wit_cql_list_to_val(&args);
        match val {
            Val::List(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Val::Variant(d, _) if d == "int-val"));
                assert!(matches!(&items[1], Val::Variant(d, _) if d == "text-val"));
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    /// Generate a minimal valid WASM component binary.
    /// This is the smallest valid component that wasmtime will accept.
    fn minimal_component_bytes() -> Vec<u8> {
        // Component header: magic + version + layer
        // \0asm = magic, 0d 00 = version 13, 01 00 = layer (component)
        vec![
            0x00, 0x61, 0x73, 0x6d, // \0asm
            0x0d, 0x00, // version 13
            0x01, 0x00, // layer = component
        ]
    }
}
