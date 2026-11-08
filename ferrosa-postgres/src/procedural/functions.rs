//! PL/pgSQL functions (D13): `CREATE [OR REPLACE] FUNCTION … LANGUAGE plpgsql` and
//! `DROP FUNCTION [IF EXISTS]`.
//!
//! A function is stored in the schema as a user function of language `plpgsql` through the
//! same schema-change path a CQL `CREATE FUNCTION` takes (`DdlExecutor::create_function`), so
//! it replicates the same way. Its body is a JSON envelope holding the PL/pgSQL source and
//! signature; each node compiles it on first use and caches the result by the hash of that
//! envelope, so every node runs the same compiled code for the same source.
//!
//! The function's identity is `(schema, name, argument types)`. A `RETURNS trigger` function
//! records the PostgreSQL pseudo-type `pg_catalog.trigger` as its return type (`void`
//! likewise) — a marker in the type system, never a real column type.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ferrosa_common::cql_type::CqlType;
use ferrosa_plpgsql::utility::{CreateFunction, QualifiedName};
use ferrosa_plpgsql::{FunctionSignature, Program, ReturnType};
use ferrosa_schema::UserFunctionMetadata;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::extended::Session;
use crate::messages::BackendMessage;
use crate::query::error_response;
use crate::server::QueryContext;

/// The `language` a PL/pgSQL function is stored under.
pub(crate) const LANGUAGE: &str = "plpgsql";
/// Compiled functions a node keeps (by source hash) before evicting one.
pub(crate) const CACHE_CAPACITY: usize = 256;

/// How this node runs PL/pgSQL functions. Chosen explicitly at startup and logged; never a
/// silent fallback.
pub enum FunctionBackend {
    /// D13: compile to a sandboxed component (`ferrosa-udf`). Compilation needs the
    /// `plpgsql-compile` build feature and the AssemblyScript toolchain bundle.
    Compiled(Arc<ferrosa_udf::UdfExecutor>),
    /// Run functions on the interpreter (the reference the compiled code is proved and tested
    /// against). For tests and builds without the compiler; selected by name, never by default.
    Interpreted,
    /// No way to run functions on this node: `CREATE FUNCTION` and trigger firing fail with
    /// `0A000` carrying this reason.
    Unavailable(String),
}

/// A function ready to run: its parsed program, and its compiled component when compiled.
pub(crate) struct Loaded {
    pub(crate) program: Arc<Program>,
    pub(crate) component: Option<Arc<ferrosa_udf::PlpgsqlComponent>>,
}

/// The stored body: the source and signature, as JSON.
#[derive(Debug, Serialize, Deserialize)]
struct StoredBody {
    v: u32,
    source: String,
    args: Vec<(Option<String>, String)>,
    returns: String,
    volatility: Option<String>,
}

impl StoredBody {
    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            args: self.args.clone(), // the signature is rebuilt for each parse of a cached body
            returns: match self.returns.as_str() {
                "trigger" => ReturnType::Trigger,
                "void" => ReturnType::Void,
                other => ReturnType::Scalar(other.to_string()),
            },
        }
    }
}

/// Compiled functions by source hash.
#[derive(Default)]
pub(crate) struct FunctionCache {
    entries: Mutex<HashMap<[u8; 32], Arc<Loaded>>>,
}

impl FunctionCache {
    fn get(&self, key: &[u8; 32]) -> Option<Arc<Loaded>> {
        lock(&self.entries).get(key).map(Arc::clone)
    }

    fn insert(&self, key: [u8; 32], loaded: Arc<Loaded>) {
        let mut entries = lock(&self.entries);
        if entries.len() >= CACHE_CAPACITY && !entries.contains_key(&key) {
            if let Some(victim) = entries.keys().next().copied() {
                tracing::debug!("PL/pgSQL function cache full; evicting one compiled function");
                entries.remove(&victim);
            }
        }
        entries.insert(key, loaded);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!("PL/pgSQL function cache lock poisoned; continuing with its contents");
            poisoned.into_inner()
        }
    }
}

fn marker(name: &str) -> CqlType {
    CqlType::Udt {
        keyspace: "pg_catalog".into(),
        name: name.into(),
        fields: Vec::new(),
    }
}

fn return_marker(returns: &ReturnType) -> Result<CqlType, BackendMessage> {
    match returns {
        ReturnType::Trigger => Ok(marker("trigger")),
        ReturnType::Void => Ok(marker("void")),
        ReturnType::Scalar(name) => cql_type(name),
    }
}

fn cql_type(name: &str) -> Result<CqlType, BackendMessage> {
    crate::pg_types::cql_type_for_pg_name(name)
        .map_err(|e| error_response("42704", &format!("type \"{name}\": {e}")))
}

fn returns_text(returns: &ReturnType) -> String {
    match returns {
        ReturnType::Trigger => "trigger".into(),
        ReturnType::Void => "void".into(),
        ReturnType::Scalar(name) => name.to_ascii_lowercase(),
    }
}

pub(crate) fn resolve_schema(ctx: &QueryContext, name: &QualifiedName) -> String {
    name.schema
        .clone() // owned keyspace name for the schema key
        .unwrap_or_else(|| ctx.default_schema.clone()) // owned keyspace name for the schema key
}

fn refuse_in_txn(session: &Session, what: &str) -> Result<(), BackendMessage> {
    if session.in_txn() {
        return Err(error_response(
            "25001",
            &format!("{what} cannot run inside a transaction block: DDL is not transactional here"),
        ));
    }
    Ok(())
}

fn ddl(ctx: &QueryContext, what: &str) -> Result<Arc<dyn crate::ddl::DdlExecutor>, BackendMessage> {
    ctx.ddl.as_ref().map(Arc::clone).ok_or_else(|| {
        error_response(
            "0A000",
            &format!("{what} is not available: this server has no schema-change path"),
        )
    })
}

/// `CREATE [OR REPLACE] FUNCTION … LANGUAGE plpgsql`.
pub(crate) async fn create(
    ctx: &QueryContext,
    session: &mut Session,
    statement: CreateFunction,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    refuse_in_txn(session, "CREATE FUNCTION")?;
    let executor = ddl(ctx, "CREATE FUNCTION")?;
    let CreateFunction {
        or_replace,
        name,
        signature,
        body,
        volatility,
    } = statement;
    let keyspace = resolve_schema(ctx, &name);
    crate::authz::authorize(
        &ctx.schema,
        session.auth(),
        &[(
            ferrosa_schema::auth::permission::Permission::Create,
            ferrosa_schema::auth::permission::Resource::Keyspace(keyspace.clone()), // owned for the resource key
        )],
    )?;
    let arg_types = signature
        .args
        .iter()
        .map(|(_, ty)| cql_type(ty))
        .collect::<Result<Vec<_>, _>>()?;
    let return_type = return_marker(&signature.returns)?;
    let stored = StoredBody {
        v: 1,
        args: signature.args,
        returns: returns_text(&signature.returns),
        source: body,
        volatility,
    };
    let envelope = serde_json::to_string(&stored)
        .map_err(|e| error_response("XX000", &format!("encoding function body: {e}")))?;
    // Parse and compile before touching the schema: a refused body never replaces a function.
    load_envelope(ctx, &envelope).await?;
    let existing = ctx.schema.get_function(&keyspace, &name.name, &arg_types);
    if let Some(existing) = &existing {
        if !or_replace {
            return Err(error_response(
                "42723",
                &format!(
                    "function \"{}\" already exists with the same argument types",
                    name.name
                ),
            ));
        }
        if existing.return_type != return_type {
            return Err(error_response(
                "42P13",
                "cannot change return type of existing function",
            ));
        }
    }
    let arg_names = stored
        .args
        .iter()
        .enumerate()
        .map(|(i, (n, _))| n.clone().unwrap_or_else(|| format!("${}", i + 1))) // names are stored alongside the envelope
        .collect();
    let metadata = UserFunctionMetadata {
        keyspace: keyspace.clone(), // the drop below needs it too
        name: name.name.clone(),    // the drop below needs it too
        arg_names,
        arg_types: arg_types.clone(), // the drop below needs them too
        return_type,
        called_on_null: true,
        language: LANGUAGE.into(),
        body: envelope,
    };
    if existing.is_some() {
        executor
            .drop_function(&keyspace, &name.name, arg_types)
            .await
            .map_err(|e| error_response("58000", &format!("replacing function: {e}")))?;
    }
    executor
        .create_function(metadata)
        .await
        .map_err(|e| error_response("58000", &format!("storing function: {e}")))?;
    Ok(vec![BackendMessage::CommandComplete {
        tag: "CREATE FUNCTION".into(),
    }])
}

/// `DROP FUNCTION [IF EXISTS] name [(types)]`.
pub(crate) async fn drop(
    ctx: &QueryContext,
    session: &mut Session,
    if_exists: bool,
    name: QualifiedName,
    arg_types: Option<Vec<String>>,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    refuse_in_txn(session, "DROP FUNCTION")?;
    let executor = ddl(ctx, "DROP FUNCTION")?;
    let keyspace = resolve_schema(ctx, &name);
    let wanted = match arg_types {
        Some(types) => Some(
            types
                .iter()
                .map(|t| cql_type(t))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        None => None,
    };
    let Some(found) = find(ctx, &keyspace, &name.name, wanted.as_deref())? else {
        if if_exists {
            return Ok(vec![BackendMessage::CommandComplete {
                tag: "DROP FUNCTION".into(),
            }]);
        }
        return Err(error_response(
            "42883",
            &format!("function {}.{} does not exist", keyspace, name.name),
        ));
    };
    if found.language != LANGUAGE {
        return Err(error_response(
            "0A000",
            &format!(
                "function \"{}\" is a {} function: drop it through CQL",
                name.name, found.language
            ),
        ));
    }
    crate::authz::authorize(
        &ctx.schema,
        session.auth(),
        &[(
            ferrosa_schema::auth::permission::Permission::Drop,
            ferrosa_schema::auth::permission::Resource::Keyspace(keyspace.clone()), // owned for the resource key
        )],
    )?;
    if let Some((trigger, table)) = super::triggers::dependent_trigger(ctx, &keyspace, &name.name)?
    {
        return Err(error_response(
            "2BP01",
            &format!(
                "cannot drop function {}() because trigger {trigger} on table {table} depends on it",
                name.name
            ),
        ));
    }
    executor
        .drop_function(&keyspace, &found.name, found.arg_types)
        .await
        .map_err(|e| error_response("58000", &format!("dropping function: {e}")))?;
    Ok(vec![BackendMessage::CommandComplete {
        tag: "DROP FUNCTION".into(),
    }])
}

/// The function `(keyspace, name)` with these argument types, or the only one by that name
/// when no types are given.
fn find(
    ctx: &QueryContext,
    keyspace: &str,
    name: &str,
    arg_types: Option<&[CqlType]>,
) -> Result<Option<UserFunctionMetadata>, BackendMessage> {
    if let Some(types) = arg_types {
        return Ok(ctx.schema.get_function(keyspace, name, types));
    }
    let snapshot = ctx.schema.snapshot();
    let mut matching = snapshot
        .functions
        .values()
        .filter(|f| f.keyspace == keyspace && f.name == name);
    let first = matching.next().cloned(); // copied out of the shared snapshot
    if matching.next().is_some() {
        return Err(error_response(
            "42725",
            &format!("function name \"{name}\" is not unique: give its argument types"),
        ));
    }
    Ok(first)
}

/// Load a stored PL/pgSQL function `(keyspace, name)` taking no arguments (a trigger function).
pub(crate) async fn load_trigger_function(
    ctx: &QueryContext,
    keyspace: &str,
    name: &str,
) -> Result<Arc<Loaded>, BackendMessage> {
    let Some(function) = ctx.schema.get_function(keyspace, name, &[]) else {
        return Err(error_response(
            "42883",
            &format!("function {keyspace}.{name}() does not exist"),
        ));
    };
    if function.language != LANGUAGE {
        return Err(error_response(
            "0A000",
            &format!("function {keyspace}.{name}() is not a PL/pgSQL function"),
        ));
    }
    let loaded = load_envelope(ctx, &function.body).await?;
    if loaded.program.kind != ferrosa_plpgsql::RoutineKind::Trigger {
        return Err(error_response(
            "42P17",
            &format!("function {keyspace}.{name} must return type trigger"),
        ));
    }
    Ok(loaded)
}

/// Parse and compile a stored body, through the cache.
async fn load_envelope(ctx: &QueryContext, envelope: &str) -> Result<Arc<Loaded>, BackendMessage> {
    let key: [u8; 32] = Sha256::digest(envelope.as_bytes()).into();
    if let Some(loaded) = ctx.procedural.functions.get(&key) {
        return Ok(loaded);
    }
    let stored: StoredBody = serde_json::from_str(envelope).map_err(|e| {
        tracing::error!(%e, "stored PL/pgSQL function body is unreadable");
        error_response(
            "XX001",
            &format!("stored PL/pgSQL function body is unreadable: {e}"),
        )
    })?;
    let program = ferrosa_plpgsql::parse_function(&stored.source, &stored.signature())
        .map_err(super::pl_error)?;
    let component = compile(&ctx.procedural.backend, &program).await?;
    let loaded = Arc::new(Loaded {
        program: Arc::new(program),
        component,
    });
    ctx.procedural.functions.insert(key, Arc::clone(&loaded));
    Ok(loaded)
}

async fn compile(
    backend: &FunctionBackend,
    program: &Program,
) -> Result<Option<Arc<ferrosa_udf::PlpgsqlComponent>>, BackendMessage> {
    match backend {
        FunctionBackend::Interpreted => Ok(None),
        FunctionBackend::Unavailable(reason) => Err(error_response(
            "0A000",
            &format!("PL/pgSQL functions cannot run on this node: {reason}"),
        )),
        FunctionBackend::Compiled(executor) => compile_component(executor, program).await.map(Some),
    }
}

#[cfg(feature = "plpgsql-compile")]
async fn compile_component(
    executor: &Arc<ferrosa_udf::UdfExecutor>,
    program: &Program,
) -> Result<Arc<ferrosa_udf::PlpgsqlComponent>, BackendMessage> {
    let program = program.clone(); // the compiler runs on a blocking thread and needs its own copy
    let executor = Arc::clone(executor);
    let compiled = tokio::task::spawn_blocking(move || {
        let bytes = ferrosa_udf::component::compile_plpgsql(&program)
            .map_err(|e| format!("compiling PL/pgSQL: {e}"))?;
        executor
            .load_plpgsql(&bytes)
            .map_err(|e| format!("loading compiled PL/pgSQL: {e}"))
    })
    .await
    .map_err(|e| error_response("XX000", &format!("PL/pgSQL compiler thread failed: {e}")))?;
    compiled.map_err(|e| {
        tracing::error!(error = %e, "PL/pgSQL compilation failed");
        error_response("0A000", &e)
    })
}

#[cfg(not(feature = "plpgsql-compile"))]
async fn compile_component(
    _executor: &Arc<ferrosa_udf::UdfExecutor>,
    _program: &Program,
) -> Result<Arc<ferrosa_udf::PlpgsqlComponent>, BackendMessage> {
    Err(error_response(
        "0A000",
        "PL/pgSQL compilation needs a ferrosa built with the `plpgsql-compile` feature",
    ))
}

/// Run a loaded function against the session (compiled when it was compiled).
pub(crate) async fn call(
    ctx: &QueryContext,
    session: &mut Session,
    loaded: &Arc<Loaded>,
    call: ferrosa_plpgsql::Call,
    depth: usize,
) -> Result<Option<ferrosa_plpgsql::Value>, ferrosa_plpgsql::SqlError> {
    if let Some(component) = &loaded.component {
        let FunctionBackend::Compiled(executor) = &ctx.procedural.backend else {
            return Err(ferrosa_plpgsql::SqlError::new(
                "XX000",
                "a compiled PL/pgSQL function was loaded without a compiling backend",
            ));
        };
        let executor = Arc::clone(executor);
        let component = Arc::clone(component);
        return super::host::run(ctx, session, depth, move |host| {
            executor.call_plpgsql(&component, call, Box::new(host)).0
        })
        .await;
    }
    let program = Arc::clone(&loaded.program);
    super::host::run(ctx, session, depth, move |mut host| {
        ferrosa_plpgsql::interpret(&program, call, &mut host)
    })
    .await
}
