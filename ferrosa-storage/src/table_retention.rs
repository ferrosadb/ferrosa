//! Incarnation-bound retained table directories. No automatic resurrection or GC.
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use ferrosa_common::{Error, Result, TableSchema};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const BINDING: &str = ".retained-table.json";
const ROOT: &str = "retired-tables";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    schema: TableSchema,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidFormat(message.into())
}

pub(crate) fn identity(schema: &TableSchema) -> Result<Uuid> {
    schema.extensions.get("ferrosa.table_id")
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or_else(|| invalid("recoverable retirement requires a valid physical table UUID"))
}

fn table_path(root: &Path, schema: &TableSchema) -> Result<PathBuf> {
    let name = format!("{}.{}", schema.keyspace, schema.table);
    let mut parts = Path::new(&name).components();
    if !matches!(parts.next(), Some(std::path::Component::Normal(_))) || parts.next().is_some() {
        return Err(invalid("invalid retained table name"));
    }
    Ok(root.join("sstables").join(name))
}

fn directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(invalid(format!("{} is not a real directory", path.display()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(path)?;
            File::open(path.parent().ok_or_else(|| invalid("directory has no parent"))?)?.sync_all()?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// Stable metadata fence, never moved with a retired table. The caller also
/// holds its existing schema lifecycle writer and the node's directory ownership.
pub(crate) fn lock(root: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options.open(root.join(".table-retention.lock"))?;
    if !file.metadata()?.is_file() {
        return Err(invalid("table retention lock is not a regular file"));
    }
    file.lock_exclusive()?;
    Ok(file)
}

fn load(directory: &Path) -> Result<Option<Binding>> {
    let path = directory.join(BINDING);
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        if !metadata.file_type().is_file() {
            return Err(invalid("retained table binding is not a regular file"));
        }
    }
    let binding: Option<Binding> = crate::schema_snapshot::load_bounded_json(directory, BINDING)?;
    if let Some(binding) = &binding {
        if binding.version != 1 {
            return Err(invalid("unsupported retained table binding version"));
        }
        identity(&binding.schema)?;
    }
    Ok(binding)
}

fn matches(binding: &Binding, schema: &TableSchema, exact: bool) -> Result<()> {
    if binding.schema.keyspace != schema.keyspace || binding.schema.table != schema.table
        || identity(&binding.schema)? != identity(schema)? {
        return Err(invalid("retained physical table identity does not match requested schema"));
    }
    if exact && serde_json::to_value(&binding.schema)? != serde_json::to_value(schema)? {
        return Err(invalid("retained physical layout does not match explicit recovery schema"));
    }
    Ok(())
}

/// Registration may use an explicitly restored binding but must never silently
/// replace a still-retained incarnation with an empty directory.
pub(crate) fn validate_registration(root: &Path, schema: &TableSchema) -> Result<()> {
    let active = table_path(root, schema)?;
    if let Some(binding) = load(&active)? {
        matches(&binding, schema, false)?;
        if crate::table_drops::is_pending_sweep(root, &schema.keyspace, &schema.table)? {
            return Err(invalid("retained incarnation conflicts with legacy pending sweep; explicit recovery required"));
        }
    }
    if let Ok(id) = identity(schema) {
        if root.join(ROOT).join(id.to_string()).join("data").try_exists()? {
            return Err(invalid("physical incarnation is retained; explicit recovery required before registration"));
        }
    }
    Ok(())
}

pub(crate) struct Retirement {
    active: PathBuf,
    retained: PathBuf,
}

impl Retirement {
    pub(crate) fn prepare(root: &Path, schema: &TableSchema) -> Result<Self> {
        let id = identity(schema)?;
        let active = table_path(root, schema)?;
        if !std::fs::symlink_metadata(&active)?.file_type().is_dir() {
            return Err(invalid("physical table directory is not a real directory"));
        }
        if crate::table_drops::is_pending_sweep(root, &schema.keyspace, &schema.table)? {
            return Err(invalid("legacy pending sweep prevents recoverable retirement"));
        }
        if let Some(binding) = load(&active)? {
            matches(&binding, schema, true)?;
        }
        let retained_root = root.join(ROOT);
        directory(&retained_root)?;
        let destination = retained_root.join(id.to_string());
        directory(&destination)?;
        let retained = destination.join("data");
        if retained.try_exists()? {
            return Err(invalid("retained incarnation already has data; refusing overwrite"));
        }
        crate::schema_snapshot::persist_bounded_json(&active, BINDING, &Binding { version: 1, schema: schema.clone() })?;
        Ok(Self { active, retained })
    }

    pub(crate) fn move_data(self) -> Result<()> {
        std::fs::rename(&self.active, &self.retained)?;
        File::open(self.active.parent().ok_or_else(|| invalid("physical table has no parent"))?)?.sync_all()?;
        File::open(self.retained.parent().ok_or_else(|| invalid("retention has no parent"))?)?.sync_all()?;
        Ok(())
    }
}

/// Restore only the requested historical layout. Caller must hold `lock` and
/// the same schema lifecycle writer as registration and DROP. No table is
/// registered here; the engine owns that final step under this same lease.
pub(crate) fn restore(root: &Path, schema: &TableSchema) -> Result<()> {
    let retained = root.join(ROOT).join(identity(schema)?.to_string()).join("data");
    let active = table_path(root, schema)?;
    let source_exists = retained.try_exists()?;
    let active_exists = active.try_exists()?;
    if source_exists && active_exists {
        return Err(invalid("both retained and active physical directories exist; refusing ambiguous restore"));
    }
    if active_exists {
        let binding = load(&active)?.ok_or_else(|| invalid("existing directory is not an explicit retained recovery"))?;
        return matches(&binding, schema, true);
    }
    let binding = load(&retained)?.ok_or_else(|| invalid("requested retained incarnation is missing its binding"))?;
    matches(&binding, schema, true)?;
    std::fs::rename(&retained, &active)?;
    File::open(retained.parent().ok_or_else(|| invalid("retained data has no parent"))?)?.sync_all()?;
    File::open(active.parent().ok_or_else(|| invalid("active data has no parent"))?)?.sync_all()?;
    Ok(())
}
