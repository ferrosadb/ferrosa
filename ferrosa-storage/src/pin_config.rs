//! Configuration for NVMe table pinning, parsed from table extensions.

use std::collections::HashMap;

/// Pin mode for a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinMode {
    /// No pinning — uses default storage tier.
    None,
    /// Pinned to local NVMe storage.
    NvMe,
}

/// Configuration for NVMe pinning on a table.
#[derive(Debug, Clone)]
pub struct PinConfig {
    /// The active pin mode.
    pub mode: PinMode,
    /// Optional cap on bytes pinned to NVMe for this table.
    pub max_bytes: Option<u64>,
}

impl PinConfig {
    /// Parse from table extensions map.
    ///
    /// Returns a `PinConfig` with `PinMode::None` when no `storage.pin` key is present.
    /// An unrecognised `storage.pin` value is treated as `PinMode::None`.
    /// An unparseable `storage.pin_max_bytes` value is silently ignored (returns `None`).
    pub fn from_extensions(ext: &HashMap<String, String>) -> Self {
        let mode = match ext.get("storage.pin").map(|s| s.as_str()) {
            Some("nvme") => PinMode::NvMe,
            _ => PinMode::None,
        };
        let max_bytes = ext
            .get("storage.pin_max_bytes")
            .and_then(|v| v.parse().ok());
        Self { mode, max_bytes }
    }

    /// Returns `true` if the table is pinned to any storage tier.
    pub fn is_pinned(&self) -> bool {
        matches!(self.mode, PinMode::NvMe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_config_parses_nvme() {
        let mut ext = HashMap::new();
        ext.insert("storage.pin".to_string(), "nvme".to_string());
        let config = PinConfig::from_extensions(&ext);
        assert!(config.is_pinned());
        assert!(matches!(config.mode, PinMode::NvMe));
    }

    #[test]
    fn pin_config_default_none() {
        let config = PinConfig::from_extensions(&HashMap::new());
        assert!(!config.is_pinned());
        assert!(matches!(config.mode, PinMode::None));
    }

    #[test]
    fn pin_config_parses_max_bytes() {
        let mut ext = HashMap::new();
        ext.insert("storage.pin".to_string(), "nvme".to_string());
        ext.insert(
            "storage.pin_max_bytes".to_string(),
            "10737418240".to_string(),
        );
        let config = PinConfig::from_extensions(&ext);
        assert_eq!(config.max_bytes, Some(10737418240));
    }

    #[test]
    fn pin_config_invalid_max_bytes_ignored() {
        let mut ext = HashMap::new();
        ext.insert("storage.pin".to_string(), "nvme".to_string());
        ext.insert(
            "storage.pin_max_bytes".to_string(),
            "not_a_number".to_string(),
        );
        let config = PinConfig::from_extensions(&ext);
        assert_eq!(config.max_bytes, None);
    }
}
