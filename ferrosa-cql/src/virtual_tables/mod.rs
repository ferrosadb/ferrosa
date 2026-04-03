//! Virtual table implementations for `ferrosa-cql`.
//!
//! Each submodule provides a concrete `VirtualTable` implementation backed
//! by live runtime state. Tables are registered in the `VirtualTableRegistry`
//! during server startup so they are visible to CQL `SELECT` queries.

pub mod active_queries;
pub mod alerts;
pub mod billing;
pub mod connections;
pub mod consolidation_status;
pub mod full_scan_reasons;
pub mod query_fingerprints;
pub mod stubs;
pub mod table_access;

pub use active_queries::{ActiveQueriesTable, QueryGuard, QueryTracker};
pub use alerts::{AlertRegistry, AlertSeverity, AlertsTable};
pub use billing::{BillingMeter, BillingMetersTable};
pub use connections::{ConnectionInfo, ConnectionTracker, ConnectionsTable};
pub use consolidation_status::ConsolidationStatusTable;
pub use full_scan_reasons::{FullScanReasonsTable, FullScanTracker};
pub use query_fingerprints::{QueryFingerprintTracker, QueryFingerprintsTable};
pub use stubs::register_all_stubs;
pub use table_access::{TableAccessSummaryTable, TableAccessTracker};
