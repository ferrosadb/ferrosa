//! Billing metering virtual table (T-28: O5.1).
//!
//! Per-request byte tracking: `bytes_in` (request frame size) and
//! `bytes_out` (response frame size), aggregated per client_address +
//! keyspace in 1-minute buckets.
//!
//! Virtual table: `system_observability.billing_meters`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Maximum number of distinct billing buckets to keep in memory.
const MAX_BUCKETS: usize = 100_000;

/// Key for a billing bucket: (client_address, keyspace, minute_epoch).
type BucketKey = (String, String, u64);

/// Atomic counters for a single billing bucket.
pub struct BillingBucket {
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub request_count: AtomicU64,
}

impl BillingBucket {
    fn new() -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            request_count: AtomicU64::new(0),
        }
    }
}

/// Concurrent billing meter tracker.
pub struct BillingMeter {
    buckets: DashMap<BucketKey, Arc<BillingBucket>>,
}

impl BillingMeter {
    /// Create a new empty meter.
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// Get the current minute epoch (seconds since epoch, floored to minute).
    fn current_minute() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60 * 60)
            .unwrap_or(0)
    }

    /// Record bytes for a request.
    pub fn record(&self, client_address: &str, keyspace: &str, bytes_in: u64, bytes_out: u64) {
        let minute = Self::current_minute();
        let key = (client_address.to_string(), keyspace.to_string(), minute);

        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| Arc::new(BillingBucket::new()))
            .clone();

        bucket.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        bucket.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
        bucket.request_count.fetch_add(1, Ordering::Relaxed);

        // Evict old buckets if over capacity.
        if self.buckets.len() > MAX_BUCKETS {
            self.evict_oldest();
        }
    }

    /// Evict the oldest minute bucket.
    fn evict_oldest(&self) {
        let mut oldest_key = None;
        let mut oldest_minute = u64::MAX;

        for entry in self.buckets.iter() {
            let minute = entry.key().2;
            if minute < oldest_minute {
                oldest_minute = minute;
                oldest_key = Some(entry.key().clone());
            }
        }

        if let Some(key) = oldest_key {
            self.buckets.remove(&key);
        }
    }

    /// Number of active billing buckets.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}

impl Default for BillingMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.billing_meters`
pub struct BillingMetersTable {
    meter: Arc<BillingMeter>,
    columns: Vec<VirtualColumnDef>,
}

impl BillingMetersTable {
    pub fn new(meter: Arc<BillingMeter>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "client_address".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "keyspace".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "minute_epoch".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "bytes_in".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "bytes_out".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "request_count".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { meter, columns }
    }
}

impl VirtualTable for BillingMetersTable {
    fn name(&self) -> &str {
        "billing_meters"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1, 2]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        self.meter
            .buckets
            .iter()
            .map(|entry| {
                let (client, ks, minute) = entry.key();
                let b = entry.value();
                let cells = vec![
                    CellValue::live(client.as_bytes().to_vec(), 0),
                    CellValue::live(ks.as_bytes().to_vec(), 0),
                    CellValue::live((*minute as i64).to_be_bytes().to_vec(), 0),
                    CellValue::live(
                        (b.bytes_in.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (b.bytes_out.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (b.request_count.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                ];
                VirtualRow { cells }
            })
            .collect()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn billing_meter_records_and_aggregates() {
        let meter = Arc::new(BillingMeter::new());
        meter.record("10.0.0.1", "myks", 100, 200);
        meter.record("10.0.0.1", "myks", 50, 100);
        // Same client/keyspace/minute should aggregate into 1 bucket.
        assert_eq!(meter.bucket_count(), 1);

        let table = BillingMetersTable::new(meter);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        // bytes_in should be 150
        let bin_bytes = rows[0].cells[3].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(bin_bytes.try_into().unwrap()), 150);
        // bytes_out should be 300
        let bout_bytes = rows[0].cells[4].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(bout_bytes.try_into().unwrap()), 300);
        // request_count should be 2
        let count_bytes = rows[0].cells[5].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(count_bytes.try_into().unwrap()), 2);
    }

    #[test]
    fn billing_different_clients_separate() {
        let meter = BillingMeter::new();
        meter.record("10.0.0.1", "ks", 10, 20);
        meter.record("10.0.0.2", "ks", 10, 20);
        assert_eq!(meter.bucket_count(), 2);
    }

    #[test]
    fn billing_table_metadata() {
        let meter = Arc::new(BillingMeter::new());
        let table = BillingMetersTable::new(meter);
        assert_eq!(table.name(), "billing_meters");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 6);
    }
}
