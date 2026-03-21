//! TimeSeriesAggregator (WriteObserver) and ConsolidationWorker.
//!
//! The aggregator inserts into ring buffers inline on the write path and
//! sends consolidation tasks to an async worker via a bounded channel.
