use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::codec::Lane;
use crate::rpc::client::orphan_response_count;

const LANES: [Lane; 3] = [Lane::Raft, Lane::Data, Lane::Bulk];
const DEFAULT_DATA_LANE_MAX_IN_FLIGHT: usize = 256;

static RPC_REQUESTS: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static RPC_TIMEOUTS: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static RPC_IN_FLIGHT: [AtomicI64; 3] = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
static LANE_QUEUE_WAITS_NS: [AtomicU64; 3] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static LANE_ENQUEUES: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DATA_LANE_REJECTED: AtomicU64 = AtomicU64::new(0);
static DATA_LANE_ACTIVE: AtomicUsize = AtomicUsize::new(0);
static DATA_LANE_MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(DEFAULT_DATA_LANE_MAX_IN_FLIGHT);

pub fn record_lane_queue_wait(lane: Lane, wait: Duration) {
    LANE_ENQUEUES[lane.index()].fetch_add(1, Ordering::Relaxed);
    let nanos = wait.as_nanos().min(u64::MAX as u128) as u64;
    LANE_QUEUE_WAITS_NS[lane.index()].fetch_add(nanos, Ordering::Relaxed);
}

pub fn try_start_rpc(lane: Lane, data_lane_max_in_flight: usize) -> bool {
    if lane == Lane::Data {
        let cap = data_lane_max_in_flight.max(1);
        DATA_LANE_MAX_IN_FLIGHT.store(cap, Ordering::Relaxed);
        let mut current = DATA_LANE_ACTIVE.load(Ordering::Relaxed);
        loop {
            if current >= cap {
                DATA_LANE_REJECTED.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            match DATA_LANE_ACTIVE.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    RPC_REQUESTS[lane.index()].fetch_add(1, Ordering::Relaxed);
    RPC_IN_FLIGHT[lane.index()].fetch_add(1, Ordering::Relaxed);
    true
}

pub fn finish_rpc(lane: Lane) {
    RPC_IN_FLIGHT[lane.index()].fetch_sub(1, Ordering::Relaxed);
    if lane == Lane::Data {
        DATA_LANE_ACTIVE.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn record_rpc_timeout(lane: Lane) {
    RPC_TIMEOUTS[lane.index()].fetch_add(1, Ordering::Relaxed);
}

pub fn render_prometheus() -> String {
    let mut output = String::new();
    output.push_str(
        "# HELP ferrosa_net_rpc_requests_total Internode RPC requests started by lane.\n",
    );
    output.push_str("# TYPE ferrosa_net_rpc_requests_total counter\n");
    output.push_str(
        "# HELP ferrosa_net_rpc_timeouts_total Internode RPC request timeouts by lane.\n",
    );
    output.push_str("# TYPE ferrosa_net_rpc_timeouts_total counter\n");
    output.push_str(
        "# HELP ferrosa_net_rpc_in_flight Internode RPC requests currently in flight by lane.\n",
    );
    output.push_str("# TYPE ferrosa_net_rpc_in_flight gauge\n");
    output.push_str(
        "# HELP ferrosa_net_lane_enqueue_total Internode lane actor enqueue attempts by lane.\n",
    );
    output.push_str("# TYPE ferrosa_net_lane_enqueue_total counter\n");
    output.push_str("# HELP ferrosa_net_lane_queue_wait_seconds_total Total time callers waited for lane actor queue capacity.\n");
    output.push_str("# TYPE ferrosa_net_lane_queue_wait_seconds_total counter\n");

    for lane in LANES {
        let label = lane.as_str();
        let index = lane.index();
        output.push_str(&format!(
            "ferrosa_net_rpc_requests_total{{lane=\"{label}\"}} {}\n",
            RPC_REQUESTS[index].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "ferrosa_net_rpc_timeouts_total{{lane=\"{label}\"}} {}\n",
            RPC_TIMEOUTS[index].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "ferrosa_net_rpc_in_flight{{lane=\"{label}\"}} {}\n",
            RPC_IN_FLIGHT[index].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "ferrosa_net_lane_enqueue_total{{lane=\"{label}\"}} {}\n",
            LANE_ENQUEUES[index].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "ferrosa_net_lane_queue_wait_seconds_total{{lane=\"{label}\"}} {:.9}\n",
            LANE_QUEUE_WAITS_NS[index].load(Ordering::Relaxed) as f64 / 1_000_000_000.0
        ));
    }

    output.push_str("# HELP ferrosa_net_data_lane_rejected_total Data lane RPCs rejected by local backpressure.\n");
    output.push_str("# TYPE ferrosa_net_data_lane_rejected_total counter\n");
    output.push_str(&format!(
        "ferrosa_net_data_lane_rejected_total {}\n",
        DATA_LANE_REJECTED.load(Ordering::Relaxed)
    ));
    output.push_str("# HELP ferrosa_net_data_lane_max_in_flight Configured process-wide data lane in-flight cap.\n");
    output.push_str("# TYPE ferrosa_net_data_lane_max_in_flight gauge\n");
    output.push_str(&format!(
        "ferrosa_net_data_lane_max_in_flight {}\n",
        DATA_LANE_MAX_IN_FLIGHT.load(Ordering::Relaxed)
    ));
    output.push_str("# HELP ferrosa_net_rpc_orphan_responses_total RPC responses received after caller timeout or cancellation.\n");
    output.push_str("# TYPE ferrosa_net_rpc_orphan_responses_total counter\n");
    output.push_str(&format!(
        "ferrosa_net_rpc_orphan_responses_total {}\n",
        orphan_response_count()
    ));

    output
}
