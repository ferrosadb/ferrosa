//! Adapter from the CQL WASM executor to storage's RRD aggregate hook.

use std::sync::Arc;

use ferrosa_common::CqlType;
use ferrosa_storage::{TimeSeriesWasmAggregateExecutor, TimeSeriesWasmAggregateInvocation};
use ferrosa_udf::{StreamingAggregateInvocation, UdfExecutor};

pub struct UdfTimeSeriesAggregateExecutor {
    udf_executor: Arc<UdfExecutor>,
}

impl UdfTimeSeriesAggregateExecutor {
    pub fn new(udf_executor: Arc<UdfExecutor>) -> Self {
        Self { udf_executor }
    }
}

impl TimeSeriesWasmAggregateExecutor for UdfTimeSeriesAggregateExecutor {
    fn start(
        &self,
        keyspace: &str,
        function_name: &str,
        arg_type: &str,
    ) -> Result<Box<dyn TimeSeriesWasmAggregateInvocation>, String> {
        let arg_type = rrd_arg_type_to_cql(arg_type)?;
        let invocation = self
            .udf_executor
            .start_streaming_aggregate(keyspace, function_name, &[arg_type])
            .map_err(|err| err.to_string())?;
        Ok(Box::new(UdfTimeSeriesAggregateInvocation { invocation }))
    }
}

struct UdfTimeSeriesAggregateInvocation {
    invocation: StreamingAggregateInvocation,
}

impl TimeSeriesWasmAggregateInvocation for UdfTimeSeriesAggregateInvocation {
    fn update(&mut self, value: f64) -> Result<(), String> {
        self.invocation.update(value).map_err(|err| err.to_string())
    }

    fn finalize(self: Box<Self>) -> Result<f64, String> {
        self.invocation.finalize().map_err(|err| err.to_string())
    }
}

fn rrd_arg_type_to_cql(arg_type: &str) -> Result<CqlType, String> {
    match arg_type {
        "double" => Ok(CqlType::Double),
        "float" => Ok(CqlType::Float),
        "int" => Ok(CqlType::Int),
        "bigint" | "counter" | "timestamp" => Ok(CqlType::Bigint),
        other => Err(format!(
            "unsupported RRD WASM aggregate argument type: {other}"
        )),
    }
}
