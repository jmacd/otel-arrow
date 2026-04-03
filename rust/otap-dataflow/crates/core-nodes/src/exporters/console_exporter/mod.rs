// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Console exporter that prints OTLP data with hierarchical formatting.

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NodeControlMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::OtapPayload;
use otap_df_pdata::views::otap::OtapLogsView;
use otap_df_pdata::views::otap::OtapMetricsView;
use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
use otap_df_pdata_views::views::common::InstrumentationScopeView;
use otap_df_pdata_views::views::logs::{
    LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView,
};
use otap_df_pdata_views::views::metrics::{
    DataType, DataView, GaugeView, MetricView, MetricsView, NumberDataPointView,
    ResourceMetricsView, ScopeMetricsView, SumView, Value,
};
use otap_df_pdata_views::views::resource::ResourceView;
use otap_df_telemetry::otel_error;
use otap_df_telemetry::self_tracing::{AnsiCode, ColorMode, LOG_BUFFER_SIZE, StyledBufWriter};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// The URN for the console exporter
pub const CONSOLE_EXPORTER_URN: &str = "urn:otel:exporter:console";

/// Configuration for the console exporter
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ConsoleExporterConfig {
    /// Whether to use ANSI colors in output (default: true)
    #[serde(default = "default_color")]
    pub color: bool,
    /// Whether to use Unicode box-drawing characters (default: true)
    #[serde(default = "default_unicode")]
    pub unicode: bool,
}

const fn default_color() -> bool {
    true
}

const fn default_unicode() -> bool {
    true
}

/// Console exporter that prints OTLP data with hierarchical formatting
pub struct ConsoleExporter {
    formatter: HierarchicalFormatter,
}

impl ConsoleExporter {
    /// Create a new console exporter with the given configuration.
    #[must_use]
    pub const fn new(config: ConsoleExporterConfig) -> Self {
        Self {
            formatter: HierarchicalFormatter::new(config.color, config.unicode),
        }
    }
}

/// Declare the Console Exporter as a local exporter factory
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static CONSOLE_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: CONSOLE_EXPORTER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig| {
        let config: ConsoleExporterConfig = serde_json::from_value(node_config.config.clone())
            .map_err(|e| ConfigError::InvalidUserConfig {
                error: format!("Failed to parse console exporter config: {}", e),
            })?;
        Ok(ExporterWrapper::local(
            ConsoleExporter::new(config),
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<ConsoleExporterConfig>,
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ConsoleExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => break,
                Message::PData(data) => {
                    self.export(data.payload_ref()).await;
                    effect_handler.notify_ack(AckMsg::new(data)).await?;
                }
                _ => {
                    // do nothing
                }
            }
        }

        Ok(TerminalState::default())
    }
}

impl ConsoleExporter {
    async fn export(&self, payload: &OtapPayload) {
        match payload.signal_type() {
            SignalType::Logs => self.export_logs(payload).await,
            SignalType::Traces => self.export_traces(payload).await,
            SignalType::Metrics => self.export_metrics(payload).await,
        }
    }

    async fn export_logs(&self, payload: &OtapPayload) {
        match payload {
            OtapPayload::OtlpBytes(bytes) => match RawLogsData::try_from(bytes) {
                Ok(logs_view) => {
                    self.formatter.print_logs_data(&logs_view).await;
                }
                Err(e) => {
                    otel_error!("console.logs_view.otlp_create_failed", error = ?e, message = "Failed to create OTLP logs view");
                }
            },
            OtapPayload::OtapArrowRecords(records) => match OtapLogsView::try_from(records) {
                Ok(logs_view) => {
                    self.formatter.print_logs_data(&logs_view).await;
                }
                Err(e) => {
                    otel_error!("console.logs_view.otap_create_failed", error = ?e, message = "Failed to create OTAP logs view");
                }
            },
        }
    }

    async fn export_traces(&self, _payload: &OtapPayload) {
        // TODO: Implement traces formatting.
        otel_error!(
            "console.traces.not_implemented",
            message = "Traces formatting not yet implemented"
        );
    }

    async fn export_metrics(&self, payload: &OtapPayload) {
        match payload {
            OtapPayload::OtlpBytes(_bytes) => {
                // OTLP bytes metrics view not yet available; convert to OTAP first.
                otel_error!(
                    "console.metrics.otlp_bytes_unsupported",
                    message = "OTLP bytes metrics display not yet supported; use OTAP Arrow format"
                );
            }
            OtapPayload::OtapArrowRecords(records) => match OtapMetricsView::try_from(records) {
                Ok(metrics_view) => {
                    self.formatter.print_metrics_data(&metrics_view).await;
                }
                Err(e) => {
                    otel_error!("console.metrics_view.otap_create_failed", error = ?e, message = "Failed to create OTAP metrics view");
                }
            },
        }
    }
}

/// Tree drawing characters (Unicode or ASCII).
#[derive(Clone, Copy)]
struct TreeChars {
    vertical: &'static str,
    tee: &'static str,
    corner: &'static str,
}

impl TreeChars {
    const UNICODE: Self = Self {
        vertical: "│",
        tee: "├─",
        corner: "└─",
    };
    const ASCII: Self = Self {
        vertical: "|",
        tee: "+-",
        corner: "\\-",
    };
}

/// Hierarchical formatter for OTLP data.
pub struct HierarchicalFormatter {
    color: ColorMode,
    tree: TreeChars,
}

impl HierarchicalFormatter {
    /// Create a new hierarchical formatter.
    #[must_use]
    pub const fn new(use_color: bool, use_unicode: bool) -> Self {
        Self {
            color: if use_color {
                ColorMode::Color
            } else {
                ColorMode::NoColor
            },
            tree: if use_unicode {
                TreeChars::UNICODE
            } else {
                TreeChars::ASCII
            },
        }
    }

    /// Format logs from OTLP bytes to a writer.
    pub async fn print_logs_data<L: LogsDataView>(&self, logs_data: &L) {
        let mut output = Vec::new();
        self.format_logs_data_to(logs_data, &mut output);

        use tokio::io::AsyncWriteExt;

        if let Err(err) = tokio::io::stdout().write_all(&output).await {
            otel_error!("console.write_failed", error = ?err, message = "Could not write to console");
        }
    }

    /// Format logs from a LogsDataView to a writer.
    fn format_logs_data_to<L: LogsDataView>(&self, logs_data: &L, output: &mut Vec<u8>) {
        for resource_logs in logs_data.resources() {
            self.format_resource_logs_to(&resource_logs, output);
        }
    }

    /// Format a ResourceLogs with its nested scopes.
    fn format_resource_logs_to<R: ResourceLogsView>(
        &self,
        resource_logs: &R,
        output: &mut Vec<u8>,
    ) {
        let first_ts = self.get_first_log_timestamp(resource_logs);

        // Format resource header
        self.format_line(output, |w| {
            w.format_header_line(
                Some(first_ts),
                resource_logs.resource().iter().flat_map(|r| r.attributes()),
                |w| {
                    w.write_styled(AnsiCode::Cyan, |w| {
                        let _ = w.write_all(b"RESOURCE");
                    });
                    let _ = w.write_all(b"   ");
                },
                |w| {
                    let _ = w.write_all(b"v1.Resource");
                },
                |_| {}, // No line suffix.
            );
        });

        // Format each scope
        let mut scopes = resource_logs.scopes().peekable();
        while let Some(scope_logs) = scopes.next() {
            let is_last_scope = scopes.peek().is_none();
            self.format_scope_logs_to(&scope_logs, is_last_scope, output);
        }
    }

    /// Get the first timestamp from log records in a ResourceLogs.
    fn get_first_log_timestamp<R: ResourceLogsView>(&self, resource_logs: &R) -> SystemTime {
        for scope_logs in resource_logs.scopes() {
            for log_record in scope_logs.log_records() {
                if let Some(ts) = log_record.time_unix_nano() {
                    return nanos_to_time(ts);
                }
                if let Some(ts) = log_record.observed_time_unix_nano() {
                    return nanos_to_time(ts);
                }
            }
        }
        SystemTime::UNIX_EPOCH
    }

    /// Format a ScopeLogs with its nested log records.
    fn format_scope_logs_to<S: ScopeLogsView>(
        &self,
        scope_logs: &S,
        is_last_scope: bool,
        output: &mut Vec<u8>,
    ) {
        let first_ts = scope_logs
            .log_records()
            .find_map(|lr| lr.time_unix_nano().or_else(|| lr.observed_time_unix_nano()))
            .map(nanos_to_time)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let prefix = self.tree.vertical;
        let scope = scope_logs.scope();

        // Extract name/version for inline display (keep as raw bytes to avoid allocation)
        let name = scope.as_ref().and_then(|s| s.name());
        let version = scope.as_ref().and_then(|s| s.version());

        self.format_line(output, |w| {
            w.format_header_line(
                Some(first_ts),
                scope.iter().flat_map(|s| s.attributes()),
                |w| {
                    let _ = w.write_all(prefix.as_bytes());
                    let _ = w.write_all(b" ");
                    w.write_styled(AnsiCode::Magenta, |w| {
                        let _ = w.write_all(b"SCOPE");
                    });
                    let _ = w.write_all(b"    ");
                },
                |w| match (name, version) {
                    (Some(n), Some(v)) => {
                        let _ = w.write_all(n);
                        let _ = w.write_all(b"/");
                        let _ = w.write_all(v);
                    }
                    (Some(n), None) => {
                        let _ = w.write_all(n);
                    }
                    _ => {
                        let _ = w.write_all(b"v1.InstrumentationScope");
                    }
                },
                |_| {}, // No line suffix.
            );
        });

        // Format each log record
        let mut records = scope_logs.log_records().peekable();
        while let Some(log_record) = records.next() {
            let is_last_record = records.peek().is_none();
            self.format_log_record_to(&log_record, is_last_scope, is_last_record, output);
        }
    }

    /// Format a single log record.
    fn format_log_record_to<L: LogRecordView>(
        &self,
        log_record: &L,
        is_last_scope: bool,
        is_last_record: bool,
        output: &mut Vec<u8>,
    ) {
        let time = log_record
            .time_unix_nano()
            .or_else(|| log_record.observed_time_unix_nano())
            .map(nanos_to_time)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let event_name = log_record
            .event_name()
            .map(|s| String::from_utf8_lossy(s).into_owned());

        let severity = log_record.severity_number();
        let severity_text = log_record.severity_text();
        let tree = self.tree;

        self.format_line(output, |w| {
            w.format_log_line(
                Some(time),
                log_record,
                |w| {
                    let _ = w.write_all(tree.vertical.as_bytes());
                    let _ = w.write_all(b" ");
                    if is_last_record && is_last_scope {
                        let _ = w.write_all(tree.corner.as_bytes());
                    } else {
                        let _ = w.write_all(tree.tee.as_bytes());
                    }
                    let _ = w.write_all(b" ");
                    w.write_severity(severity, severity_text.as_ref().map(|s| s.as_ref()));
                },
                |w| {
                    if let Some(name) = event_name {
                        let _ = w.write_all(name.as_bytes());
                    }
                },
                |_| {}, // No line suffix (scope printed above).
            );
        });
    }

    /// Format a line to the output buffer.
    fn format_line<F>(&self, output: &mut Vec<u8>, f: F)
    where
        F: FnOnce(&mut StyledBufWriter<'_>),
    {
        let mut buf = [0u8; LOG_BUFFER_SIZE];
        let mut w = StyledBufWriter::new(&mut buf, self.color);
        f(&mut w);
        let len = w.position();
        output.extend_from_slice(&buf[..len]);
    }

    // --- Metrics formatting ---

    /// Print metrics data to stdout.
    pub async fn print_metrics_data<M: MetricsView>(&self, metrics_data: &M) {
        let mut output = Vec::new();
        self.format_metrics_data_to(metrics_data, &mut output);

        use tokio::io::AsyncWriteExt;

        if let Err(err) = tokio::io::stdout().write_all(&output).await {
            otel_error!("console.write_failed", error = ?err, message = "Could not write metrics to console");
        }
    }

    /// Format metrics from a MetricsView to a byte buffer.
    fn format_metrics_data_to<M: MetricsView>(&self, metrics_data: &M, output: &mut Vec<u8>) {
        for resource_metrics in metrics_data.resources() {
            self.format_resource_metrics_to(&resource_metrics, output);
        }
    }

    /// Format a ResourceMetrics with its nested scopes.
    fn format_resource_metrics_to<R: ResourceMetricsView>(
        &self,
        resource_metrics: &R,
        output: &mut Vec<u8>,
    ) {
        let first_ts = self.get_first_metric_timestamp(resource_metrics);

        self.format_line(output, |w| {
            w.format_header_line(
                Some(first_ts),
                resource_metrics
                    .resource()
                    .iter()
                    .flat_map(|r| r.attributes()),
                |w| {
                    w.write_styled(AnsiCode::Cyan, |w| {
                        let _ = w.write_all(b"RESOURCE");
                    });
                    let _ = w.write_all(b"   ");
                },
                |w| {
                    let _ = w.write_all(b"v1.Resource");
                },
                |_| {},
            );
        });

        let mut scopes = resource_metrics.scopes().peekable();
        while let Some(scope_metrics) = scopes.next() {
            let is_last_scope = scopes.peek().is_none();
            self.format_scope_metrics_to(&scope_metrics, is_last_scope, output);
        }
    }

    /// Get the first timestamp from metrics in a ResourceMetrics.
    fn get_first_metric_timestamp<R: ResourceMetricsView>(
        &self,
        resource_metrics: &R,
    ) -> SystemTime {
        for scope_metrics in resource_metrics.scopes() {
            for metric in scope_metrics.metrics() {
                if let Some(data) = metric.data() {
                    if let Some(ts) = Self::first_data_point_time(&data) {
                        return nanos_to_time(ts);
                    }
                }
            }
        }
        SystemTime::UNIX_EPOCH
    }

    /// Extract the first timestamp from a DataView.
    fn first_data_point_time<'a, D: DataView<'a>>(data: &D) -> Option<u64> {
        match data.value_type() {
            DataType::Gauge => data
                .as_gauge()
                .and_then(|g| g.data_points().next().map(|dp| dp.time_unix_nano())),
            DataType::Sum => data
                .as_sum()
                .and_then(|s| s.data_points().next().map(|dp| dp.time_unix_nano())),
            _ => None,
        }
    }

    /// Format a ScopeMetrics with its nested metrics.
    fn format_scope_metrics_to<S: ScopeMetricsView>(
        &self,
        scope_metrics: &S,
        is_last_scope: bool,
        output: &mut Vec<u8>,
    ) {
        let prefix = self.tree.vertical;
        let scope = scope_metrics.scope();
        let name = scope.as_ref().and_then(|s| s.name());
        let version = scope.as_ref().and_then(|s| s.version());

        self.format_line(output, |w| {
            w.format_header_line(
                None,
                scope.iter().flat_map(|s| s.attributes()),
                |w| {
                    let _ = w.write_all(prefix.as_bytes());
                    let _ = w.write_all(b" ");
                    w.write_styled(AnsiCode::Magenta, |w| {
                        let _ = w.write_all(b"SCOPE");
                    });
                    let _ = w.write_all(b"    ");
                },
                |w| match (name, version) {
                    (Some(n), Some(v)) => {
                        let _ = w.write_all(n);
                        let _ = w.write_all(b"/");
                        let _ = w.write_all(v);
                    }
                    (Some(n), None) => {
                        let _ = w.write_all(n);
                    }
                    _ => {
                        let _ = w.write_all(b"v1.InstrumentationScope");
                    }
                },
                |_| {},
            );
        });

        let mut metrics = scope_metrics.metrics().peekable();
        while let Some(metric) = metrics.next() {
            let is_last_metric = metrics.peek().is_none();
            self.format_metric_to(&metric, is_last_scope, is_last_metric, output);
        }
    }

    /// Format a single metric with its data points.
    fn format_metric_to<MV: MetricView>(
        &self,
        metric: &MV,
        is_last_scope: bool,
        is_last_metric: bool,
        output: &mut Vec<u8>,
    ) {
        let name = metric.name();
        let unit = metric.unit();
        let tree = self.tree;

        let data = match metric.data() {
            Some(d) => d,
            None => return,
        };

        let type_label = match data.value_type() {
            DataType::Gauge => "gauge",
            DataType::Sum => "sum",
            DataType::Histogram => "histogram",
            DataType::ExponentialHistogram => "exp_histogram",
            DataType::Summary => "summary",
        };

        // Collect data points for display.
        let points = Self::collect_number_data_points(&data);

        for (i, (ts_nanos, value_str, dp_attrs)) in points.iter().enumerate() {
            let is_last_point = i == points.len() - 1;
            let time = nanos_to_time(*ts_nanos);

            self.format_line(output, |w| {
                // Tree prefix
                let _ = w.write_all(tree.vertical.as_bytes());
                let _ = w.write_all(b" ");
                if is_last_metric && is_last_scope && is_last_point {
                    let _ = w.write_all(tree.corner.as_bytes());
                } else {
                    let _ = w.write_all(tree.tee.as_bytes());
                }
                let _ = w.write_all(b" ");

                // Timestamp
                w.write_timestamp(time);
                let _ = w.write_all(b" ");

                // Type badge
                w.write_styled(AnsiCode::Yellow, |w| {
                    let _ = w.write_all(type_label.as_bytes());
                });
                let _ = w.write_all(b" ");

                // Metric name
                w.write_styled(AnsiCode::Bold, |w| {
                    let _ = w.write_all(name);
                });

                // Unit
                if !unit.is_empty() {
                    let _ = w.write_all(b" (");
                    let _ = w.write_all(unit);
                    let _ = w.write_all(b")");
                }

                // Value
                let _ = w.write_all(b" = ");
                let _ = w.write_all(value_str.as_bytes());

                // Data point attributes
                if !dp_attrs.is_empty() {
                    let _ = w.write_all(b" [");
                    let _ = w.write_all(dp_attrs.as_bytes());
                    let _ = w.write_all(b"]");
                }

                let _ = w.write_all(b"\n");
            });
        }
    }

    /// Collect number data points from a DataView as (timestamp, value_string, attrs_string).
    fn collect_number_data_points<'a, D: DataView<'a>>(data: &D) -> Vec<(u64, String, String)> {
        let mut points = Vec::new();

        match data.value_type() {
            DataType::Gauge => {
                if let Some(gauge) = data.as_gauge() {
                    for dp in gauge.data_points() {
                        points.push(Self::format_number_data_point(&dp));
                    }
                }
            }
            DataType::Sum => {
                if let Some(sum) = data.as_sum() {
                    for dp in sum.data_points() {
                        points.push(Self::format_number_data_point(&dp));
                    }
                }
            }
            _ => {
                // Histogram/Summary: show type only for now.
                points.push((0, "(unsupported type)".to_string(), String::new()));
            }
        }

        if points.is_empty() {
            points.push((0, "(no data points)".to_string(), String::new()));
        }
        points
    }

    /// Format a single NumberDataPoint as (timestamp, value_string, attrs_string).
    fn format_number_data_point<N: NumberDataPointView>(dp: &N) -> (u64, String, String) {
        let ts = dp.time_unix_nano();
        let value_str = match dp.value() {
            Some(Value::Integer(v)) => format!("{v}"),
            Some(Value::Double(v)) => format!("{v}"),
            None => "null".to_string(),
        };
        let attrs: Vec<String> = dp
            .attributes()
            .map(|a| {
                use otap_df_pdata_views::views::common::{AnyValueView, AttributeView};
                let key = String::from_utf8_lossy(a.key()).into_owned();
                let val = a
                    .value()
                    .map(|v| {
                        v.as_string()
                            .map(|s| String::from_utf8_lossy(s).into_owned())
                            .or_else(|| v.as_int64().map(|i| format!("{i}")))
                            .or_else(|| v.as_double().map(|d| format!("{d}")))
                            .or_else(|| v.as_bool().map(|b| format!("{b}")))
                            .unwrap_or_else(|| "?".to_string())
                    })
                    .unwrap_or_default();
                format!("{key}={val}")
            })
            .collect();
        (ts, value_str, attrs.join(","))
    }
}

/// Convert nanoseconds since UNIX_EPOCH to SystemTime.
#[inline]
fn nanos_to_time(nanos: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::testing::fixtures::logs_with_full_resource_and_scope;
    use prost::Message;

    #[test]
    fn test_format_logs() {
        let logs_data = logs_with_full_resource_and_scope();
        let bytes = OtlpProtoBytes::ExportLogsRequest(logs_data.encode_to_vec().into());
        let formatter = HierarchicalFormatter::new(false, true);

        let mut output = Vec::new();
        let logs_view = RawLogsData::try_from(&bytes).expect("logs");
        formatter.format_logs_data_to(&logs_view, &mut output);

        let text = String::from_utf8_lossy(&output);

        // The fixture creates two scopes with two logs each:
        // - scope-alpha/1.0.0: INFO + WARN
        // - scope-beta/2.0.0: ERROR + DEBUG
        let expected = "\
2025-01-15T10:30:00.000Z  RESOURCE   v1.Resource [res.id=self]
2025-01-15T10:30:00.000Z  │ SCOPE    scope-alpha/1.0.0 [scopekey=scopeval]
2025-01-15T10:30:00.000Z  │ ├─ INFO  event_1: first log in alpha
2025-01-15T10:30:01.000Z  │ ├─ WARN  second log in alpha
2025-01-15T10:30:02.000Z  │ SCOPE    scope-beta/2.0.0
2025-01-15T10:30:02.000Z  │ ├─ HOTHOT first log in beta
2025-01-15T10:30:03.000Z  │ └─ DEBUG event_2: [detail=no body here]
";
        assert_eq!(text, expected);
    }
}
