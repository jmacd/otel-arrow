// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Passive metrics accumulator and Prometheus renderer.
//!
//! [`MetricsTap`] accumulates metric snapshots fed by an external actor
//! (admin periodic task or the internal telemetry receiver). It converts
//! delta counters to cumulative values and renders Prometheus text
//! exposition on demand.
//!
//! The tap is **not** an actor — it never calls `visit_metrics_and_reset`
//! itself. The owning actor is responsible for reading from the registry
//! and feeding the tap.

use std::collections::HashMap;
use std::fmt::Write as _;

use crate::attributes::AttributeSetHandler;
use crate::descriptor::{Instrument, MetricsDescriptor, MetricsField, Temporality};
use crate::instrument::MmscSnapshot;
use crate::metrics::{MetricValue, MetricsIterator};
use crate::self_metrics::views::ViewResolver;

/// Identifies a single timeseries: (scope_name, instrument_name, label_set).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TimeseriesKey {
    scope_name: &'static str,
    field_name: &'static str,
    labels: String,
}

/// Accumulated state for a single scalar timeseries.
#[derive(Debug, Clone, Copy)]
enum AccumulatedValue {
    /// Cumulative counter (delta → cumulative).
    CumulativeU64(u64),
    CumulativeF64(f64),
    /// Last-value gauge.
    GaugeU64(u64),
    GaugeF64(f64),
}

/// Accumulated MMSC state for a single timeseries.
#[derive(Debug, Clone, Copy)]
struct AccumulatedMmsc {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl AccumulatedMmsc {
    fn merge(&mut self, s: &MmscSnapshot) {
        if s.min < self.min {
            self.min = s.min;
        }
        if s.max > self.max {
            self.max = s.max;
        }
        self.sum += s.sum;
        self.count += s.count;
    }
}

/// Passive metrics accumulator and Prometheus text renderer.
///
/// Maintains cumulative state for all observed timeseries.
/// Thread-safety: wrap in `Arc<parking_lot::Mutex<MetricsTap>>` or
/// `Arc<parking_lot::RwLock<MetricsTap>>` as needed by the caller.
#[derive(Debug)]
pub struct MetricsTap {
    /// View resolver for instrument renaming.
    views: ViewResolver,

    /// Accumulated scalar values keyed by timeseries identity.
    scalars: HashMap<TimeseriesKey, AccumulatedValue>,

    /// Accumulated MMSC values keyed by timeseries identity.
    mmscs: HashMap<TimeseriesKey, AccumulatedMmsc>,
}

impl MetricsTap {
    /// Creates a new empty `MetricsTap` with the given view configuration.
    #[must_use]
    pub fn new(views: ViewResolver) -> Self {
        Self {
            views,
            scalars: HashMap::new(),
            mmscs: HashMap::new(),
        }
    }

    /// Returns a reference to the view resolver.
    #[must_use]
    pub fn views(&self) -> &ViewResolver {
        &self.views
    }

    /// Feed a single metric set snapshot into the tap.
    ///
    /// The caller provides the descriptor, attributes, and an iterator
    /// of (field, value) pairs — the same signature as the closure in
    /// `visit_metrics_and_reset`.
    pub fn feed(
        &mut self,
        descriptor: &'static MetricsDescriptor,
        attributes: &dyn AttributeSetHandler,
        metrics_iter: MetricsIterator<'_>,
    ) {
        let labels = self.format_labels(descriptor, attributes);

        for (field, value) in metrics_iter {
            let key = TimeseriesKey {
                scope_name: descriptor.name,
                field_name: field.name,
                labels: labels.clone(),
            };
            self.feed_scalar_or_mmsc(field, value, key);
        }
    }

    /// Feed a metric set from pre-collected values.
    ///
    /// Like `feed`, but takes a `MetricValue` slice instead of a
    /// `MetricsIterator`. Used when values have been collected during
    /// a registry visit and need to be fed to the tap afterwards.
    pub fn feed_values(
        &mut self,
        descriptor: &'static MetricsDescriptor,
        attributes: &dyn AttributeSetHandler,
        values: &[MetricValue],
    ) {
        let labels = self.format_labels(descriptor, attributes);

        for (field, value) in descriptor.metrics.iter().zip(values) {
            let key = TimeseriesKey {
                scope_name: descriptor.name,
                field_name: field.name,
                labels: labels.clone(),
            };
            self.feed_scalar_or_mmsc(field, *value, key);
        }
    }

    /// Accumulate a single metric value into the tap.
    fn feed_scalar_or_mmsc(
        &mut self,
        field: &MetricsField,
        value: MetricValue,
        key: TimeseriesKey,
    ) {
        match value {
            MetricValue::U64(v) => {
                let entry = self
                    .scalars
                    .entry(key)
                    .or_insert_with(|| match field.instrument {
                        Instrument::Counter | Instrument::UpDownCounter => {
                            match field.temporality {
                                Some(Temporality::Delta) => AccumulatedValue::CumulativeU64(0),
                                _ => AccumulatedValue::GaugeU64(0),
                            }
                        }
                        Instrument::Gauge | Instrument::Histogram => AccumulatedValue::GaugeU64(0),
                        Instrument::Mmsc => unreachable!("U64 value for Mmsc instrument"),
                    });
                match entry {
                    AccumulatedValue::CumulativeU64(acc) => *acc += v,
                    AccumulatedValue::GaugeU64(acc) => *acc = v,
                    AccumulatedValue::CumulativeF64(acc) => *acc += v as f64,
                    AccumulatedValue::GaugeF64(acc) => *acc = v as f64,
                }
            }
            MetricValue::F64(v) => {
                let entry = self
                    .scalars
                    .entry(key)
                    .or_insert_with(|| match field.instrument {
                        Instrument::Counter | Instrument::UpDownCounter => {
                            match field.temporality {
                                Some(Temporality::Delta) => AccumulatedValue::CumulativeF64(0.0),
                                _ => AccumulatedValue::GaugeF64(0.0),
                            }
                        }
                        Instrument::Gauge | Instrument::Histogram => {
                            AccumulatedValue::GaugeF64(0.0)
                        }
                        Instrument::Mmsc => unreachable!("F64 value for Mmsc instrument"),
                    });
                match entry {
                    AccumulatedValue::CumulativeF64(acc) => *acc += v,
                    AccumulatedValue::GaugeF64(acc) => *acc = v,
                    AccumulatedValue::CumulativeU64(acc) => *acc += v as u64,
                    AccumulatedValue::GaugeU64(acc) => *acc = v as u64,
                }
            }
            MetricValue::Mmsc(s) => {
                let entry = self.mmscs.entry(key).or_insert(AccumulatedMmsc {
                    min: f64::MAX,
                    max: f64::MIN,
                    sum: 0.0,
                    count: 0,
                });
                entry.merge(&s);
            }
        }
    }

    /// Feed all metric sets from the registry (visit-and-reset pattern).
    ///
    /// This is a convenience method: the caller passes their registry
    /// handle and this method drives the visit.
    pub fn feed_from_registry(&mut self, registry: &crate::registry::TelemetryRegistryHandle) {
        registry.visit_metrics_and_reset(|desc, attrs, iter| {
            self.feed(desc, attrs, iter);
        });
    }

    /// Feed from registry and return a console-formatted string of the
    /// delta values consumed in this tick.
    ///
    /// Unlike `render_console()` (which shows cumulative state), this
    /// returns only the values from the current registry visit — what
    /// changed since the last reset.
    pub fn feed_from_registry_with_console(
        &mut self,
        registry: &crate::registry::TelemetryRegistryHandle,
    ) -> String {
        let mut out = String::new();
        registry.visit_metrics_and_reset(|desc, attrs, iter| {
            let labels = self.format_labels(desc, attrs);
            for (field, value) in iter {
                if value.is_zero() {
                    // Still feed the tap for Prometheus accumulation.
                    let key = TimeseriesKey {
                        scope_name: desc.name,
                        field_name: field.name,
                        labels: labels.clone(),
                    };
                    self.feed_scalar_or_mmsc(field, value, key);
                    continue;
                }
                let resolved = self.views.resolve(desc.name, field.name, "");
                match value {
                    MetricValue::U64(v) => {
                        if labels.is_empty() {
                            let _ = writeln!(&mut out, "{}: {v}", resolved.name);
                        } else {
                            let _ =
                                writeln!(&mut out, "{}{{{}}} {v}", resolved.name, labels);
                        }
                    }
                    MetricValue::F64(v) => {
                        let v_str = format_f64(v);
                        if labels.is_empty() {
                            let _ = writeln!(&mut out, "{}: {v_str}", resolved.name);
                        } else {
                            let _ = writeln!(
                                &mut out,
                                "{}{{{}}} {v_str}",
                                resolved.name, labels
                            );
                        }
                    }
                    MetricValue::Mmsc(s) => {
                        if labels.is_empty() {
                            let _ = writeln!(
                                &mut out,
                                "{}: min={} max={} sum={} count={}",
                                resolved.name,
                                format_f64(s.min),
                                format_f64(s.max),
                                format_f64(s.sum),
                                s.count
                            );
                        } else {
                            let _ = writeln!(
                                &mut out,
                                "{}{{{}}} min={} max={} sum={} count={}",
                                resolved.name,
                                labels,
                                format_f64(s.min),
                                format_f64(s.max),
                                format_f64(s.sum),
                                s.count
                            );
                        }
                    }
                }
                // Also feed the tap for Prometheus accumulation.
                let key = TimeseriesKey {
                    scope_name: desc.name,
                    field_name: field.name,
                    labels: labels.clone(),
                };
                self.feed_scalar_or_mmsc(field, value, key);
            }
        });
        out
    }

    /// Render Prometheus text exposition format.
    ///
    /// Returns the full text body suitable for an HTTP `/metrics` response.
    #[must_use]
    pub fn render_prometheus(&self, timestamp_millis: Option<i64>) -> String {
        let mut out = String::new();
        let ts_suffix = timestamp_millis
            .map(|ms| format!(" {ms}"))
            .unwrap_or_default();

        // Track which metric names have had HELP/TYPE emitted.
        let mut seen = std::collections::HashSet::new();

        // Render scalars — group by (scope_name, field_name) for HELP/TYPE.
        let mut scalar_entries: Vec<_> = self.scalars.iter().collect();
        scalar_entries.sort_by_key(|(k, _)| (k.scope_name, k.field_name));

        for (key, value) in &scalar_entries {
            let resolved = self.views.resolve(
                key.scope_name,
                key.field_name,
                "", /* brief looked up below */
            );
            let metric_name = sanitize_prom_metric_name(&resolved.name);

            if seen.insert(metric_name.clone()) {
                let prom_type = match value {
                    AccumulatedValue::CumulativeU64(_) | AccumulatedValue::CumulativeF64(_) => {
                        "counter"
                    }
                    AccumulatedValue::GaugeU64(_) | AccumulatedValue::GaugeF64(_) => "gauge",
                };
                if !resolved.description.is_empty() {
                    let _ = writeln!(
                        &mut out,
                        "# HELP {} {}",
                        metric_name,
                        escape_prom_help(&resolved.description)
                    );
                }
                let _ = writeln!(&mut out, "# TYPE {metric_name} {prom_type}");
            }

            let value_str = match value {
                AccumulatedValue::CumulativeU64(v) | AccumulatedValue::GaugeU64(v) => {
                    format!("{v}")
                }
                AccumulatedValue::CumulativeF64(v) | AccumulatedValue::GaugeF64(v) => {
                    format_f64(*v)
                }
            };

            if key.labels.is_empty() {
                let _ = writeln!(&mut out, "{metric_name} {value_str}{ts_suffix}");
            } else {
                let _ = writeln!(
                    &mut out,
                    "{metric_name}{{{}}} {value_str}{ts_suffix}",
                    key.labels
                );
            }
        }

        // Render MMSC as histogram-style metrics.
        let mut mmsc_entries: Vec<_> = self.mmscs.iter().collect();
        mmsc_entries.sort_by_key(|(k, _)| (k.scope_name, k.field_name));

        for (key, mmsc) in &mmsc_entries {
            if mmsc.count == 0 {
                continue;
            }
            let resolved = self.views.resolve(key.scope_name, key.field_name, "");
            let metric_name = sanitize_prom_metric_name(&resolved.name);
            let brief = escape_prom_help(&resolved.description);

            for (suffix, prom_type, val) in [
                ("_min", "gauge", mmsc.min),
                ("_max", "gauge", mmsc.max),
                ("_sum", "counter", mmsc.sum),
            ] {
                let sub_name = format!("{metric_name}{suffix}");
                if seen.insert(sub_name.clone()) {
                    if !brief.is_empty() {
                        let _ = writeln!(&mut out, "# HELP {sub_name} {brief}");
                    }
                    let _ = writeln!(&mut out, "# TYPE {sub_name} {prom_type}");
                }
                if key.labels.is_empty() {
                    let _ = writeln!(&mut out, "{sub_name} {}{ts_suffix}", format_f64(val));
                } else {
                    let _ = writeln!(
                        &mut out,
                        "{sub_name}{{{}}} {}{ts_suffix}",
                        key.labels,
                        format_f64(val)
                    );
                }
            }
            let count_name = format!("{metric_name}_count");
            if seen.insert(count_name.clone()) {
                if !brief.is_empty() {
                    let _ = writeln!(&mut out, "# HELP {count_name} {brief}");
                }
                let _ = writeln!(&mut out, "# TYPE {count_name} counter");
            }
            if key.labels.is_empty() {
                let _ = writeln!(&mut out, "{count_name} {}{ts_suffix}", mmsc.count);
            } else {
                let _ = writeln!(
                    &mut out,
                    "{count_name}{{{}}} {}{ts_suffix}",
                    key.labels, mmsc.count
                );
            }
        }

        out
    }

    /// Render a compact console-friendly summary of current metrics.
    #[must_use]
    pub fn render_console(&self) -> String {
        let mut out = String::new();

        let mut scalar_entries: Vec<_> = self.scalars.iter().collect();
        scalar_entries.sort_by_key(|(k, _)| (k.scope_name, k.field_name));

        for (key, value) in &scalar_entries {
            let resolved = self.views.resolve(key.scope_name, key.field_name, "");
            let value_str = match value {
                AccumulatedValue::CumulativeU64(v) | AccumulatedValue::GaugeU64(v) => {
                    format!("{v}")
                }
                AccumulatedValue::CumulativeF64(v) | AccumulatedValue::GaugeF64(v) => {
                    format_f64(*v)
                }
            };
            if key.labels.is_empty() {
                let _ = writeln!(&mut out, "{}: {value_str}", resolved.name);
            } else {
                let _ = writeln!(&mut out, "{}{{{}}} {value_str}", resolved.name, key.labels);
            }
        }

        let mut mmsc_entries: Vec<_> = self.mmscs.iter().collect();
        mmsc_entries.sort_by_key(|(k, _)| (k.scope_name, k.field_name));

        for (key, mmsc) in &mmsc_entries {
            if mmsc.count == 0 {
                continue;
            }
            let resolved = self.views.resolve(key.scope_name, key.field_name, "");
            if key.labels.is_empty() {
                let _ = writeln!(
                    &mut out,
                    "{}: min={} max={} sum={} count={}",
                    resolved.name,
                    format_f64(mmsc.min),
                    format_f64(mmsc.max),
                    format_f64(mmsc.sum),
                    mmsc.count
                );
            } else {
                let _ = writeln!(
                    &mut out,
                    "{}{{{}}} min={} max={} sum={} count={}",
                    resolved.name,
                    key.labels,
                    format_f64(mmsc.min),
                    format_f64(mmsc.max),
                    format_f64(mmsc.sum),
                    mmsc.count
                );
            }
        }

        out
    }

    /// Build the label string for a metric set.
    fn format_labels(
        &self,
        descriptor: &'static MetricsDescriptor,
        attributes: &dyn AttributeSetHandler,
    ) -> String {
        let mut labels = String::new();
        if !descriptor.name.is_empty() {
            let _ = write!(
                &mut labels,
                "set=\"{}\"",
                escape_prom_label_value(descriptor.name)
            );
        }
        for (key, value) in attributes.iter_attributes() {
            if !labels.is_empty() {
                labels.push(',');
            }
            let _ = write!(
                &mut labels,
                "{}=\"{}\"",
                sanitize_prom_label_key(key),
                escape_prom_label_value(&value.to_string_value())
            );
        }
        labels
    }
}

// --- Prometheus formatting helpers ---

fn sanitize_prom_metric_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_prom_label_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn escape_prom_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

fn escape_prom_help(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

fn format_f64(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if v == v.trunc() && v.abs() < 1e15 {
        // Integer-like values: avoid trailing decimals.
        format!("{v:.0}")
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attributes::{AttributeSetHandler, AttributeValue};
    use crate::descriptor::{
        AttributeField, AttributeValueType, AttributesDescriptor, Instrument, MetricValueType,
        MetricsField, Temporality,
    };

    static TEST_DESC: MetricsDescriptor = MetricsDescriptor {
        name: "test_scope",
        metrics: &[
            MetricsField {
                name: "counter1",
                unit: "1",
                brief: "A test counter",
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::U64,
            },
            MetricsField {
                name: "gauge1",
                unit: "By",
                brief: "A test gauge",
                instrument: Instrument::Gauge,
                temporality: None,
                value_type: MetricValueType::U64,
            },
        ],
    };

    static TEST_ATTRS_DESC: AttributesDescriptor = AttributesDescriptor {
        name: "test_attrs",
        fields: &[AttributeField {
            key: "node.id",
            r#type: AttributeValueType::String,
            brief: "Node ID",
        }],
    };

    struct TestAttrs(Vec<AttributeValue>);
    impl AttributeSetHandler for TestAttrs {
        fn descriptor(&self) -> &'static AttributesDescriptor {
            &TEST_ATTRS_DESC
        }
        fn attribute_values(&self) -> &[AttributeValue] {
            &self.0
        }
    }

    #[test]
    fn feed_and_render_prometheus() {
        let mut tap = MetricsTap::new(ViewResolver::default());
        let attrs = TestAttrs(vec![AttributeValue::String("node_a".into())]);
        let values = [MetricValue::U64(42), MetricValue::U64(100)];
        let iter = MetricsIterator::new(TEST_DESC.metrics, &values);

        tap.feed(&TEST_DESC, &attrs, iter);

        let prom = tap.render_prometheus(None);
        assert!(prom.contains("counter1"), "expected counter1 in:\n{prom}");
        assert!(prom.contains("42"), "expected value 42 in:\n{prom}");
        assert!(prom.contains("gauge1"), "expected gauge1 in:\n{prom}");
        assert!(prom.contains("100"), "expected value 100 in:\n{prom}");
        assert!(
            prom.contains("node_id=\"node_a\""),
            "expected label in:\n{prom}"
        );
    }

    #[test]
    fn feed_from_registry_integration() {
        use crate::metrics::{MetricSet, MetricSetHandler};
        use crate::registry::TelemetryRegistryHandle;

        #[derive(Debug)]
        struct TestMetrics {
            values: Vec<MetricValue>,
        }
        impl Default for TestMetrics {
            fn default() -> Self {
                Self {
                    values: vec![MetricValue::U64(0), MetricValue::U64(0)],
                }
            }
        }
        impl MetricSetHandler for TestMetrics {
            fn descriptor(&self) -> &'static MetricsDescriptor {
                &TEST_DESC
            }
            fn snapshot_values(&self) -> Vec<MetricValue> {
                self.values.clone()
            }
            fn clear_values(&mut self) {
                self.values.iter_mut().for_each(MetricValue::reset);
            }
            fn needs_flush(&self) -> bool {
                self.values.iter().any(|&v| !v.is_zero())
            }
        }

        let registry = TelemetryRegistryHandle::new();
        let attrs = TestAttrs(vec![AttributeValue::String("n1".into())]);
        let metric_set: MetricSet<TestMetrics> = registry.register_metric_set(attrs);
        let key = metric_set.key;

        registry
            .accumulate_metric_set_snapshot(key, &[MetricValue::U64(10), MetricValue::U64(50)]);

        let mut tap = MetricsTap::new(ViewResolver::default());
        tap.feed_from_registry(&registry);

        let prom = tap.render_prometheus(None);
        assert!(prom.contains("counter1"), "expected counter1 in:\n{prom}");
        assert!(prom.contains("10"), "expected 10 in:\n{prom}");
        assert!(prom.contains("gauge1"), "expected gauge1 in:\n{prom}");
        assert!(prom.contains("50"), "expected 50 in:\n{prom}");
    }
}
