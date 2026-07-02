// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Lazy-versus-eager Parquet break-even model.
//!
//! The premise of the study elsewhere in this module is that a vendor accepts
//! Parquet, so the gateway encodes it eagerly on the write path. That pays the
//! flatten and Parquet-write cost on *every* file, which is the price of having
//! the file indexed and query-ready before anyone reads it. If the average file
//! is read fewer than once, it may be cheaper to write Arrow IPC to disk and
//! convert to Parquet lazily, on the first read, so files that are never read
//! never pay the conversion.
//!
//! This module is the cost arithmetic only; the benchmark measures the per-step
//! CPU and byte sizes and feeds them in. It answers: at what average read rate
//! `R` does eager become cheaper than lazy (the break-even `R*`)?
//!
//! # The two strategies, per file
//!
//! Let `R` be the expected number of reads of a file over its lifetime. Reads
//! are modeled as Poisson with mean `R`, so the probability a file is read at
//! least once (which is what triggers a lazy conversion) is `p = 1 - e^{-R}`.
//!
//! - **Eager**: pay `flatten + pq_write` once on the write path; transport and
//!   store the Parquet bytes; each of the `R` reads pays `pq_read`.
//! - **Lazy**: pay `ipc_encode` once on the write path; transport and store the
//!   Arrow IPC bytes; on the first read (probability `p`) pay the deferred
//!   conversion `ipc_decode + flatten + pq_write` and rewrite the file as
//!   Parquet; thereafter reads pay `pq_read`.
//!
//! # Why the per-read term cancels
//!
//! Both strategies serve reads from Parquet (lazy converts on the first read),
//! so the per-read cost `pq_read + read-I/O` is identical and drops out of the
//! comparison. What remains is a write-path and at-rest trade:
//!
//! - Lazy saves the eager `flatten + pq_write` on the write path, replacing it
//!   with the much cheaper `ipc_encode`.
//! - Lazy pays the conversion `ipc_decode + flatten + pq_write` only with
//!   probability `p`, i.e. only on files that are actually read.
//! - **Large-batch regime (the assumption here):** warm Arrow IPC is larger than
//!   the equivalent Parquet file, so lazy pays *more* for transport and for
//!   storage-at-rest until (and unless) a file is converted. Both of those work
//!   against lazy and are what pull the break-even `R*` down.
//!
//! So lazy wins when the write-path CPU it avoids on unread files outweighs the
//! conversion CPU on read files plus the size penalty of holding and moving the
//! larger IPC bytes.

/// Measured per-file cost components for one input shape and compressor.
///
/// Times are milliseconds of CPU for each pipeline step; sizes are the
/// serialized bytes of each at-rest / on-the-wire form. In the large-batch
/// regime this study assumes, [`Self::ipc_bytes`] exceeds [`Self::pq_bytes`].
#[derive(Clone, Copy, Debug)]
pub struct Components {
    /// Arrow IPC encode: transport-optimize + IPC-serialize(+compress). The
    /// lazy write-path cost.
    pub ipc_encode_ms: f64,
    /// Arrow IPC decode: IPC-deserialize + transport-decode. Paid once on a lazy
    /// conversion, to get back an OTAP batch to flatten.
    pub ipc_decode_ms: f64,
    /// Flatten the OTAP batch into the single Parquet-ready record batch.
    pub flatten_ms: f64,
    /// Arrow Parquet write.
    pub pq_write_ms: f64,
    /// Arrow Parquet read. Paid on every read by both strategies, so it cancels.
    pub pq_read_ms: f64,
    /// Serialized Arrow IPC size, steady-state (warm) bytes per batch.
    pub ipc_bytes: f64,
    /// Serialized Parquet file size, bytes.
    pub pq_bytes: f64,
}

impl Components {
    /// Eager write-path CPU: flatten then Parquet write.
    #[must_use]
    pub fn eager_write_ms(&self) -> f64 {
        self.flatten_ms + self.pq_write_ms
    }

    /// Lazy conversion CPU: decode the IPC back to OTAP, flatten, write Parquet.
    #[must_use]
    pub fn convert_ms(&self) -> f64 {
        self.ipc_decode_ms + self.flatten_ms + self.pq_write_ms
    }
}

/// Cost knobs: prices per unit of each resource. All are configurable; the
/// benchmark supplies illustrative cloud-like defaults via [`Self::default`].
///
/// Storage is priced with retention already folded in (see
/// [`Self::with_retention_months`]), so `store_per_gb` is the cost of holding one
/// GB for the whole retention window, not a per-month rate.
#[derive(Clone, Copy, Debug)]
pub struct CostKnobs {
    /// Dollars per CPU-millisecond.
    pub cpu_per_ms: f64,
    /// Dollars per GB transported over the network (gateway -> ingestion).
    pub net_per_gb: f64,
    /// Dollars per GB of disk/object-store I/O (bytes written or read).
    pub io_per_gb: f64,
    /// Dollars per GB held at rest for the whole retention window.
    pub store_per_gb: f64,
}

/// One byte expressed in GB, for converting sizes to the `*_per_gb` knobs.
const BYTES_PER_GB: f64 = 1_000_000_000.0;

impl Default for CostKnobs {
    /// Illustrative defaults, loosely modeled on commodity cloud pricing. They
    /// are deliberately round and are meant to be overridden; the study reports
    /// the break-even `R*`, which is what the knobs feed, not an absolute cost.
    ///
    /// - CPU: 1 vCPU-hour at about \$0.05, so \$0.05 / 3_600_000 per CPU-ms.
    /// - Network egress: \$0.08 / GB.
    /// - Object-store I/O: \$0.005 / GB moved to or from the store.
    /// - Storage: \$0.023 / GB-month at a 3-month retention = \$0.069 / GB.
    fn default() -> Self {
        Self {
            cpu_per_ms: 0.05 / 3_600_000.0,
            net_per_gb: 0.08,
            io_per_gb: 0.005,
            store_per_gb: 0.023 * 3.0,
        }
    }
}

impl CostKnobs {
    /// Return a copy with the storage price set from a per-GB-month rate and a
    /// retention window in months.
    #[must_use]
    pub fn with_retention_months(mut self, per_gb_month: f64, months: f64) -> Self {
        self.store_per_gb = per_gb_month * months;
        self
    }

    /// CPU-only knobs: network, I/O, and storage priced at zero. Isolates the
    /// write-path-versus-conversion CPU trade.
    #[must_use]
    pub fn cpu_only(self) -> Self {
        Self {
            cpu_per_ms: self.cpu_per_ms,
            net_per_gb: 0.0,
            io_per_gb: 0.0,
            store_per_gb: 0.0,
        }
    }

    /// Add storage to a set of knobs (leaving CPU, and whatever else is set).
    #[must_use]
    pub fn with_storage(mut self, store_per_gb: f64) -> Self {
        self.store_per_gb = store_per_gb;
        self
    }

    /// Add network transport to a set of knobs.
    #[must_use]
    pub fn with_network(mut self, net_per_gb: f64) -> Self {
        self.net_per_gb = net_per_gb;
        self
    }

    /// Add disk/object-store I/O to a set of knobs.
    #[must_use]
    pub fn with_io(mut self, io_per_gb: f64) -> Self {
        self.io_per_gb = io_per_gb;
        self
    }
}

/// Probability a file is read at least once, given `r` expected reads modeled as
/// a Poisson process: `p = 1 - e^{-r}`. This is what triggers a lazy conversion.
#[must_use]
pub fn read_probability(r: f64) -> f64 {
    1.0 - (-r).exp()
}

/// Expected total cost of one file under the **eager** strategy at read rate `r`.
#[must_use]
pub fn eager_cost(c: &Components, k: &CostKnobs, r: f64) -> f64 {
    let pq_gb = c.pq_bytes / BYTES_PER_GB;
    // Write path: flatten + Parquet write, always.
    let write = k.cpu_per_ms * c.eager_write_ms();
    // Transport the Parquet file once, write it to the store once.
    let transport = k.net_per_gb * pq_gb;
    let store_write = k.io_per_gb * pq_gb;
    // Hold the Parquet file at rest for the retention window.
    let at_rest = k.store_per_gb * pq_gb;
    // Each read: Parquet read CPU plus reading the file back from the store.
    let reads = r * (k.cpu_per_ms * c.pq_read_ms + k.io_per_gb * pq_gb);
    write + transport + store_write + at_rest + reads
}

/// Expected total cost of one file under the **lazy** strategy at read rate `r`.
#[must_use]
pub fn lazy_cost(c: &Components, k: &CostKnobs, r: f64) -> f64 {
    let ipc_gb = c.ipc_bytes / BYTES_PER_GB;
    let pq_gb = c.pq_bytes / BYTES_PER_GB;
    let p = read_probability(r);

    // Write path: Arrow IPC encode, always. Transport and store the IPC bytes.
    let write = k.cpu_per_ms * c.ipc_encode_ms;
    let transport = k.net_per_gb * ipc_gb;
    let store_write = k.io_per_gb * ipc_gb;

    // Conversion happens only on files that are read (probability p): decode the
    // IPC, flatten, write Parquet, and pay the I/O to read the IPC and write the
    // resulting Parquet file back to the store.
    let convert = p * (k.cpu_per_ms * c.convert_ms() + k.io_per_gb * (ipc_gb + pq_gb));

    // At rest: converted files hold Parquet, the rest hold the larger IPC.
    let at_rest = k.store_per_gb * (p * pq_gb + (1.0 - p) * ipc_gb);

    // Reads are served from Parquet after conversion, identical to eager.
    let reads = r * (k.cpu_per_ms * c.pq_read_ms + k.io_per_gb * pq_gb);

    write + transport + store_write + convert + at_rest + reads
}

/// The break-even read rate `R*`: the value of `r` at which lazy and eager cost
/// the same. Below `R*` lazy is cheaper; above it eager is cheaper.
///
/// Returns `None` when the sign of `lazy - eager` does not change over
/// `[0, max_r]`, i.e. one strategy dominates across the whole range. When lazy
/// is cheaper even at `max_r`, `R*` is beyond the searched range and the caller
/// should report "> max_r".
#[must_use]
pub fn breakeven_reads(c: &Components, k: &CostKnobs, max_r: f64) -> Option<f64> {
    let diff = |r: f64| lazy_cost(c, k, r) - eager_cost(c, k, r);
    let lo_sign = diff(0.0);
    let hi_sign = diff(max_r);
    // No crossing in range: same sign at both ends (treat exact-zero endpoints as
    // no interior crossing to find).
    if lo_sign == 0.0 {
        return Some(0.0);
    }
    if lo_sign.signum() == hi_sign.signum() {
        return None;
    }
    // Bisection: diff is continuous and monotonic enough over the range for the
    // sign change to be a single crossing.
    let (mut lo, mut hi) = (0.0f64, max_r);
    for _ in 0..100 {
        let mid = 0.5 * (lo + hi);
        if diff(mid).signum() == lo_sign.signum() {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Some(0.5 * (lo + hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Components roughly matching the 50k-record `zstd` headline from
    /// `ANALYSIS.md`: cheap IPC encode/decode, expensive flatten + Parquet write,
    /// and IPC larger than Parquet at rest (the large-batch regime).
    fn sample() -> Components {
        Components {
            ipc_encode_ms: 18.9,
            ipc_decode_ms: 4.2,
            flatten_ms: 44.0,
            pq_write_ms: 127.0,
            pq_read_ms: 39.0,
            ipc_bytes: 390_640.0,
            pq_bytes: 240_211.0,
        }
    }

    #[test]
    fn read_probability_is_poisson() {
        assert!((read_probability(0.0) - 0.0).abs() < 1e-12);
        assert!((read_probability(1.0) - (1.0 - std::f64::consts::E.recip())).abs() < 1e-9);
        // Monotonically increasing toward 1.
        assert!(read_probability(0.5) < read_probability(2.0));
        assert!(read_probability(10.0) > 0.999);
    }

    #[test]
    fn per_read_term_cancels_in_the_difference() {
        // Two knob sets that differ only in per-read-affecting prices should not
        // move the break-even, because reads are identical across strategies.
        let c = sample();
        let base = CostKnobs::default();
        let r_base = breakeven_reads(&c, &base, 100.0);
        // pq_read only appears in the (cancelling) read term; changing it must not
        // move R* by changing the read cost. Verify by constructing a Components
        // with a very different pq_read and identical everything else.
        let mut c2 = c;
        c2.pq_read_ms = c.pq_read_ms * 5.0;
        let r2 = breakeven_reads(&c2, &base, 100.0);
        match (r_base, r2) {
            (Some(a), Some(b)) => assert!((a - b).abs() < 1e-6, "R* moved: {a} vs {b}"),
            (None, None) => {}
            other => panic!("break-even presence changed: {other:?}"),
        }
    }

    #[test]
    fn cpu_only_lazy_wins_at_zero_reads_and_has_a_breakeven() {
        // With only CPU priced, a never-read file is cheaper lazy: it skips the
        // eager flatten + Parquet write. As reads rise, the per-file conversion
        // eats that saving, so a break-even R* exists.
        let c = sample();
        let k = CostKnobs::default().cpu_only();
        assert!(
            lazy_cost(&c, &k, 0.0) < eager_cost(&c, &k, 0.0),
            "CPU-only: lazy should win when the file is never read"
        );
        let r = breakeven_reads(&c, &k, 1000.0).expect("a CPU-only crossing exists");
        assert!(r > 0.0, "CPU-only R* should be positive: {r}");
        let d = (lazy_cost(&c, &k, r) - eager_cost(&c, &k, r)).abs();
        assert!(d < 1e-12, "costs not equal at R*: diff {d}");
    }

    #[test]
    fn full_tco_with_long_retention_lets_eager_dominate_everywhere() {
        // The key large-batch finding: at a 3-month retention, holding the bigger
        // IPC bytes at rest costs more than the one-time flatten + write CPU it
        // saves, so eager is cheaper at *every* read rate and no break-even in R
        // exists. Storage, not read count, decides it.
        let c = sample();
        let k = CostKnobs::default(); // 3-month retention, network, and I/O on.
        assert!(
            lazy_cost(&c, &k, 0.0) > eager_cost(&c, &k, 0.0),
            "full TCO: eager should win even at zero reads"
        );
        assert!(
            breakeven_reads(&c, &k, 1000.0).is_none(),
            "full TCO with long retention should have no break-even in R"
        );
    }

    #[test]
    fn expensive_cpu_restores_a_breakeven() {
        // Bytes (egress + storage) on the larger IPC dwarf the CPU saving at
        // commodity prices, so lazy needs CPU to be the dominant priced resource.
        // Crank the CPU price ~15x (premium managed compute) and a break-even in R
        // reappears even with network, I/O, and storage all priced.
        let mut k = CostKnobs::default();
        k.cpu_per_ms *= 15.0;
        let c = sample();
        assert!(
            lazy_cost(&c, &k, 0.0) < eager_cost(&c, &k, 0.0),
            "expensive CPU: lazy should win at zero reads"
        );
        let r = breakeven_reads(&c, &k, 1000.0).expect("crossing with expensive CPU");
        assert!(r > 0.0, "R* should be positive: {r}");
    }

    #[test]
    fn adding_the_storage_penalty_lowers_the_breakeven() {
        // In the large-batch regime IPC is bigger, so each priced dimension that
        // scales with size penalizes lazy and pushes R* down (or removes it).
        let c = sample();
        let cpu = CostKnobs::default().cpu_only();
        let plus_store = cpu.with_storage(0.023 * 0.25); // a short 1-week-ish window
        let r_cpu = breakeven_reads(&c, &cpu, 1000.0).expect("cpu crossing");
        match breakeven_reads(&c, &plus_store, 1000.0) {
            Some(r_store) => assert!(
                r_store <= r_cpu + 1e-9,
                "adding storage should not raise R*: {r_store} vs {r_cpu}"
            ),
            None => { /* storage penalty removed the crossing entirely: also valid */ }
        }
    }
}
