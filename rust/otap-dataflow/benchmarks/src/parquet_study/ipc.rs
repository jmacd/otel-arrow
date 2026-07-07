// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! IPC baseline contender: the OTAP representation "as we have it today" -- the
//! interleaved Arrow IPC streams produced by [`Producer`] / consumed by
//! [`Consumer`], with the per-payload IPC streams optionally compressed.
//!
//! - write: [`Producer::produce_bar`] (applies transport-optimized encoding and
//!   serializes each payload's record batch to an Arrow IPC stream) then
//!   prost-encodes the resulting [`BatchArrowRecords`] to bytes.
//! - read: prost-decodes the [`BatchArrowRecords`], [`Consumer::consume_bar`]
//!   deserializes the IPC streams, [`from_record_messages`] reassembles the
//!   [`OtapArrowRecords`], and `decode_transport_optimized_ids` restores the
//!   logical (non-transport-optimized) batch so it matches the input exactly.

use otap_df_pdata::Consumer;
use otap_df_pdata::encode::producer::{Producer, ProducerOptions};
use otap_df_pdata::otap::{Logs, OtapArrowRecords, from_record_messages};
use otap_df_pdata::proto::opentelemetry::arrow::v1::BatchArrowRecords;
use prost::Message;

use super::{Codec, Compressor, StudyResult};

/// Contender that encodes OTAP logs as compressed interleaved Arrow IPC streams.
pub struct IpcCodec {
    /// Compression applied to each per-payload IPC stream.
    pub compressor: Compressor,
}

/// Pipeline sub-step: apply the OTAP transport-optimized encoding in place
/// (delta/dictionary encodings on id and value columns, parent-id remapping).
/// This is the first thing `Producer::produce_bar` does; measuring it alone lets
/// the benchmark separate it from the Arrow IPC serialization.
pub fn transport_encode(logs: &mut OtapArrowRecords) -> StudyResult<()> {
    logs.encode_transport_optimized()?;
    Ok(())
}

/// Pipeline sub-step: serialize a logs batch to wire bytes. This runs the whole
/// encode side (transport-optimized encoding, then Arrow IPC serialization with
/// compression, then prost-encoding the `BatchArrowRecords`). The IPC
/// serialization time alone is this minus [`transport_encode`].
pub fn encode_to_bytes(mut logs: OtapArrowRecords, compressor: Compressor) -> StudyResult<Vec<u8>> {
    let mut producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: compressor.ipc(),
    });
    let bar = producer.produce_bar(&mut logs)?;
    let mut buf = Vec::with_capacity(1024);
    bar.encode(&mut buf)?;
    Ok(buf)
}

/// Pipeline sub-step: deserialize wire bytes into a logs batch that is still in
/// the transport-optimized encoding (prost-decode, then `Consumer::consume_bar`,
/// then `from_record_messages`). Does not run the transport decode.
pub fn deserialize(bytes: &[u8]) -> StudyResult<OtapArrowRecords> {
    let mut bar = BatchArrowRecords::decode(bytes)?;
    let mut consumer = Consumer::default();
    let messages = consumer.consume_bar(&mut bar)?;
    Ok(OtapArrowRecords::Logs(from_record_messages::<Logs>(
        messages,
    )?))
}

/// Pipeline sub-step: reverse the transport-optimized encoding in place, leaving
/// the logical OTAP logs batch.
pub fn transport_decode(logs: &mut OtapArrowRecords) -> StudyResult<()> {
    logs.decode_transport_optimized_ids()?;
    Ok(())
}

/// Serialize the same logs batch `count` times through a single long-lived
/// [`Producer`], returning the wire size of each batch.
///
/// This models OTAP streaming: the Arrow schema is written once into the stream
/// and dictionaries are delta-encoded, so `sizes[0]` is the cold size (schema
/// plus full dictionaries plus data) while `sizes[1..]` are the steady-state
/// sizes (data plus only new dictionary entries). Sending the identical batch
/// repeatedly is a best case for dictionary amortization; real telemetry with
/// varying values falls between the cold and steady-state sizes.
pub fn stream_batch_sizes(
    logs: &OtapArrowRecords,
    compressor: Compressor,
    count: usize,
) -> StudyResult<Vec<usize>> {
    let mut producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: compressor.ipc(),
    });
    let mut sizes = Vec::with_capacity(count);
    for _ in 0..count {
        let mut batch = logs.clone();
        let bar = producer.produce_bar(&mut batch)?;
        let mut buf = Vec::with_capacity(1024);
        bar.encode(&mut buf)?;
        sizes.push(buf.len());
    }
    Ok(sizes)
}

impl Codec for IpcCodec {
    fn name(&self) -> &'static str {
        "ipc"
    }

    fn write(&self, logs: OtapArrowRecords) -> StudyResult<Vec<u8>> {
        encode_to_bytes(logs, self.compressor)
    }

    fn read(&self, bytes: &[u8]) -> StudyResult<OtapArrowRecords> {
        let mut logs = deserialize(bytes)?;
        transport_decode(&mut logs)?;
        Ok(logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_study::Compressor;
    use crate::parquet_study::datagen::{LogsGenParams, gen_logs_otap};

    #[test]
    fn ipc_round_trip_preserves_structure() {
        let params = LogsGenParams {
            num_resources: 2,
            num_scopes: 2,
            num_logs: 3,
        };
        let (otap, _) = gen_logs_otap(&params);

        for compressor in Compressor::IPC {
            let codec = IpcCodec { compressor };
            let bytes = codec.write(otap.clone()).expect("write");
            let decoded = codec.read(&bytes).expect("read");
            // The IPC round-trip is lossless, though the decoded batch may
            // dictionary-encode some value columns, so compare structurally.
            crate::parquet_study::attrs::assert_logs_equivalent(
                &otap,
                &decoded,
                codec.name(),
                compressor.label(),
            );
        }
    }

    #[test]
    fn streaming_amortizes_schema_and_dictionaries() {
        let params = LogsGenParams {
            num_resources: 1,
            num_scopes: 1,
            num_logs: 2000,
        };
        let (otap, _) = gen_logs_otap(&params);

        for compressor in Compressor::IPC {
            let sizes = stream_batch_sizes(&otap, compressor, 5).expect("stream sizes");
            // The steady-state batch (2nd onward) omits the schema header and
            // re-sends only new dictionary entries, so it is smaller than the
            // cold first batch.
            assert!(
                sizes[1] < sizes[0],
                "{compressor:?}: steady {} not smaller than cold {}",
                sizes[1],
                sizes[0]
            );
            // The drop is one-time: with identical batches, every steady-state
            // batch is the same size. Arrow IPC does not compress frames against
            // each other, so frame N is not smaller than frame 2 despite carrying
            // identical data.
            assert!(
                sizes[2..].iter().all(|&s| s == sizes[1]),
                "{compressor:?}: steady-state not flat: {:?}",
                sizes
            );
        }
    }

    /// Produce `count` batches through one long-lived producer with IPC
    /// compression disabled, concatenate the wire bytes, and compress the whole
    /// stream once with zstd at `level`. This models the size a stream-level
    /// (cross-batch) compressor could reach, whereas Arrow IPC compresses each
    /// batch independently and cannot exploit redundancy across batches.
    fn stream_whole_zstd(logs: &OtapArrowRecords, count: usize, level: i32) -> usize {
        let mut producer = Producer::new_with_options(ProducerOptions {
            ipc_compression: None,
        });
        let mut stream = Vec::new();
        for _ in 0..count {
            let mut batch = logs.clone();
            let bar = producer.produce_bar(&mut batch).expect("produce");
            bar.encode(&mut stream).expect("encode");
        }
        zstd::stream::encode_all(&stream[..], level)
            .expect("zstd")
            .len()
    }

    /// Arrow IPC compresses each batch independently, so it never exploits the
    /// large redundancy across the near-identical steady-state batches. This test
    /// shows two things about that unexploited redundancy:
    ///
    /// 1. A whole-stream compressor at default effort recovers almost none of it,
    ///    because each uncompressed batch (~2.4 MB at 10k logs) is larger than the
    ///    match window, so an extra near-duplicate batch still costs about as much
    ///    as one Arrow IPC batch.
    /// 2. A whole-stream compressor with a large window and long-distance matching
    ///    does find it, collapsing each extra near-duplicate batch to a tiny
    ///    fraction of an Arrow IPC batch.
    ///
    /// The identical-batch case here is a best case; real telemetry batches differ
    /// substantially, so the recoverable cross-batch redundancy is far smaller.
    #[test]
    fn cross_batch_redundancy_needs_large_window() {
        let params = LogsGenParams {
            num_resources: 1,
            num_scopes: 1,
            num_logs: 10_000,
        };
        let (otap, _) = gen_logs_otap(&params);

        let count = 8;
        // Arrow IPC steady-state batch size (per-batch independent compression).
        let warm = stream_batch_sizes(&otap, Compressor::Zstd, count).expect("sizes")[1];

        // Default effort: window smaller than one uncompressed batch, so an extra
        // near-duplicate batch costs about the same as an Arrow IPC batch.
        let low_1 = stream_whole_zstd(&otap, 1, 3);
        let low_n = stream_whole_zstd(&otap, count, 3);
        let low_marginal = (low_n - low_1) / (count - 1);
        assert!(
            low_marginal * 2 > warm,
            "default-effort cross-batch unexpectedly cheap: {low_marginal} vs warm {warm}"
        );

        // High effort: large window plus long-distance matching finds the
        // cross-batch redundancy and collapses each extra batch to near zero.
        let high_1 = stream_whole_zstd(&otap, 1, 19);
        let high_n = stream_whole_zstd(&otap, count, 19);
        let high_marginal = (high_n - high_1) / (count - 1);
        assert!(
            high_marginal * 10 < warm,
            "high-effort cross-batch did not collapse: {high_marginal} vs warm {warm}"
        );
    }

    /// Realistic-stream counterpart to `cross_batch_redundancy_needs_large_window`.
    ///
    /// That test uses byte-for-byte duplicate batches, the best case for
    /// cross-batch compression. This one uses `gen_logs_otap_rich_stream`, which
    /// produces distinct batches from the same services: shared resource/scope
    /// attributes stay stable, but every record has a fresh trace/span id,
    /// timestamp, body, and high-cardinality attributes. It measures whether
    /// sending *uncompressed* Arrow IPC through a large-window stream compressor
    /// beats today's per-batch arrow-ipc zstd, and at what CPU. Run with:
    ///   cargo test -p otap-df-benchmarks --lib \
    ///     parquet_study::ipc::tests::cross_batch_realistic_stream_report -- --nocapture
    #[test]
    fn cross_batch_realistic_stream_report() {
        use crate::parquet_study::datagen::{RichGenParams, gen_logs_otap_rich_stream};
        use std::time::Instant;
        use zstd::zstd_safe::CParameter;

        let params = RichGenParams {
            label: "medium",
            num_resources: 10,
            num_scopes: 2,
            num_logs: 500, // 10 * 2 * 500 = 10,000 logs per batch
            num_resource_attrs: 5,
            num_scope_attrs: 2,
            num_log_attrs: 9,
        };
        let count = 8usize;
        let batches = gen_logs_otap_rich_stream(&params, count);

        // (a) Today: arrow-ipc per-batch zstd through one long-lived producer.
        // The first batch is cold (schema + full dictionaries), the rest warm.
        let mut producer = Producer::new_with_options(ProducerOptions {
            ipc_compression: Some(arrow_ipc::CompressionType::ZSTD),
        });
        let mut ipc_total = 0usize;
        let t = Instant::now();
        for b in &batches {
            let mut batch = b.clone();
            let bar = producer.produce_bar(&mut batch).expect("produce");
            let mut buf = Vec::new();
            bar.encode(&mut buf).expect("encode");
            ipc_total += buf.len();
        }
        let ipc_ms = t.elapsed().as_secs_f64() * 1e3;

        // Uncompressed per-batch wire bytes (one long-lived producer). These are
        // the inputs both the whole-stream and dictionary approaches compress.
        let mut producer = Producer::new_with_options(ProducerOptions {
            ipc_compression: None,
        });
        let mut raw_batches: Vec<Vec<u8>> = Vec::with_capacity(count);
        for b in &batches {
            let mut batch = b.clone();
            let bar = producer.produce_bar(&mut batch).expect("produce");
            let mut buf = Vec::new();
            bar.encode(&mut buf).expect("encode");
            raw_batches.push(buf);
        }
        let raw_total: usize = raw_batches.iter().map(Vec::len).sum();
        let stream: Vec<u8> = raw_batches.concat();

        // (b) Whole-stream compression of the concatenated uncompressed IPC.
        // Gives up per-batch independent decode, but a large window + LDM can see
        // across batch boundaries. Returns (total_bytes, ms).
        let whole = |level: i32, ldm: bool, window: Option<u32>| -> (usize, f64) {
            let t = Instant::now();
            let mut c = zstd::bulk::Compressor::new(level).expect("compressor");
            if ldm {
                c.set_parameter(CParameter::EnableLongDistanceMatching(true))
                    .expect("ldm");
            }
            if let Some(w) = window {
                c.set_parameter(CParameter::WindowLog(w)).expect("window");
            }
            let out = c.compress(&stream).expect("compress");
            (out.len(), t.elapsed().as_secs_f64() * 1e3)
        };
        let l3 = whole(3, false, None);
        let l19 = whole(19, false, None);
        let l19w = whole(19, true, Some(27)); // 128 MiB window + long-distance matching

        // (c) Per-batch zstd primed with a dictionary trained on the batches.
        // This keeps per-batch independent decode (unlike whole-stream) while
        // giving each small batch the cross-batch context a dictionary carries.
        // The dictionary is shipped once, so it is not counted per batch.
        let dict = zstd::dict::from_samples(&raw_batches, 110 * 1024).expect("train dict");
        let t = Instant::now();
        let mut dict_total = 0usize;
        for raw in &raw_batches {
            let mut c = zstd::bulk::Compressor::with_dictionary(3, &dict).expect("dict compressor");
            dict_total += c.compress(raw).expect("compress").len();
        }
        let dict_ms = t.elapsed().as_secs_f64() * 1e3;

        let mib = |b: usize| b as f64 / (1024.0 * 1024.0);
        let row = |name: &str, total: usize, ms: f64| {
            println!(
                "| {name:<28} | {:>9.3} | {:>9.3} | {:>7.1} | {:>6.2}x |",
                mib(total),
                mib(total) / count as f64,
                ms,
                ipc_total as f64 / total as f64,
            );
        };
        println!(
            "\ncross-batch, realistic distinct batches: {count} x {} logs, dict {} B\n",
            params.total_logs(),
            dict.len()
        );
        println!("| approach                     | total MiB | /batch MiB | comp ms | vs ipc |");
        println!("|------------------------------|-----------|-----------|---------|--------|");
        row("arrow-ipc per-batch zstd", ipc_total, ipc_ms);
        println!("| (uncompressed IPC stream)    | {:>9.3} | {:>9.3} |       - |      - |", mib(raw_total), mib(raw_total)/count as f64);
        row("whole-stream zstd L3", l3.0, l3.1);
        row("whole-stream zstd L19", l19.0, l19.1);
        row("whole-stream zstd L19+win27", l19w.0, l19w.1);
        row("per-batch zstd+dict L3", dict_total, dict_ms);
        println!();

        // Cross-batch compression does help on realistic data, but only a little:
        // whole-stream L19 beats today's per-batch zstd yet stays well above half
        // its size, nowhere near the ~9x collapse the identical-batch best case
        // shows. Distinct trace ids and bodies leave almost no cross-batch
        // redundancy to exploit.
        assert!(
            l19.0 < ipc_total,
            "whole-stream L19 ({}) should beat per-batch ipc ({ipc_total})",
            l19.0
        );
        assert!(
            l19.0 * 2 > ipc_total,
            "realistic cross-batch gain unexpectedly large: L19 {} vs ipc {ipc_total}",
            l19.0
        );
        // The large window plus long-distance matching adds almost nothing once
        // the batches are genuinely distinct, so "a bigger zstd window for a
        // greater recall window" is not the lever on realistic telemetry.
        assert!(
            (l19w.0 as f64) > (l19.0 as f64) * 0.95,
            "large window recovered more than expected: L19+win {} vs L19 {}",
            l19w.0,
            l19.0
        );
    }

    /// Direct write-cost comparison over one realistic stream. Parquet is held at
    /// zstd level 3 (its `ZstdLevel` here is 3); the whole-stream pass is shown at
    /// levels 3, 9, and 19 so the size/CPU curve against Parquet is visible. Every
    /// row times the full write path:
    ///
    /// - arrow-ipc per-batch zstd: produce (transport-optimize + serialize) with
    ///   per-buffer zstd, summed over batches. Today's wire cost.
    /// - whole-stream zstd L{3,9,19}: produce all batches uncompressed, then one
    ///   zstd pass over the concatenation. Gives up per-batch independent decode.
    /// - parquet per-file: flatten + parquet-ready + write, one file per batch,
    ///   what the exporter emits.
    /// - parquet single-file: flatten + parquet-ready all batches into one file
    ///   with one row group each, the amortized analog to whole-stream.
    ///
    /// Run with:
    ///   cargo test -p benchmarks --lib --release \
    ///     parquet_study::ipc::tests::parquet_vs_whole_stream_write_report -- --nocapture
    #[test]
    fn parquet_vs_whole_stream_write_report() {
        use crate::parquet_study::datagen::{RichGenParams, gen_logs_otap_rich_stream};
        use crate::parquet_study::{nested, parquet_io};
        use parquet::arrow::ArrowWriter;
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::properties::WriterProperties;
        use std::time::Instant;

        let params = RichGenParams {
            label: "medium",
            num_resources: 10,
            num_scopes: 2,
            num_logs: 500, // 10,000 logs per batch
            num_resource_attrs: 5,
            num_scope_attrs: 2,
            num_log_attrs: 9,
        };
        let count = 8usize;
        let batches = gen_logs_otap_rich_stream(&params, count);
        let zstd3 = || Compression::ZSTD(ZstdLevel::try_new(3).expect("level"));

        // (1) arrow-ipc per-batch zstd, full produce path.
        let t = Instant::now();
        let mut ipc_total = 0usize;
        let mut producer = Producer::new_with_options(ProducerOptions {
            ipc_compression: Some(arrow_ipc::CompressionType::ZSTD),
        });
        for b in &batches {
            let mut batch = b.clone();
            let bar = producer.produce_bar(&mut batch).expect("produce");
            let mut buf = Vec::new();
            bar.encode(&mut buf).expect("encode");
            ipc_total += buf.len();
        }
        let ipc_ms = t.elapsed().as_secs_f64() * 1e3;

        // (2) whole-stream zstd: produce uncompressed once, then compress the
        // concatenation at several levels. The produce cost is shared, so each
        // level's write time is produce + that level's compression pass.
        let t = Instant::now();
        let mut producer = Producer::new_with_options(ProducerOptions {
            ipc_compression: None,
        });
        let mut stream = Vec::new();
        for b in &batches {
            let mut batch = b.clone();
            let bar = producer.produce_bar(&mut batch).expect("produce");
            bar.encode(&mut stream).expect("encode");
        }
        let ws_produce_ms = t.elapsed().as_secs_f64() * 1e3;
        let ws_at = |level: i32| -> (usize, f64) {
            let t = Instant::now();
            let out = zstd::stream::encode_all(&stream[..], level).expect("zstd").len();
            (out, ws_produce_ms + t.elapsed().as_secs_f64() * 1e3)
        };
        let ws3 = ws_at(3);
        let ws9 = ws_at(9);
        let ws19 = ws_at(19);
        let ws_ms = ws3.1;

        // (3) parquet, one file per batch.
        let t = Instant::now();
        let mut pq_files_total = 0usize;
        for b in &batches {
            let flat = nested::flatten(b).expect("flatten");
            let ready = parquet_io::to_parquet_ready(&flat).expect("ready");
            pq_files_total += parquet_io::write_parquet(&ready, zstd3()).expect("write").len();
        }
        let pq_files_ms = t.elapsed().as_secs_f64() * 1e3;

        // (4) parquet, all batches in one file (one row group per batch).
        let t = Instant::now();
        let ready: Vec<_> = batches
            .iter()
            .map(|b| {
                let flat = nested::flatten(b).expect("flatten");
                parquet_io::to_parquet_ready(&flat).expect("ready")
            })
            .collect();
        let props = WriterProperties::builder()
            .set_compression(zstd3())
            .build();
        let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
        let mut writer =
            ArrowWriter::try_new(&mut buf, ready[0].schema(), Some(props)).expect("writer");
        for r in &ready {
            writer.write(r).expect("write batch");
        }
        let _ = writer.close().expect("close");
        let pq_one_total = buf.len();
        let pq_one_ms = t.elapsed().as_secs_f64() * 1e3;

        let mib = |b: usize| b as f64 / (1024.0 * 1024.0);
        let row = |name: &str, total: usize, ms: f64| {
            println!(
                "| {name:<26} | {:>9.3} | {:>7.1} | {:>7.1} | {:>6.2}x | {:>6.2}x |",
                mib(total),
                ms,
                mib(total) / (ms / 1e3), // MiB/s throughput
                mib(ipc_total) / mib(total),
                ms / ipc_ms,
            );
        };
        println!(
            "\nwrite cost, realistic stream: {count} x {} logs (parquet zstd L3; whole-stream level as labeled)\n",
            params.total_logs()
        );
        println!(
            "| approach                   | total MiB | write ms |   MiB/s | vs ipc B | vs ipc ms |"
        );
        println!(
            "|----------------------------|-----------|----------|---------|----------|-----------|"
        );
        row("arrow-ipc per-batch zstd", ipc_total, ipc_ms);
        row("whole-stream zstd L3", ws3.0, ws3.1);
        row("whole-stream zstd L9", ws9.0, ws9.1);
        row("whole-stream zstd L19", ws19.0, ws19.1);
        row("parquet per-file", pq_files_total, pq_files_ms);
        row("parquet single-file", pq_one_total, pq_one_ms);
        println!();

        // The point of the comparison: Parquet is smaller but costs far more CPU
        // to write than either IPC path, holding the codec fixed.
        assert!(
            pq_files_total < ipc_total && pq_one_total < ipc_total,
            "parquet should be smaller: files {pq_files_total}, one {pq_one_total}, ipc {ipc_total}"
        );
        assert!(
            pq_files_ms > ws_ms && pq_one_ms > ws_ms,
            "parquet write should cost more CPU than whole-stream: files {pq_files_ms}, one {pq_one_ms}, ws {ws_ms}"
        );
    }
}
