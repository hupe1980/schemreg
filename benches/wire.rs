//! Benchmarks for the hot paths: wire framing and cache lookup.
//!
//! Run with `cargo bench --features glue`.
//!
//! These exist to turn the performance guide's claims into measurements. The
//! claims under test:
//!
//! - Framing is allocation-light and scales linearly with payload size only.
//! - A cache hit is `O(1)` in schema size — `Schema` holds `Arc<str>`, so
//!   serving a 64 KiB schema must cost the same as a 64-byte one.
//! - Coalescing turns N concurrent cold misses into one backend call, and the
//!   bookkeeping to do so is cheap relative to the network call it saves.

use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use schemreg::{
    CachedSchemaRegistry, Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient,
    SchemaType, SchemaVersion, decode_protobuf_message_indexes, decode_wire_format,
    decode_wire_format_bytes, detect_wire_format, encode_protobuf_wire_format, encode_wire_format,
};

const PAYLOAD_SIZES: &[usize] = &[64, 1024, 64 * 1024];

// ── Framing ───────────────────────────────────────────────────────────────

fn bench_confluent_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("confluent_framing");
    for &size in PAYLOAD_SIZES {
        let payload = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("encode", size), &payload, |b, payload| {
            b.iter(|| encode_wire_format(black_box(42u32), black_box(payload)));
        });

        let framed = encode_wire_format(42u32, &payload);
        group.bench_with_input(BenchmarkId::new("decode", size), &framed, |b, framed| {
            b.iter(|| decode_wire_format(black_box(framed)).expect("valid frame"));
        });

        // The zero-copy variant should be indistinguishable from the slice one:
        // `Bytes::slice` is a refcount bump, not a copy. If this diverges with
        // payload size, something started copying.
        group.bench_with_input(
            BenchmarkId::new("decode_bytes_zero_copy", size),
            &framed,
            |b, framed| {
                b.iter(|| decode_wire_format_bytes(black_box(framed)).expect("valid frame"));
            },
        );
    }
    group.finish();
}

fn bench_protobuf_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("protobuf_framing");
    let payload = vec![0xABu8; 1024];

    // The optimised single-byte path against the general multi-segment one.
    for (label, indexes) in [
        ("default_[0]", vec![0]),
        ("top_level_[2]", vec![2]),
        ("nested_[1,0]", vec![1, 0]),
        ("deep_[2,1,4,0]", vec![2, 1, 4, 0]),
    ] {
        group.bench_function(BenchmarkId::new("encode", label), |b| {
            b.iter(|| {
                encode_protobuf_wire_format(
                    black_box(42u32),
                    black_box(&indexes),
                    black_box(&payload),
                )
            });
        });

        let framed = encode_protobuf_wire_format(42u32, &indexes, &payload);
        let after_header = &framed[5..];
        group.bench_function(BenchmarkId::new("decode_index", label), |b| {
            b.iter(|| decode_protobuf_message_indexes(black_box(after_header)).expect("valid"));
        });
    }
    group.finish();
}

fn bench_detection(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_wire_format");
    let confluent = encode_wire_format(1u32, &[0u8; 256]);
    let unknown = Bytes::from(vec![0x42u8; 256]);
    let truncated = Bytes::from_static(&[0x00, 0x01]);

    group.bench_function("confluent", |b| {
        b.iter(|| detect_wire_format(black_box(&confluent)));
    });
    group.bench_function("unknown", |b| {
        b.iter(|| detect_wire_format(black_box(&unknown)));
    });
    group.bench_function("truncated", |b| {
        b.iter(|| detect_wire_format(black_box(&truncated)));
    });
    group.finish();
}

// ── Cache ─────────────────────────────────────────────────────────────────

struct StubRegistry {
    schema_text: Arc<str>,
}

impl SchemaRegistryClient for StubRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        Ok(Arc::new(Schema::new(
            id,
            SchemaType::Avro,
            Arc::clone(&self.schema_text),
        )))
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        unreachable!("not exercised by these benchmarks")
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        unreachable!("not exercised by these benchmarks")
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        Ok(SchemaId::from(1u32))
    }
}

/// A cache hit must cost the same regardless of schema size. If this benchmark
/// scales with the schema text, `Schema` has stopped being `Arc`-shaped and
/// every hit is copying the schema again.
fn bench_cache_hit_is_size_independent(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");

    let mut group = c.benchmark_group("cache_hit");
    for &size in &[64usize, 4096, 64 * 1024] {
        let schema_text: Arc<str> = Arc::from("x".repeat(size).as_str());
        let cached = CachedSchemaRegistry::new(StubRegistry { schema_text });

        // Warm it so every measured iteration is a hit.
        runtime.block_on(async {
            cached
                .get_schema_by_id(SchemaId::from(1u32))
                .await
                .expect("warm");
        });

        group.bench_with_input(
            BenchmarkId::new("schema_bytes", size),
            &cached,
            |b, cached| {
                b.to_async(&runtime).iter(|| async {
                    cached
                        .get_schema_by_id(black_box(SchemaId::from(1u32)))
                        .await
                        .expect("hit")
                });
            },
        );
    }
    group.finish();
}

/// Cold-miss coalescing overhead: N tasks racing for one uncached ID.
///
/// The absolute number matters less than the shape — it should stay roughly
/// flat per task as N grows, because the contended section is a `HashMap`
/// lookup plus a channel push.
fn bench_coalescing(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("runtime");

    let mut group = c.benchmark_group("coalesced_cold_miss");
    for &tasks in &[1usize, 8, 64, 256] {
        group.throughput(Throughput::Elements(tasks as u64));
        group.bench_with_input(BenchmarkId::from_parameter(tasks), &tasks, |b, &tasks| {
            b.to_async(&runtime).iter_batched(
                || {
                    Arc::new(CachedSchemaRegistry::new(StubRegistry {
                        schema_text: Arc::from(r#"{"type":"string"}"#),
                    }))
                },
                |cached| async move {
                    let mut handles = Vec::with_capacity(tasks);
                    for _ in 0..tasks {
                        let cached = Arc::clone(&cached);
                        handles.push(tokio::spawn(async move {
                            cached
                                .get_schema_by_id(SchemaId::from(1u32))
                                .await
                                .expect("fetch")
                        }));
                    }
                    for h in handles {
                        let _ = h.await;
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_confluent_framing,
    bench_protobuf_framing,
    bench_detection,
    bench_cache_hit_is_size_independent,
    bench_coalescing,
);
criterion_main!(benches);
