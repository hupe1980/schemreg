//! The two producer-side decisions every encoder makes, side by side.
//!
//! ```bash
//! cargo run --example schema_resolution --features confluent
//! ```
//!
//! Before it can write a byte, a producer has to answer:
//!
//! 1. **Which identifier does this subject resolve to?** — `SchemaResolution`
//!    - `AutoRegister` *(default)* — register the schema; needs `Subject:Write`
//!    - `LookupOnly` — find it or fail; needs only `Subject:Read`
//!    - `UseLatestVersion` — follow whatever the subject's head currently is
//! 2. **Which framing carries it?** — `Framing`
//!    - `SchemaId` *(default)* — `0x00` + 4 bytes, understood everywhere
//!    - `SchemaGuid` — `0x01` + 16 bytes, Confluent Platform 8 and newer
//!
//! …plus a placement choice made per call: `encode` puts the prefix in front of
//! the payload, `encode_with_header` puts it in a Kafka record header instead.
//!
//! The default is the one that writes to your registry. That is the same
//! default the Confluent Java serdes use, and the same one that quietly creates
//! a production schema version the first time a developer's local schema drifts
//! — which is why `LookupOnly` exists and why this example leads with it.

#[cfg(not(feature = "confluent"))]
fn main() {
    eprintln!(
        "This example requires the `confluent` Cargo feature.\n\
         Run with:  cargo run --example schema_resolution --features confluent"
    );
}

#[cfg(feature = "confluent")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use bytes::Bytes;
    use schemreg::{
        ConfluentSchemaEncoder, EncodeTarget, Framing, PayloadEncoder, Result, Schema, SchemaGuid,
        SchemaId, SchemaReference, SchemaRegistryClient, SchemaResolution, SchemaType,
        SchemaVersion, decode_schema_id_header, decode_wire_format,
    };

    const SCHEMA: &str =
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;
    const GUID_TEXT: &str = "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f";

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    // ── A stub registry that reports what it was asked ────────────────────
    //
    // `writes` counts registrations, which is the number this example is really
    // about. Replace with `ConfluentSchemaRegistry::builder()…build()?`.
    struct Stub {
        /// Whether `orders-value` already holds this exact schema.
        registered: bool,
        writes: AtomicU32,
        reads: AtomicU32,
    }

    impl Stub {
        fn new(registered: bool) -> Arc<Self> {
            Arc::new(Self {
                registered,
                writes: AtomicU32::new(0),
                reads: AtomicU32::new(0),
            })
        }
        fn schema(&self, id: u32) -> Arc<Schema> {
            let guid: SchemaGuid = GUID_TEXT.parse().unwrap_or(SchemaGuid::from_bytes([0; 16]));
            Arc::new(Schema::new(id, SchemaType::Avro, SCHEMA).with_guid(guid))
        }
    }

    impl SchemaRegistryClient for Stub {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.schema(id.as_u32()))
        }
        async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.schema(77))
        }
        async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
            Ok(self.schema(1))
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(SchemaId::new(11))
        }
        async fn lookup_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<Option<Arc<Schema>>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.registered.then(|| self.schema(22)))
        }
    }

    fn encoder(
        registry: Arc<Stub>,
        resolution: SchemaResolution,
        framing: Framing,
    ) -> Result<ConfluentSchemaEncoder<Arc<Stub>>> {
        ConfluentSchemaEncoder::builder()
            .registry(registry)
            .schema(SCHEMA, SchemaType::Avro)
            .resolution(resolution)
            .framing(framing)
            .build()
    }

    let body = Bytes::from_static(b"serialised-avro-bytes");

    // ── 1. AutoRegister (default) — writes to the registry ────────────────
    println!("=== SchemaResolution::AutoRegister (default) ===");
    let registry = Stub::new(false);
    let enc = encoder(
        Arc::clone(&registry),
        SchemaResolution::AutoRegister,
        Framing::SchemaId,
    )?;
    let framed = enc
        .encode(body.clone(), "orders", None, EncodeTarget::Value)
        .await?;
    let (key, _) = decode_wire_format(&framed)?;
    println!("  prefix:        {}", hex(&framed[..5]));
    println!("  identifier:    {key}");
    println!(
        "  registry writes: {}  ← a producer created a schema version",
        registry.writes.load(Ordering::SeqCst)
    );

    // ── 2. LookupOnly — read-only, and loud when it drifts ────────────────
    println!("\n=== SchemaResolution::LookupOnly (schema already registered) ===");
    let registry = Stub::new(true);
    let enc = encoder(
        Arc::clone(&registry),
        SchemaResolution::LookupOnly,
        Framing::SchemaId,
    )?;
    for _ in 0..3 {
        enc.encode(body.clone(), "orders", None, EncodeTarget::Value)
            .await?;
    }
    println!(
        "  registry writes: {}  ← never, whatever the traffic",
        registry.writes.load(Ordering::SeqCst)
    );
    println!(
        "  registry reads:  {}  ← the per-subject cache collapsed 3 encodes into 1",
        registry.reads.load(Ordering::SeqCst)
    );

    println!("\n=== SchemaResolution::LookupOnly (schema NOT registered) ===");
    let enc = encoder(
        Stub::new(false),
        SchemaResolution::LookupOnly,
        Framing::SchemaId,
    )?;
    match enc
        .encode(body.clone(), "orders", None, EncodeTarget::Value)
        .await
    {
        Ok(_) => println!("  unexpectedly succeeded"),
        Err(e) => {
            println!("  error:       {e}");
            println!("  is_not_found: {}", e.is_not_found());
            println!(
                "  is_retryable: {}  ← a retry loop stops instead of spinning",
                e.is_retryable()
            );
        }
    }

    // ── 3. UseLatestVersion — follow the subject's head ───────────────────
    println!("\n=== SchemaResolution::UseLatestVersion ===");
    let registry = Stub::new(true);
    let enc = encoder(
        Arc::clone(&registry),
        SchemaResolution::UseLatestVersion,
        Framing::SchemaId,
    )?;
    let framed = enc
        .encode(body.clone(), "orders", None, EncodeTarget::Value)
        .await?;
    println!(
        "  identifier:  {}  ← the subject head, not this encoder's own schema",
        decode_wire_format(&framed)?.0
    );
    match enc.cached_schema_key("orders-value") {
        Some(k) => println!("  cached:      {k}"),
        None => println!("  cached:      <nothing>"),
    }
    enc.invalidate_subject("orders-value");
    println!("  after invalidate_subject(): the next encode re-reads the head");

    // ── 4. Framing::SchemaGuid — wire format v1 ───────────────────────────
    println!("\n=== Framing::SchemaGuid (Confluent Platform 8+) ===");
    let enc = encoder(
        Stub::new(true),
        SchemaResolution::LookupOnly,
        Framing::SchemaGuid,
    )?;
    let framed = enc
        .encode(body.clone(), "orders", None, EncodeTarget::Value)
        .await?;
    let (key, payload) = decode_wire_format(&framed)?;
    println!("  prefix:      {}", hex(&framed[..17]));
    println!("  identifier:  {key}");
    println!(
        "  payload:     {} bytes, byte-identical to the v0 case",
        payload.len()
    );

    // ── 5. Header placement — the payload carries no prefix ───────────────
    println!("\n=== encode_with_header — identifier in a Kafka record header ===");
    let enc = encoder(
        Stub::new(true),
        SchemaResolution::LookupOnly,
        Framing::SchemaGuid,
    )?;
    let record = enc
        .encode_with_header(body.clone(), "orders", None, EncodeTarget::Value)
        .await?;
    println!("  header name:  {}", record.header_name);
    println!("  header value: {}", hex(&record.header_value));
    println!(
        "  payload:      {} bytes, unframed — write both, or the schema is lost",
        record.payload.len()
    );
    let (key, indexes) = decode_schema_id_header(&record.header_value)?;
    println!("  decoded:      {key}, message-index {indexes:?}");
    assert_eq!(&record.payload[..], &body[..]);

    println!(
        "\nPick LookupOnly wherever schemas are owned by CI, and SchemaGuid \
         wherever records outlive the registry that assigned their IDs."
    );
    Ok(())
}
