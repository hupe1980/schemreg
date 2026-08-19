+++
title = "Using it with a Kafka client"
description = "Wire schemreg into the krafka Kafka client: a Serializer/Deserializer adapter with production-grade error mapping, typed Avro codecs, and schema IDs in Kafka record headers."
weight = 5
+++

`schemreg` produces and consumes [`bytes::Bytes`]. It is not a Kafka client, and
it never opens a connection to a broker — wiring the framed bytes to a topic is
the client's job.

This page shows that wiring for [krafka], a pure-Rust async Kafka client. The
same shape applies to any client that exposes a serializer hook; only the trait
names change.

[`bytes::Bytes`]: https://docs.rs/bytes
[krafka]: https://github.com/hupe1980/krafka

## Why they are separate crates

A schema registry is a different service from a Kafka broker: different
protocol, different auth model, different release cadence. Every mature client
draws the line in the same place — Java's `kafka-clients` ships no registry
support (`kafka-avro-serializer` is a separate artifact), librdkafka has none
(`libschemaregistry` is separate), and franz-go keeps `pkg/sr` out of `kgo`.

What a client owns is the *place* the transformation happens. krafka spells that
[`Serializer`] and [`Deserializer`], the equivalent of Java's `key.serializer` /
`value.serializer`. `schemreg` fills it.

[`Serializer`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Serializer.html
[`Deserializer`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Deserializer.html

```sh
cargo add krafka
cargo add schemreg --features confluent,avro
```

## The adapter

The two traits are the same shape — `Bytes` in, `Bytes` out, plus the topic and
a key/value flag — so the bridge is a newtype. The error mapping is the only
part that needs thought.

```rust,ignore
use std::future::Future;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
use krafka::serdes::{Deserializer, Serializer};
use krafka::KrafkaError;
use schemreg::{EncodeTarget, PayloadDecoder, PayloadEncoder, SchemaRegError};

/// Bridges a `schemreg` encoder into krafka's producer hook.
pub struct SchemaSerializer<T>(pub T);

/// Bridges a `schemreg` decoder into krafka's consumer hook.
pub struct SchemaDeserializer<T>(pub T);

/// Preserve the retry classification across the boundary.
///
/// `KrafkaError::is_retriable()` drives krafka's own retry logic, and it reads
/// `Network` and `Timeout` as transient and everything else as permanent. If
/// every registry failure collapsed into one variant, an unreachable registry
/// would be given up on and an incompatible schema would be retried forever —
/// so map the two classes apart. `SchemaRegError::is_retryable()` already
/// answers exactly that question.
fn to_krafka(err: SchemaRegError) -> KrafkaError {
    if err.is_retryable() {
        KrafkaError::network(io::Error::other(err.to_string()))
    } else {
        KrafkaError::serialization(err.to_string())
    }
}

fn target(is_key: bool) -> EncodeTarget {
    if is_key { EncodeTarget::Key } else { EncodeTarget::Value }
}

impl<T: PayloadEncoder> Serializer for SchemaSerializer<T> {
    fn serialize(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = krafka::Result<Bytes>> + Send + '_>> {
        let topic = topic.to_owned();
        let record_name = record_name.map(str::to_owned);
        Box::pin(async move {
            self.0
                .encode(payload, &topic, record_name.as_deref(), target(is_key))
                .await
                .map_err(to_krafka)
        })
    }
}

impl<T: PayloadDecoder> Deserializer for SchemaDeserializer<T> {
    fn deserialize(
        &self,
        payload: Bytes,
        topic: &str,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = krafka::Result<Bytes>> + Send + '_>> {
        let topic = topic.to_owned();
        Box::pin(async move {
            self.0
                .decode(payload, &topic, target(is_key))
                .await
                .map_err(to_krafka)
        })
    }
}
```

`Deserializer` takes no `record_name`: on the read path the framing itself names
the schema, which is why a registry decoder needs no hint.

## Wiring it in

`ConfluentSchemaEncoder` implements [`PayloadEncoder`] and `WireFormatDecoder`
implements [`PayloadDecoder`], so both drop straight into the hooks. Share one
`CachedSchemaRegistry` between them — the producer's subject lookups and the
consumer's ID lookups then warm the same cache.

[`PayloadEncoder`]: https://docs.rs/schemreg/latest/schemreg/traits/trait.PayloadEncoder.html
[`PayloadDecoder`]: https://docs.rs/schemreg/latest/schemreg/traits/trait.PayloadDecoder.html

```rust,ignore
use std::sync::Arc;
use std::time::Duration;

use krafka::consumer::Consumer;
use krafka::producer::{Producer, ProducerRecord};
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaEncoder, ConfluentSchemaRegistry,
    SchemaResolution, SchemaType, WireFormatDecoder,
};

let registry = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .basic_auth("user", "password")
    .build()?;
let cached = Arc::new(CachedSchemaRegistry::new(registry));

let encoder = ConfluentSchemaEncoder::builder()
    .registry(Arc::clone(&cached))
    .schema(ORDER_SCHEMA, SchemaType::Avro)
    .resolution(SchemaResolution::LookupOnly)   // never writes to the registry
    .build()?;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .value_serializer(Arc::new(SchemaSerializer(encoder)))
    .build()
    .await?;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("orders")
    .value_deserializer(Arc::new(SchemaDeserializer(WireFormatDecoder::confluent(cached))))
    .build()
    .await?;
```

From here `send` frames every value on the way out and `poll` unframes it on the
way in. The application only ever sees payload bytes:

```rust,ignore
producer.send("orders", Some(b"order-1"), &avro_bytes).await?;

consumer.subscribe(&["orders"]).await?;
for record in consumer.poll(Duration::from_secs(1)).await? {
    let Some(payload) = record.value else { continue };   // already unframed
    handle(&payload);
}
```

### Subject strategies that need a record name

`SubjectNameStrategy::RecordName` and `TopicRecordName` derive the subject from
the record's type, not its topic. krafka carries that through as
[`ProducerRecord::record_name`], which the adapter above forwards:

[`ProducerRecord::record_name`]: https://docs.rs/krafka/latest/krafka/producer/struct.ProducerRecord.html

```rust,ignore
let record = ProducerRecord::new("orders", avro_bytes)
    .with_key("order-1")
    .with_record_name("com.example.Order");

producer.send_record(record).await?;
```

Leave it unset for the default `TopicName` strategy, which ignores it.

## Typed codecs

The serializer hook is `Bytes -> Bytes`, so it fits the framing-only encoders.
The typed codecs — `AvroSchemaEncoder`, `JsonSchemaEncoder`,
`ProtobufSchemaEncoder` — take a *value*, not bytes, so they cannot implement
`PayloadEncoder`. Call them directly and send the result:

```rust,ignore
use apache_avro::types::Value;
use schemreg::{AvroSchemaDecoder, AvroSchemaEncoder, EncodeTarget};

let encoder = AvroSchemaEncoder::builder()
    .registry(Arc::clone(&cached))
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)
    .build()?;
let decoder = AvroSchemaDecoder::new(Arc::clone(&cached));

// Producer: serialise and frame in one step, then send the framed bytes.
let framed = encoder.encode(order, "orders", EncodeTarget::Value).await?;
producer.send("orders", Some(b"order-1"), &framed).await?;

// Consumer: no value_deserializer configured — decode the raw record instead.
for record in consumer.poll(Duration::from_secs(1)).await? {
    let Some(bytes) = record.value else { continue };
    let value: Value = decoder.decode(bytes).await?;
}
```

Pick one or the other per topic. Configuring a `value_serializer` *and* framing
by hand would frame the record twice.

## Schema IDs in Kafka headers

Confluent Platform 8 can carry the identifier in a record header instead of in
the payload prefix. That placement cannot go through the serializer hook — the
hook only rewrites the payload, and here the payload is the part that stays
untouched — so build the record explicitly.

`encode_with_header` returns all three pieces at once, which is what keeps the
header and the payload from drifting apart:

```rust,ignore
use schemreg::Framing;

let encoder = AvroSchemaEncoder::builder()
    .registry(Arc::clone(&cached))
    .schema(ORDER_SCHEMA)
    .framing(Framing::SchemaGuid)   // Confluent's header serializer emits GUIDs
    .build()?;

let framed = encoder
    .encode_with_header(order, "orders", EncodeTarget::Value)
    .await?;

let record = ProducerRecord::new("orders", framed.payload)   // no prefix
    .with_key("order-1")
    .with_header(framed.header_name, framed.header_value);

producer.send_record(record).await?;
```

On the consumer side, read the header and fall back to the payload prefix when
it is absent — the same header-first order Confluent's own deserializer uses, so
one consumer reads both old and new producers:

```rust,ignore
use schemreg::{VALUE_SCHEMA_ID_HEADER, decode_schema_id_header, decode_wire_format_bytes};

for record in consumer.poll(Duration::from_secs(1)).await? {
    let Some(value) = record.value.clone() else { continue };

    let (key, payload) = match record.header(VALUE_SCHEMA_ID_HEADER.as_bytes()).flatten() {
        // Header framing: the identifier is beside the payload.
        Some(header) => (decode_schema_id_header(header)?.0, value),
        // Prefix framing: the identifier is in front of it.
        None => decode_wire_format_bytes(&value)?,
    };

    let schema = cached.get_schema_by_key(key).await?;
    handle(&schema, &payload);
}
```

`decode_schema_id_header` also returns the Protobuf message-index path when one
is present; it is `None` for Avro and JSON Schema.

## Error handling at the boundary

krafka reports a value that could not be decoded as
`KrafkaError::RecordDeserialization`, carrying the topic, partition, and offset
— which is what lets a consumer seek past a permanently undecodable record
rather than stalling the partition on it.

The mapping in the adapter is what makes that classification meaningful:

| `schemreg` condition | `is_retryable()` | Mapped to | krafka's `is_retriable()` |
|---|---|---|---|
| Registry unreachable, HTTP 5xx, 429 | `true` | `KrafkaError::Network` | `true` |
| Subject or schema not found | `false` | `KrafkaError::Serialization` | `false` |
| Auth rejected | `false` | `KrafkaError::Serialization` | `false` |
| Incompatible or invalid schema | `false` | `KrafkaError::Serialization` | `false` |

`schemreg` already retries transport failures internally with jittered back-off
(see [Resilience](@/docs/resilience.md)), so an error that reaches this boundary
has usually exhausted that budget. Mapping it to a retriable krafka error gives
the send one more chance at a higher level; mapping a permanent failure to a
retriable one would spin forever.
