#!/usr/bin/env python3
"""Generate wire-format conformance fixtures using the OFFICIAL Confluent serializers.

The point of this script is that **schemreg does not participate in producing
these bytes**. Every fixture is the output of `confluent_kafka.schema_registry`,
the reference implementation that Kafka consumers in every other language
interoperate with. `tests/conformance_fixtures.rs` then asserts that schemreg
decodes each fixture correctly *and* re-encodes to the identical byte sequence.

Without this, the crate's Protobuf golden vectors are only as good as somebody's
reading of the specification — which is precisely how the v0.3.0 message-index
bug survived a fully green test suite.

Usage (normally via docker compose, see conformance/README.md):

    SCHEMA_REGISTRY_URL=http://localhost:8081 python generate_fixtures.py out.json
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import shop_pb2

TOPIC = "conformance"

AVRO_SCHEMA = """
{
  "type": "record",
  "name": "Order",
  "namespace": "shop",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "quantity", "type": "int"}
  ]
}
"""

JSON_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "quantity": {"type": "integer"}
  },
  "required": ["id", "quantity"],
  "additionalProperties": false
}
"""

with open(os.path.join(os.path.dirname(__file__), "shop.proto")) as fh:
    PROTO_SOURCE = fh.read()


def hexdump(raw: bytes) -> str:
    return raw.hex()


def main(out_path: str) -> int:
    url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    client = SchemaRegistryClient({"url": url})

    fixtures: list[dict[str, Any]] = []

    def record(name: str, note: str, schema_type: str, schema_text: str,
               framed: bytes, message_indexes: list[int] | None) -> None:
        fixtures.append(
            {
                "name": name,
                "note": note,
                "schema_type": schema_type,
                "schema": schema_text,
                # The schema ID is assigned by the registry, so it varies per
                # run. The Rust side asserts on it only relative to the bytes.
                "framed_hex": hexdump(framed),
                "message_indexes": message_indexes,
            }
        )

    # ── Avro ──────────────────────────────────────────────────────────────
    # Each format gets its own topic: they share a schema *name* but not a
    # schema type, and one subject cannot hold both.
    avro_ser = AvroSerializer(client, AVRO_SCHEMA)
    framed = avro_ser(
        {"id": "order-1", "quantity": 3},
        SerializationContext(f"{TOPIC}-avro", MessageField.VALUE),
    )
    record(
        "avro_order",
        "Plain 5-byte Confluent header; no message-index array for Avro.",
        "AVRO",
        AVRO_SCHEMA.strip(),
        framed,
        None,
    )

    # ── JSON Schema ───────────────────────────────────────────────────────
    json_ser = JSONSerializer(JSON_SCHEMA, client)
    framed = json_ser(
        {"id": "order-2", "quantity": 7},
        SerializationContext(f"{TOPIC}-json", MessageField.VALUE),
    )
    record(
        "json_order",
        "Plain 5-byte Confluent header; JSON Schema uses no message-index either.",
        "JSON",
        JSON_SCHEMA.strip(),
        framed,
        None,
    )

    # ── Protobuf: every message-index shape in the file ───────────────────
    #
    # This is the set that matters. `Order` exercises the mandated single-0x00
    # optimisation; the rest exercise the ZigZag-encoded count that v0.3.0 got
    # wrong.
    protobuf_cases = [
        ("Order", shop_pb2.Order(id="order-3", quantity=1), [0],
         "First top-level message — the mandated single-0x00 optimisation."),
        ("Invoice", shop_pb2.Invoice(id="inv-1"), [1],
         "Second top-level message — ZigZag(count=1)=2, ZigZag(1)=2."),
        ("Refund", shop_pb2.Refund(id="ref-1"), [2],
         "Third top-level message — ZigZag(count=1)=2, ZigZag(2)=4."),
        ("Invoice.Line", shop_pb2.Invoice.Line(sku="SKU-1"), [1, 0],
         "Nested one level — ZigZag(count=2)=4, then ZigZag(1), ZigZag(0)."),
        ("Invoice.Tax", shop_pb2.Invoice.Tax(code="VAT"), [1, 1],
         "Nested one level, second nested type."),
        ("Invoice.Tax.Rate", shop_pb2.Invoice.Tax.Rate(basis_points=1950), [1, 1, 0],
         "Nested two levels — three-segment index path."),
    ]

    for msg_name, message, expected_indexes, note in protobuf_cases:
        # A fresh serializer per message type: the ProtobufSerializer derives
        # the message-index from the message class it is constructed with.
        ser = ProtobufSerializer(
            type(message),
            client,
            {"use.deprecated.format": False},
        )
        framed = ser(message, SerializationContext(f"{TOPIC}-{msg_name}", MessageField.VALUE))
        record(
            f"protobuf_{msg_name.replace('.', '_').lower()}",
            note,
            "PROTOBUF",
            PROTO_SOURCE.strip(),
            framed,
            expected_indexes,
        )

    payload = {
        "_comment": (
            "GENERATED FILE — do not edit by hand. Produced by "
            "conformance/generate_fixtures.py using the official confluent-kafka "
            "Python serializers. Regenerate with: "
            "docker compose -f conformance/docker-compose.yml up --build "
            "--abort-on-container-exit"
        ),
        "generator": "confluent-kafka-python",
        "fixtures": fixtures,
    }

    with open(out_path, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")

    print(f"wrote {len(fixtures)} fixtures to {out_path}")
    for f in fixtures:
        print(f"  {f['name']:28s} {f['framed_hex'][:32]}…")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "confluent_conformance.json"))
