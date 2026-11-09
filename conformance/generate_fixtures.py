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

import confluent_kafka
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    header_schema_id_serializer,
    prefix_schema_id_serializer,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
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


def v1_prefix_schema_id_serializer(payload: bytes, ctx, schema_id) -> bytes:
    """The v1 (GUID) *prefix*, written by the reference's own ``SchemaId``
    encoder. No stock Confluent serializer emits this placement — the default
    writes the v0 prefix, the header serializer the v1 *header* — but
    ``SchemaId.from_bytes`` reads it, so these bytes are still the reference's
    and not schemreg's."""
    return schema_id.guid_to_bytes() + payload


PLACEMENTS = {
    "prefix-v0": prefix_schema_id_serializer,
    "header-v1": header_schema_id_serializer,
    "prefix-v1": v1_prefix_schema_id_serializer,
}


def main(out_path: str) -> int:
    url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    client = SchemaRegistryClient({"url": url})
    fixtures: list[dict[str, Any]] = []

    def serialize(make, placement: str, value, topic: str, field):
        """Run a reference serializer with the given schema-id placement and
        capture the identifier it framed with."""
        captured: dict[str, Any] = {}
        inner = PLACEMENTS[placement]

        def capturing(payload, ctx, schema_id):
            captured["id"] = schema_id.id
            captured["guid"] = str(schema_id.guid) if schema_id.guid else None
            return inner(payload, ctx, schema_id)

        ser = make({"schema.id.serializer": capturing})
        headers: list[tuple[str, bytes]] = []
        out = ser(value, SerializationContext(topic, field, headers))
        return out, headers, captured

    def check_with_reference(make_deser, out, headers, topic, field, expected, name):
        """The reference must read back what it wrote, or the fixture is void."""
        got = make_deser()(out, SerializationContext(topic, field, list(headers)))
        if got != expected:
            raise SystemExit(f"{name}: the reference deserializer read {got!r}, wrote {expected!r}")

    def record(name, note, schema_type, schema_text, placement, field, out, headers,
               captured, message_indexes):
        side = "key" if field == MessageField.KEY else "value"
        entry: dict[str, Any] = {
            "name": name,
            "note": note,
            "schema_type": schema_type,
            "schema": schema_text,
            "placement": placement,
            "side": side,
            "schema_id": captured["id"],
            "guid": captured["guid"],
            "message_indexes": message_indexes,
        }
        if placement.startswith("header"):
            [(header_name, header_value)] = headers
            entry["header_name"] = header_name
            entry["header_hex"] = hexdump(header_value)
            entry["payload_hex"] = hexdump(out)
        else:
            if headers:
                raise SystemExit(f"{name}: a prefix placement must not write a header")
            entry["framed_hex"] = hexdump(out)
        fixtures.append(entry)

    # ── Avro ──────────────────────────────────────────────────────────────
    # Each format gets its own topic: they share a schema *name* but not a
    # schema type, and one subject cannot hold both.
    avro_value = {"id": "order-1", "quantity": 3}
    for placement, field in [
        ("prefix-v0", MessageField.VALUE),
        ("header-v1", MessageField.VALUE),
        ("header-v1", MessageField.KEY),
        ("prefix-v1", MessageField.VALUE),
    ]:
        topic = f"{TOPIC}-avro"
        out, headers, captured = serialize(
            lambda conf: AvroSerializer(client, AVRO_SCHEMA, conf=conf),
            placement, avro_value, topic, field)
        check_with_reference(lambda: AvroDeserializer(client), out, headers, topic, field,
                             avro_value, placement)
        side = "key" if field == MessageField.KEY else "value"
        record(f"avro_order_{placement.replace('-', '_')}_{side}",
               f"Avro, {placement}, {side} side. No message-index array for Avro.",
               "AVRO", AVRO_SCHEMA.strip(), placement, field, out, headers, captured, None)

    # ── JSON Schema ───────────────────────────────────────────────────────
    json_value = {"id": "order-2", "quantity": 7}
    for placement in ["prefix-v0", "header-v1"]:
        topic = f"{TOPIC}-json"
        out, headers, captured = serialize(
            lambda conf: JSONSerializer(JSON_SCHEMA, client, conf=conf),
            placement, json_value, topic, MessageField.VALUE)
        check_with_reference(lambda: JSONDeserializer(None, schema_registry_client=client),
                             out, headers, topic, MessageField.VALUE, json_value, placement)
        record(f"json_order_{placement.replace('-', '_')}_value",
               f"JSON Schema, {placement}. JSON Schema uses no message-index either.",
               "JSON", JSON_SCHEMA.strip(), placement, MessageField.VALUE, out, headers,
               captured, None)

    # ── Protobuf: every message-index shape in the file ───────────────────
    #
    # `Order` exercises the mandated single-0x00 optimisation; the rest the
    # ZigZag-encoded count that v0.3.0 got wrong. In header mode the index
    # array travels in the header, after the GUID.
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
        runs = [("prefix-v0", MessageField.VALUE), ("header-v1", MessageField.VALUE)]
        if msg_name == "Invoice.Line":
            runs += [("header-v1", MessageField.KEY), ("prefix-v1", MessageField.VALUE)]
        for placement, field in runs:
            topic = f"{TOPIC}-{msg_name}"
            msg_type = type(message)
            out, headers, captured = serialize(
                lambda conf: ProtobufSerializer(
                    msg_type, client, {"use.deprecated.format": False, **conf}),
                placement, message, topic, field)
            check_with_reference(
                lambda: ProtobufDeserializer(msg_type, {"use.deprecated.format": False}),
                out, headers, topic, field, message, placement)
            side = "key" if field == MessageField.KEY else "value"
            record(
                f"protobuf_{msg_name.replace('.', '_').lower()}_{placement.replace('-', '_')}_{side}",
                f"{note} ({placement}, {side} side)",
                "PROTOBUF", PROTO_SOURCE.strip(), placement, field, out, headers,
                captured, expected_indexes)

    payload = {
        "_comment": (
            "GENERATED FILE — do not edit by hand. Produced by "
            "conformance/generate_fixtures.py using the official confluent-kafka "
            "Python serializers, each fixture read back by the reference "
            "deserializer. Regenerate with: just conformance"
        ),
        "generator": f"confluent-kafka-python {confluent_kafka.__version__}",
        "registry": os.environ.get("REGISTRY_IMAGE", "unknown"),
        "fixtures": fixtures,
    }

    with open(out_path, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")

    print(f"wrote {len(fixtures)} fixtures to {out_path}")
    for f in fixtures:
        print(f"  {f['name']:50s} {f.get('framed_hex', f.get('header_hex', ''))[:40]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "confluent_conformance.json"))
