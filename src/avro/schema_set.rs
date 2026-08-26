//! Parsing an Avro schema together with the schemas it references.
//!
//! A schema naming an externally defined type — `"type": "com.example.Address"`
//! rather than the record spelled out inline — parses into a tree containing
//! [`Schema::Ref`] nodes, which the codec cannot follow on its own. Its
//! `*_schemata` entry points take the definition set alongside the root;
//! [`ResolvedAvroSchema`] is that pair, held together so the two cannot drift
//! apart.
//!
//! # Ordering
//!
//! `apache-avro` builds its name table front to back and errors on a `Ref` it
//! has not seen defined yet, so the list it is given has to be topologically
//! ordered. The parser has no such requirement, so dependencies are accepted in
//! any order and sorted before the codec sees them.
//!
//! [`Schema::Ref`]: apache_avro::Schema::Ref

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{Name, Namespace};
use apache_avro::types::Value;
use serde_json::Value as JsonValue;

use crate::error::{Result, SchemaRegError};

/// Maximum number of referenced schemas resolved for a single root schema.
///
/// Bounds a locally supplied dependency list and a registry closure alike.
pub(crate) const MAX_REFERENCES: usize = 256;

/// Which schema a set belongs to. Used only to phrase errors: a missing
/// definition is one failure with three different fixes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchemaRole {
    /// The schema an [`AvroSchemaEncoder`](crate::AvroSchemaEncoder) writes with.
    WriterLocal,
    /// A writer schema fetched from the registry by a decoder.
    WriterRegistry,
    /// A reader schema configured on a decoder.
    Reader,
}

impl SchemaRole {
    /// How the schema is named in an error message.
    fn subject(self) -> &'static str {
        match self {
            Self::WriterLocal | Self::WriterRegistry => "Avro schema",
            Self::Reader => "Avro reader schema",
        }
    }

    /// What the caller has to do about a type nothing defines.
    fn remedy(self) -> &'static str {
        match self {
            Self::WriterLocal => "pass its schema JSON to `AvroSchemaEncoderBuilder::dependencies`",
            Self::WriterRegistry => {
                "register it under a subject and name it in this schema's `references` \
                 list, so the decoder can fetch it"
            }
            Self::Reader => {
                "pass its schema JSON to `AvroSchemaDecoderBuilder::reader_dependencies`"
            }
        }
    }
}

/// A parsed Avro schema plus the referenced schemas it needs to be usable.
///
/// `schemata` is the name table the codec resolves `Schema::Ref` against: the
/// whole set including `root`, definitions before users. Empty when `root` is
/// self-contained, which is the common case.
#[derive(Debug)]
pub(crate) struct ResolvedAvroSchema {
    root: AvroSchema,
    schemata: Vec<AvroSchema>,
}

impl ResolvedAvroSchema {
    /// Parse `schema_str` together with the schemas it depends on.
    ///
    /// `deps` may be given in any order. A definition supplied twice — one type
    /// reached twice through a diamond — is dropped as long as the copies
    /// agree; two different definitions of one type are an error.
    pub(crate) fn parse(schema_str: &str, deps: &[String], role: SchemaRole) -> Result<Self> {
        if deps.len() > MAX_REFERENCES {
            return Err(SchemaRegError::config(format!(
                "{} depends on {} schemas, more than the {MAX_REFERENCES} supported",
                role.subject(),
                deps.len(),
            )));
        }

        let mut inputs = if deps.is_empty() {
            Vec::new()
        } else {
            dedupe_dependencies(schema_str, deps, role)?
        };

        if inputs.is_empty() {
            // Nothing to resolve against: the schema has to stand on its own.
            let root = AvroSchema::parse_str(schema_str)
                .map_err(|e| parse_failed(&e.to_string(), role, &[]))?;
            return Ok(Self {
                root,
                schemata: Vec::new(),
            });
        }

        // The root goes last only so it can be picked back out; `parse_list`
        // preserves input order and resolves cross-references regardless of it.
        inputs.push(schema_str);
        let parsed = AvroSchema::parse_list(&inputs).map_err(|e| {
            // `inputs` ends with the root; only the dependencies can hide a
            // definition from the parser.
            parse_failed(&e.to_string(), role, &inputs[..inputs.len() - 1])
        })?;
        let root = parsed
            .last()
            .cloned()
            .ok_or_else(|| SchemaRegError::config("Avro schema list resolved to nothing"))?;

        Ok(Self {
            root,
            schemata: order_schemata(parsed, role)?,
        })
    }

    /// Build from schemas that are already parsed.
    ///
    /// Same guarantees as [`parse`](Self::parse), with canonical-form equality
    /// deciding what counts as a duplicate.
    pub(crate) fn from_parsed(
        root: AvroSchema,
        deps: Vec<AvroSchema>,
        role: SchemaRole,
    ) -> Result<Self> {
        if deps.len() > MAX_REFERENCES {
            return Err(SchemaRegError::config(format!(
                "{} depends on {} schemas, more than the {MAX_REFERENCES} supported",
                role.subject(),
                deps.len(),
            )));
        }

        let mut set: Vec<AvroSchema> = Vec::with_capacity(deps.len() + 1);
        for dep in deps {
            if dep != root && !set.contains(&dep) {
                set.push(dep);
            }
        }

        if set.is_empty() {
            // Self-contained, or self-referential — either way no name table is
            // needed, but a dangling `Ref` must not survive to decode time.
            let (defines, refs) = names_of(&root);
            if let Some(missing) = refs.difference(&defines).next() {
                return Err(missing_definition(missing, name_of(&root).as_deref(), role));
            }
            return Ok(Self {
                root,
                schemata: Vec::new(),
            });
        }

        set.push(root.clone());
        Ok(Self {
            root,
            schemata: order_schemata(set, role)?,
        })
    }

    /// The fully-qualified name of the root type, if it is a named type.
    pub(crate) fn fullname(&self) -> Option<String> {
        name_of(&self.root)
    }

    /// The name table handed to the codec.
    ///
    /// Always non-empty: a self-contained root can still reference itself — a
    /// linked list — and an empty table would strand those `Ref`s.
    fn codec_schemata(&self) -> Vec<&AvroSchema> {
        if self.schemata.is_empty() {
            vec![&self.root]
        } else {
            self.schemata.iter().collect()
        }
    }

    pub(crate) fn serialize(&self, value: Value) -> Result<Vec<u8>> {
        let result = if self.schemata.is_empty() {
            // Skips building a name table per encode; byte-identical output.
            apache_avro::to_avro_datum(&self.root, value)
        } else {
            apache_avro::to_avro_datum_schemata(&self.root, self.codec_schemata(), value)
        };
        result.map_err(|e| SchemaRegError::wire_format(format!("Avro serialization failed: {e}")))
    }

    /// Decode `bytes` written with this schema, optionally resolving to
    /// `reader`.
    ///
    /// Each side resolves against its own dependencies, so `reader` is a whole
    /// [`ResolvedAvroSchema`] rather than a bare [`AvroSchema`].
    pub(crate) fn deserialize(
        &self,
        mut bytes: &[u8],
        reader: Option<&ResolvedAvroSchema>,
    ) -> Result<Value> {
        let result = match reader {
            None if self.schemata.is_empty() => {
                apache_avro::from_avro_datum(&self.root, &mut bytes, None)
            }
            None => apache_avro::from_avro_datum_schemata(
                &self.root,
                self.codec_schemata(),
                &mut bytes,
                None,
            ),
            Some(reader) => apache_avro::from_avro_datum_reader_schemata(
                &self.root,
                self.codec_schemata(),
                &mut bytes,
                Some(&reader.root),
                reader.codec_schemata(),
            ),
        };
        result.map_err(|e| SchemaRegError::wire_format(format!("Avro deserialization failed: {e}")))
    }
}

// ── Ordering ──────────────────────────────────────────────────────────────

/// Order `parsed` so every definition precedes the schemas referencing it.
///
/// Errors when a `Ref` has no definition in the set, or when two schemas
/// reference each other: the name table is resolved in one pass, so a
/// cross-schema cycle has no valid order at all. A schema referencing *itself*
/// is not a cycle here.
fn order_schemata(parsed: Vec<AvroSchema>, role: SchemaRole) -> Result<Vec<AvroSchema>> {
    let infos: Vec<(BTreeSet<String>, BTreeSet<String>)> = parsed.iter().map(names_of).collect();

    // name → the schema that defines it.
    let mut definer: HashMap<&str, usize> = HashMap::new();
    for (index, (defines, _)) in infos.iter().enumerate() {
        for name in defines {
            if definer.insert(name.as_str(), index).is_some() {
                return Err(SchemaRegError::config(format!(
                    "`{name}` is defined by more than one schema in the {} set; \
                     Avro resolves a type name to exactly one definition, so drop the \
                     duplicate or rename one of them",
                    role.subject(),
                )));
            }
        }
    }

    let mut prerequisites: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); parsed.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); parsed.len()];
    for (index, (defines, refs)) in infos.iter().enumerate() {
        for name in refs {
            if defines.contains(name) {
                continue; // resolved within this schema — including self-reference
            }
            let Some(&definer_index) = definer.get(name.as_str()) else {
                return Err(missing_definition(
                    name,
                    name_of(&parsed[index]).as_deref(),
                    role,
                ));
            };
            // A cross-schema reference only ever resolves to another schema's
            // *top-level* type. For one nested inside another schema,
            // `apache-avro` resolves it or not depending on the iteration order
            // of an internal HashMap — so refuse it consistently rather than
            // parsing on one process start and failing on the next.
            if name_of(&parsed[definer_index]).as_deref() != Some(name.as_str()) {
                return Err(nested_definition(
                    name,
                    name_of(&parsed[definer_index]).as_deref(),
                    role,
                ));
            }
            if prerequisites[index].insert(definer_index) {
                dependents[definer_index].push(index);
            }
        }
    }

    // Kahn's algorithm, lowest index first so the output is deterministic and
    // an already-ordered input comes back unchanged.
    let mut ready: BinaryHeap<Reverse<usize>> = prerequisites
        .iter()
        .enumerate()
        .filter(|(_, prereqs)| prereqs.is_empty())
        .map(|(index, _)| Reverse(index))
        .collect();
    let mut remaining: Vec<usize> = prerequisites.iter().map(BTreeSet::len).collect();
    let mut order: Vec<usize> = Vec::with_capacity(parsed.len());

    while let Some(Reverse(index)) = ready.pop() {
        order.push(index);
        for &dependent in &dependents[index] {
            remaining[dependent] -= 1;
            if remaining[dependent] == 0 {
                ready.push(Reverse(dependent));
            }
        }
    }

    if order.len() != parsed.len() {
        let cycle: Vec<String> = (0..parsed.len())
            .filter(|index| remaining[*index] > 0)
            .filter_map(|index| name_of(&parsed[index]))
            .collect();
        return Err(SchemaRegError::config(format!(
            "circular reference between Avro schemas ({}); Avro can encode a schema that \
             refers to itself, but two schemas defined in separate subjects cannot refer \
             to each other — inline one of the definitions in the other",
            cycle.join(", "),
        )));
    }

    // `order` is a permutation of the indices, so index-and-take is safe.
    let mut slots: Vec<Option<AvroSchema>> = parsed.into_iter().map(Some).collect();
    Ok(order
        .into_iter()
        .filter_map(|index| slots[index].take())
        .collect())
}

// ── Name extraction ───────────────────────────────────────────────────────

/// The names a schema defines and the names it references, fully qualified.
///
/// Mirrors `apache_avro::schema::ResolvedSchema::resolve`, which builds the
/// codec's name table — same traversal, same namespace inheritance, same
/// silence about types nested inside logical types. Diverging would bless an
/// order the codec then rejects.
fn names_of(schema: &AvroSchema) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut defines = BTreeSet::new();
    let mut refs = BTreeSet::new();
    walk_names(schema, &None, &mut defines, &mut refs);
    (defines, refs)
}

fn walk_names(
    schema: &AvroSchema,
    enclosing: &Namespace,
    defines: &mut BTreeSet<String>,
    refs: &mut BTreeSet<String>,
) {
    match schema {
        AvroSchema::Array(inner) => walk_names(&inner.items, enclosing, defines, refs),
        AvroSchema::Map(inner) => walk_names(&inner.types, enclosing, defines, refs),
        AvroSchema::Union(union) => {
            for variant in union.variants() {
                walk_names(variant, enclosing, defines, refs);
            }
        }
        AvroSchema::Enum(inner) => {
            defines.insert(fullname(&inner.name, enclosing));
        }
        AvroSchema::Fixed(inner) => {
            defines.insert(fullname(&inner.name, enclosing));
        }
        AvroSchema::Record(inner) => {
            let name = inner.name.fully_qualified_name(enclosing);
            defines.insert(name.fullname(None));
            // A record's namespace becomes the default for its fields.
            let inner_namespace = name.namespace;
            for field in &inner.fields {
                walk_names(&field.schema, &inner_namespace, defines, refs);
            }
        }
        AvroSchema::Ref { name } => {
            refs.insert(fullname(name, enclosing));
        }
        _ => {}
    }
}

fn fullname(name: &Name, enclosing: &Namespace) -> String {
    name.fully_qualified_name(enclosing).fullname(None)
}

/// The fully-qualified name of a named Avro type (record, enum, or fixed).
/// `None` for primitives, unions, arrays, and maps.
pub(crate) fn name_of(schema: &AvroSchema) -> Option<String> {
    match schema {
        AvroSchema::Record(inner) => Some(inner.name.fullname(inner.name.namespace.clone())),
        AvroSchema::Enum(inner) => Some(inner.name.fullname(inner.name.namespace.clone())),
        AvroSchema::Fixed(inner) => Some(inner.name.fullname(inner.name.namespace.clone())),
        _ => None,
    }
}

// ── Deduplication ─────────────────────────────────────────────────────────

/// Drop dependencies that repeat a definition already in the set.
///
/// `parse_list` rejects two schemas sharing a fullname, including identical
/// ones — which a diamond produces routinely. Identical repeats are dropped;
/// differing definitions of one name are reported, since picking either would
/// decode somebody's data wrongly.
fn dedupe_dependencies<'a>(
    root: &str,
    deps: &'a [String],
    role: SchemaRole,
) -> Result<Vec<&'a str>> {
    let root_json: JsonValue = serde_json::from_str(root)
        .map_err(|e| SchemaRegError::config(format!("invalid {}: {e}", role.subject())))?;

    let mut seen: HashMap<String, JsonValue> = HashMap::new();
    if let Some(name) = top_level_fullname(&root_json) {
        seen.insert(name, root_json);
    }

    let mut kept = Vec::with_capacity(deps.len());
    for dep in deps {
        let json: JsonValue = serde_json::from_str(dep).map_err(|e| {
            SchemaRegError::config(format!(
                "invalid schema among the {} dependencies: {e}",
                role.subject()
            ))
        })?;
        let Some(name) = top_level_fullname(&json) else {
            // Unnamed at the top level is not a legal dependency; let the Avro
            // parser say so in its own words.
            kept.push(dep.as_str());
            continue;
        };
        match seen.get(&name) {
            Some(previous) if *previous == json => continue,
            Some(_) => {
                return Err(SchemaRegError::config(format!(
                    "the {} was given two different definitions of `{name}`; a type name \
                     resolves to one definition, so the set has to agree on it (a reference \
                     closure spanning two versions of the same subject does not)",
                    role.subject(),
                )));
            }
            None => {
                seen.insert(name, json);
                kept.push(dep.as_str());
            }
        }
    }
    Ok(kept)
}

/// The fullname a top-level Avro schema declares, without parsing it — which a
/// schema naming an external type does not survive.
///
/// A `name` containing a dot carries its own namespace and wins over the
/// `namespace` attribute, per the Avro specification.
fn top_level_fullname(json: &JsonValue) -> Option<String> {
    let object = json.as_object()?;
    let name = object.get("name")?.as_str()?;
    if name.contains('.') {
        return Some(name.to_string());
    }
    match object.get("namespace").and_then(JsonValue::as_str) {
        Some(namespace) if !namespace.is_empty() => Some(format!("{namespace}.{name}")),
        _ => Some(name.to_string()),
    }
}

// ── Errors ────────────────────────────────────────────────────────────────

fn missing_definition(
    missing: &str,
    referencing: Option<&str>,
    role: SchemaRole,
) -> SchemaRegError {
    let by = referencing.map_or_else(String::new, |name| format!("`{name}` "));
    // An unqualified name is also what a mistyped primitive looks like, and the
    // Avro parser reports both identically. A namespaced one is unambiguous.
    let typo_hint = if missing.contains('.') {
        ""
    } else {
        " (if it was meant to be one of Avro's primitive types, check the spelling)"
    };
    SchemaRegError::config(format!(
        "{} {by}references the type `{missing}`, which nothing in the schema set defines; {}{typo_hint}",
        role.subject(),
        role.remedy(),
    ))
}

/// A referenced type that exists, but only as part of another schema.
fn nested_definition(
    missing: &str,
    defined_inside: Option<&str>,
    role: SchemaRole,
) -> SchemaRegError {
    let inside = defined_inside.map_or_else(
        || "inside another schema in the set".to_string(),
        |name| format!("inside `{name}`"),
    );
    SchemaRegError::config(format!(
        "{} references `{missing}`, which is defined {inside} rather than as a schema of its \
         own; Avro resolves a cross-schema reference only to another schema's top-level type, \
         so give `{missing}` a schema of its own or inline the definition where it is used",
        role.subject(),
    ))
}

/// Translate the Avro parser's message for a name it could not resolve.
///
/// Both parser entry points report an unresolved type name as `Unknown
/// primitive type`, which says nothing about references. The name is the useful
/// part; the fix belongs next to it, worded as it is everywhere else here.
fn parse_failed(message: &str, role: SchemaRole, dependencies: &[&str]) -> SchemaRegError {
    const UNKNOWN: &str = "Unknown primitive type: ";
    if let Some(name) = message.strip_prefix(UNKNOWN) {
        // The parser resolves a nested definition roughly half the time; when
        // it did not, say the same thing the successful half says.
        return match nested_owner(name, dependencies) {
            Some(owner) => nested_definition(name, Some(&owner), role),
            None => missing_definition(name, None, role),
        };
    }
    if dependencies.is_empty() {
        return SchemaRegError::config(format!("invalid {}: {message}", role.subject()));
    }
    SchemaRegError::config(format!(
        "invalid {} (with {} referenced schema(s)): {message}",
        role.subject(),
        dependencies.len(),
    ))
}

/// Which supplied schema defines `name`, but only as a type nested inside
/// itself.
///
/// Reached only when parsing already failed, so cost does not matter. A schema
/// hiding a nested definition has no unresolved references of its own, so it
/// parses alone — which is what makes it inspectable here.
fn nested_owner(name: &str, dependencies: &[&str]) -> Option<String> {
    for dependency in dependencies {
        let Ok(schema) = AvroSchema::parse_str(dependency) else {
            continue;
        };
        let top_level = name_of(&schema);
        if top_level.as_deref() == Some(name) {
            continue;
        }
        if names_of(&schema).0.contains(name) {
            return top_level.or_else(|| Some("another supplied schema".to_string()));
        }
    }
    None
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use apache_avro::schema::ResolvedSchema;

    const ADDRESS: &str = r#"{"type":"record","name":"Address","namespace":"com.example",
        "fields":[{"name":"city","type":"string"}]}"#;
    const CUSTOMER: &str = r#"{"type":"record","name":"Customer","namespace":"com.example",
        "fields":[{"name":"home","type":"com.example.Address"}]}"#;
    const ORDER: &str = r#"{"type":"record","name":"Order","namespace":"com.example",
        "fields":[{"name":"buyer","type":"com.example.Customer"},
                  {"name":"shipTo","type":"com.example.Address"}]}"#;

    fn deps(schemas: &[&str]) -> Vec<String> {
        schemas.iter().map(|s| (*s).to_string()).collect()
    }

    fn ordered_names(resolved: &ResolvedAvroSchema) -> Vec<String> {
        resolved
            .schemata
            .iter()
            .map(|s| name_of(s).unwrap_or_else(|| "<anonymous>".into()))
            .collect()
    }

    /// The ordering exists for exactly one consumer: `apache-avro`'s one-pass
    /// name-table builder. Asserting our own order is not enough — assert that
    /// the thing it is built for accepts it.
    fn assert_codec_accepts(resolved: &ResolvedAvroSchema) {
        ResolvedSchema::try_from(resolved.codec_schemata())
            .expect("the codec must accept the order this module produced");
    }

    #[test]
    fn dependencies_are_sorted_definitions_first() {
        let resolved = ResolvedAvroSchema::parse(
            ORDER,
            &deps(&[CUSTOMER, ADDRESS]), // users before definitions
            SchemaRole::WriterLocal,
        )
        .expect("order must not matter");

        assert_eq!(
            ordered_names(&resolved),
            [
                "com.example.Address",
                "com.example.Customer",
                "com.example.Order"
            ]
        );
        assert_codec_accepts(&resolved);
    }

    /// An input that is already ordered must come back untouched, so the sort
    /// cannot churn a working set into a different one.
    #[test]
    fn an_already_ordered_set_is_left_alone() {
        let resolved =
            ResolvedAvroSchema::parse(ORDER, &deps(&[ADDRESS, CUSTOMER]), SchemaRole::WriterLocal)
                .expect("builds");
        assert_eq!(
            ordered_names(&resolved),
            [
                "com.example.Address",
                "com.example.Customer",
                "com.example.Order"
            ]
        );
    }

    /// A reference written relative to the enclosing namespace resolves to the
    /// same type as the fully-qualified spelling. Matching names by their raw
    /// text instead of their fully-qualified form would miss this edge and
    /// report a perfectly good dependency as missing.
    #[test]
    fn references_resolve_through_the_enclosing_namespace() {
        const INNER: &str = r#"{"type":"record","name":"Inner","namespace":"a.b",
            "fields":[{"name":"v","type":"int"}]}"#;
        // "Inner", not "a.b.Inner" — the namespace comes from the enclosing record.
        const OUTER: &str = r#"{"type":"record","name":"Outer","namespace":"a.b",
            "fields":[{"name":"inner","type":"Inner"}]}"#;

        let resolved = ResolvedAvroSchema::parse(OUTER, &deps(&[INNER]), SchemaRole::WriterLocal)
            .expect("a relative reference is still a reference");
        assert_eq!(ordered_names(&resolved), ["a.b.Inner", "a.b.Outer"]);
        assert_codec_accepts(&resolved);
    }

    /// A reference to a type defined *inside* another schema is refused, and
    /// refused every time.
    ///
    /// `apache-avro` looks such a name up in a `HashMap` of top-level inputs
    /// and, failing that, in whatever it has parsed so far, so whether it
    /// resolves depends on hash iteration order — roughly one parse in two.
    #[test]
    fn a_reference_to_a_nested_definition_is_refused_deterministically() {
        const WRAPPER: &str = r#"{"type":"record","name":"Wrapper","namespace":"com.example",
            "fields":[{"name":"address","type":{"type":"record","name":"Address",
                       "namespace":"com.example","fields":[{"name":"city","type":"string"}]}}]}"#;
        const USER: &str = r#"{"type":"record","name":"User","namespace":"com.example",
            "fields":[{"name":"home","type":"com.example.Address"},
                      {"name":"wrapped","type":"com.example.Wrapper"}]}"#;

        // Whichever way the coin lands inside the parser, the outcome is the
        // same error.
        for _ in 0..64 {
            let err = ResolvedAvroSchema::parse(USER, &deps(&[WRAPPER]), SchemaRole::WriterLocal)
                .expect_err("a nested definition is not a schema of its own");
            let message = err.to_string();
            assert!(message.contains("com.example.Address"), "{message}");
            assert!(
                message.contains("defined inside `com.example.Wrapper`"),
                "the message must be the same one every time: {message}"
            );
        }
    }

    /// Enums and fixed types are named types too, and are referenced the same
    /// way records are.
    #[test]
    fn enums_and_fixed_types_participate_in_the_order() {
        const SUIT: &str = r#"{"type":"enum","name":"Suit","namespace":"cards",
            "symbols":["HEARTS","SPADES"]}"#;
        const HASH: &str = r#"{"type":"fixed","name":"Hash","namespace":"cards","size":16}"#;
        const CARD: &str = r#"{"type":"record","name":"Card","namespace":"cards",
            "fields":[{"name":"suit","type":"cards.Suit"},
                      {"name":"id","type":"cards.Hash"}]}"#;

        let resolved =
            ResolvedAvroSchema::parse(CARD, &deps(&[HASH, SUIT]), SchemaRole::WriterLocal)
                .expect("builds");
        assert_eq!(
            ordered_names(&resolved),
            ["cards.Hash", "cards.Suit", "cards.Card"]
        );
        assert_codec_accepts(&resolved);
    }

    #[test]
    fn a_cycle_between_schemas_is_reported() {
        const LEFT: &str = r#"{"type":"record","name":"Left","namespace":"x",
            "fields":[{"name":"r","type":["null","x.Right"]}]}"#;
        const RIGHT: &str = r#"{"type":"record","name":"Right","namespace":"x",
            "fields":[{"name":"l","type":["null","x.Left"]}]}"#;

        let err = ResolvedAvroSchema::parse(LEFT, &deps(&[RIGHT]), SchemaRole::WriterLocal)
            .expect_err("no order can satisfy both");
        let message = err.to_string();
        assert!(message.contains("circular"), "{message}");
        assert!(
            message.contains("x.Left") && message.contains("x.Right"),
            "{message}"
        );
    }

    #[test]
    fn a_self_reference_is_not_a_cycle() {
        const NODE: &str = r#"{"type":"record","name":"Node","namespace":"x",
            "fields":[{"name":"next","type":["null","x.Node"]}]}"#;

        let resolved = ResolvedAvroSchema::parse(NODE, &[], SchemaRole::WriterLocal)
            .expect("a linked list needs no dependencies");
        assert!(
            resolved.schemata.is_empty(),
            "a self-contained root needs no name table of its own"
        );
        // ...but the codec still needs one, built from the root.
        assert_codec_accepts(&resolved);
    }

    #[test]
    fn an_identical_duplicate_dependency_is_dropped() {
        let resolved = ResolvedAvroSchema::parse(
            CUSTOMER,
            &deps(&[ADDRESS, ADDRESS]),
            SchemaRole::WriterLocal,
        )
        .expect("the same definition twice is still one definition");
        assert_eq!(
            ordered_names(&resolved),
            ["com.example.Address", "com.example.Customer"]
        );
    }

    /// Whitespace and key order are not schema differences.
    #[test]
    fn a_cosmetically_different_duplicate_is_still_a_duplicate() {
        const SAME: &str = r#"{ "namespace" : "com.example" , "name":"Address" , "type":"record" ,
                 "fields" : [ { "name" : "city" , "type" : "string" } ] }"#;

        ResolvedAvroSchema::parse(CUSTOMER, &deps(&[ADDRESS, SAME]), SchemaRole::WriterLocal)
            .expect("the two spellings describe one schema");
    }

    #[test]
    fn contradictory_duplicates_are_rejected() {
        const OTHER: &str = r#"{"type":"record","name":"Address","namespace":"com.example",
            "fields":[{"name":"city","type":"string"},{"name":"zip","type":"string"}]}"#;

        let err =
            ResolvedAvroSchema::parse(CUSTOMER, &deps(&[ADDRESS, OTHER]), SchemaRole::WriterLocal)
                .expect_err("two definitions of one name");
        assert!(err.to_string().contains("com.example.Address"), "{err}");
    }

    /// A dependency repeating the root's own definition is dropped rather than
    /// colliding with it.
    #[test]
    fn a_dependency_repeating_the_root_is_dropped() {
        let resolved =
            ResolvedAvroSchema::parse(ADDRESS, &deps(&[ADDRESS]), SchemaRole::WriterLocal)
                .expect("builds");
        assert!(resolved.schemata.is_empty());
        assert_eq!(resolved.fullname().as_deref(), Some("com.example.Address"));
    }

    #[test]
    fn the_dependency_count_is_bounded() {
        let many: Vec<String> = (0..=MAX_REFERENCES)
            .map(|i| format!(r#"{{"type":"record","name":"R{i}","fields":[]}}"#))
            .collect();
        let err = ResolvedAvroSchema::parse(ADDRESS, &many, SchemaRole::WriterLocal)
            .expect_err("the bound is enforced before parsing");
        assert!(err.to_string().contains("256"), "{err}");
    }

    #[test]
    fn the_error_names_the_knob_for_the_role() {
        for (role, knob) in [
            (
                SchemaRole::WriterLocal,
                "AvroSchemaEncoderBuilder::dependencies",
            ),
            (
                SchemaRole::Reader,
                "AvroSchemaDecoderBuilder::reader_dependencies",
            ),
            (SchemaRole::WriterRegistry, "references"),
        ] {
            let err = ResolvedAvroSchema::parse(CUSTOMER, &[], role).expect_err("unresolvable");
            assert!(err.to_string().contains(knob), "{role:?}: {err}");
        }
    }

    #[test]
    fn top_level_names_follow_the_avro_rules() {
        let fullname =
            |json: &str| top_level_fullname(&serde_json::from_str(json).expect("valid JSON"));
        assert_eq!(fullname(ADDRESS).as_deref(), Some("com.example.Address"));
        assert_eq!(
            fullname(r#"{"type":"record","name":"com.example.Address","fields":[]}"#).as_deref(),
            Some("com.example.Address"),
            "a dotted name carries its own namespace"
        );
        assert_eq!(
            fullname(r#"{"type":"record","name":"a.b.C","namespace":"ignored","fields":[]}"#)
                .as_deref(),
            Some("a.b.C"),
            "and wins over the namespace attribute"
        );
        assert_eq!(
            fullname(r#"{"type":"record","name":"Bare","fields":[]}"#).as_deref(),
            Some("Bare")
        );
        assert_eq!(
            fullname(r#"["null","string"]"#),
            None,
            "a union has no name"
        );
    }

    #[test]
    fn parsed_input_takes_the_same_route() {
        let parsed = AvroSchema::parse_list([ORDER, CUSTOMER, ADDRESS]).expect("parses as a set");
        let (root, rest) = parsed.split_first().expect("three schemas");

        let resolved =
            ResolvedAvroSchema::from_parsed(root.clone(), rest.to_vec(), SchemaRole::Reader)
                .expect("builds");
        assert_eq!(
            ordered_names(&resolved),
            [
                "com.example.Address",
                "com.example.Customer",
                "com.example.Order"
            ]
        );
        assert_codec_accepts(&resolved);
    }

    #[test]
    fn parsed_input_rejects_a_dangling_reference() {
        let parsed = AvroSchema::parse_list([CUSTOMER, ADDRESS]).expect("parses as a set");
        let err =
            ResolvedAvroSchema::from_parsed(parsed[0].clone(), Vec::new(), SchemaRole::Reader)
                .expect_err("Address is missing");
        assert!(err.to_string().contains("com.example.Address"), "{err}");
    }
}
