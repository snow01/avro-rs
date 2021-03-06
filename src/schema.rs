//! Logic for parsing and interacting with schemas in Avro format.
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use digest::Digest;
use failure::Error;
use regex::Regex;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde::Deserialize;
use serde_json::{self, Map, Value as JsonValue};
use time::Duration;

use crate::types::{
    DecaySetting, Value as AvroValue, ValueComparison, ValueSetting, VALUE_COMPARATOR,
};
use crate::util::string_from_json_value;
use crate::util::{vec_from_json_value, MapHelper};

lazy_static! {
    static ref LRU_LIMIT_REGEX:Regex = Regex::new("(?P<value>[[:digit:]]+)[[:space:]]*(?P<type>days|hour|minute)[[:space:]]*(?:;[[:space:]]*(?P<count>[[:digit:]]+)[[:space:]]*)?$").unwrap();
}

/// Describes errors happened while parsing Avro schemas.
#[derive(Fail, Debug)]
#[fail(display = "Failed to parse schema: {}", _0)]
pub struct ParseSchemaError(String);

impl ParseSchemaError {
    pub fn new<S>(msg: S) -> ParseSchemaError
    where
        S: Into<String>,
    {
        ParseSchemaError(msg.into())
    }
}

/// Represents an Avro schema fingerprint
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/current/spec.html#schema_fingerprints)
pub struct SchemaFingerprint {
    pub bytes: Vec<u8>,
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.bytes
                .iter()
                .map(|byte| format!("{:02x}", byte))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
pub enum Schema {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// A `bytes` Avro schema.
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    /// `String` represents a unicode character sequence.
    String,
    /// A `array` Avro schema. Avro arrays are required to have the same type for each element.
    /// This variant holds the `Schema` for the array element type.
    Array(Box<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Box<Schema>),
    /// A `union` Avro schema.
    Union(UnionSchema),

    UnionRecord(UnionRecordSchema),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        name: Name,
        doc: Documentation,
        fields: Vec<RecordField>,
        lookup: HashMap<String, usize>,
    },
    /// An `enum` Avro schema.
    Enum {
        name: Name,
        doc: Documentation,
        symbols: Vec<String>,
    },
    /// A `fixed` Avro schema.
    Fixed {
        name: Name,
        size: usize,
    },

    Date(Option<DateIndexKind>),

    Set,

    // capture limit and limit by = supported values: days, hour, minute, count
    LruSet(LruLimit),

    // optional type
    Optional(Box<Schema>),

    Counter,

    Max(Box<Schema>),

    Decay(Box<Schema>, DecayMeta),

    ValueComparator(Box<Schema>, ValueComparison),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DateIndexKind {
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LruLimit {
    Days(Duration, i32),
    Hour(Duration, i32),
    Minute(Duration, i32),
}

impl Serialize for LruLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            LruLimit::Days(limit, count) => {
                serializer.serialize_str(&format!("{} days;{}", limit.num_days(), count))
            }
            LruLimit::Hour(limit, count) => {
                serializer.serialize_str(&format!("{} hour;{}", limit.num_hours(), count))
            }
            LruLimit::Minute(limit, count) => {
                serializer.serialize_str(&format!("{} minute;{}", limit.num_minutes(), count))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecayMeta {
    decay_type: String,
    decay_rates: Vec<String>,
    fields: Vec<String>,
}

impl Serialize for DecayMeta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("decay_type", &self.decay_type)?;
        map.serialize_entry("decay_rate", &self.decay_rates)?;
        map.serialize_entry("fields", &self.fields)?;
        map.end()
    }
}

impl DecayMeta {
    pub fn to_decay_settings(&self) -> Option<DecaySetting> {
        if self.decay_type.is_empty() || self.decay_rates.is_empty() {
            return None;
        }
        Some(DecaySetting {
            decay_type: self.decay_type.clone(),
            decay_rates: self.decay_rates.clone(),
        })
    }
}

/// This type is used to simplify enum variant comparison between `Schema` and `types::Value`.
/// It may have utility as part of the public API, but defining as `pub(crate)` for now.
///
/// **NOTE** This type was introduced due to a limitation of `mem::discriminant` requiring a _value_
/// be constructed in order to get the discriminant, which makes it difficult to implement a
/// function that maps from `Discriminant<Schema> -> Discriminant<Value>`. Conversion into this
/// intermediate type should be especially fast, as the number of enum variants is small, which
/// _should_ compile into a jump-table for the conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SchemaKind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array,
    Map,
    Union,
    Record,
    UnionRecord,
    Enum,
    Fixed,
    Date,
    Set,
    LruSet,
    Optional,
    Counter,
    Max,
    Decay,
    ValueComparator,
}

impl<'a> From<&'a Schema> for SchemaKind {
    #[inline(always)]
    fn from(schema: &'a Schema) -> SchemaKind {
        // NOTE: I _believe_ this will always be fast as it should convert into a jump table.
        match schema {
            Schema::Null => SchemaKind::Null,
            Schema::Boolean => SchemaKind::Boolean,
            Schema::Int => SchemaKind::Int,
            Schema::Long => SchemaKind::Long,
            Schema::Float => SchemaKind::Float,
            Schema::Double => SchemaKind::Double,
            Schema::Bytes => SchemaKind::Bytes,
            Schema::String => SchemaKind::String,
            Schema::Array(_) => SchemaKind::Array,
            Schema::Map(_) => SchemaKind::Map,
            Schema::Union(_) => SchemaKind::Union,
            Schema::Record { .. } => SchemaKind::Record,
            Schema::Enum { .. } => SchemaKind::Enum,
            Schema::Fixed { .. } => SchemaKind::Fixed,
            Schema::Date(_) => SchemaKind::Date,
            Schema::Set => SchemaKind::Set,
            Schema::LruSet(_) => SchemaKind::LruSet,
            Schema::Optional(_) => SchemaKind::Optional,
            Schema::Counter => SchemaKind::Counter,
            Schema::Max(_) => SchemaKind::Max,
            Schema::UnionRecord(_) => SchemaKind::UnionRecord,
            Schema::Decay(_, _) => SchemaKind::Decay,
            Schema::ValueComparator(_, _) => SchemaKind::ValueComparator,
        }
    }
}

impl<'a> From<&'a AvroValue> for SchemaKind {
    #[inline(always)]
    fn from(value: &'a AvroValue) -> SchemaKind {
        match value {
            AvroValue::Null => SchemaKind::Null,
            AvroValue::Boolean(_, _) => SchemaKind::Boolean,
            AvroValue::Int(_, _) => SchemaKind::Int,
            AvroValue::Long(_, _) => SchemaKind::Long,
            AvroValue::Float(_, _) => SchemaKind::Float,
            AvroValue::Double(_, _) => SchemaKind::Double,
            AvroValue::Bytes(_, _) => SchemaKind::Bytes,
            AvroValue::String(_, _) => SchemaKind::String,
            AvroValue::Array(_, _) => SchemaKind::Array,
            AvroValue::Map(_, _) => SchemaKind::Map,
            AvroValue::Union(_, _) => SchemaKind::Union,
            AvroValue::Record(_, _) => SchemaKind::Record,
            AvroValue::Enum(_, _, _) => SchemaKind::Enum,
            AvroValue::Fixed(_, _, _) => SchemaKind::Fixed,
            AvroValue::Date(_, _) => SchemaKind::Date,
            AvroValue::Set(_, _) => SchemaKind::Set,
            AvroValue::LruSet(_, _, _) => SchemaKind::LruSet,
            AvroValue::Optional(_, _) => SchemaKind::Optional,
            AvroValue::Counter(_, _) => SchemaKind::Counter,
            AvroValue::Max(_, _) => SchemaKind::Max,
            AvroValue::UnionRecord(_, _, _) => SchemaKind::UnionRecord,
            AvroValue::DecayRecord(_, _) => SchemaKind::Decay,
            AvroValue::ValueComparator(_, _, _) => SchemaKind::ValueComparator,
        }
    }
}

/// Represents names for `record`, `enum` and `fixed` Avro schemas.
///
/// Each of these `Schema`s have a `fullname` composed of two parts:
///   * a name
///   * a namespace
///
/// `aliases` can also be defined, to facilitate schema evolution.
///
/// More information about schema names can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
#[derive(Clone, Debug, PartialEq)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,

    pub index: Option<bool>,
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// No `namespace` nor `aliases` will be defined.
    pub fn new(name: &str) -> Name {
        Name {
            name: name.to_owned(),
            namespace: None,
            aliases: None,
            index: None,
        }
    }

    /// Parse a `serde_json::Value` into a `Name`.
    fn parse(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let name = complex
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` field"))?;

        let namespace = complex.string("namespace");

        let aliases: Option<Vec<String>> = complex
            .get("aliases")
            .and_then(JsonValue::as_array)
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(JsonValue::as_str)
                    .map(|alias| alias.map(ToString::to_string))
                    .collect::<Option<_>>()
            });

        Ok(Name {
            name,
            namespace,
            aliases,
            index: complex.index(),
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullname(&self, default_namespace: Option<&str>) -> String {
        if self.name.contains('.') {
            self.name.clone()
        } else {
            let namespace = self
                .namespace
                .as_ref()
                .map(AsRef::as_ref)
                .or(default_namespace);

            match namespace {
                Some(ref namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<JsonValue>,
    /// Schema of the field.
    pub schema: Schema,
    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub order: RecordFieldOrder,
    /// Position of the field in the list of `field` of its parent `Schema`
    pub position: usize,

    pub index: Option<bool>,
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {
    /// Parse a `serde_json::Value` into a `RecordField`.
    fn parse(field: &Map<String, JsonValue>, position: usize) -> Result<Self, Error> {
        let name = field
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` in record field"))?;

        // TODO: "type" = "<record name>"
        let schema = field
            .get("type")
            .ok_or_else(|| ParseSchemaError::new("No `type` in record field").into())
            .and_then(|_type| Schema::parse_complex(field))?;

        let default = field.get("default").cloned();

        let order = field
            .get("order")
            .and_then(JsonValue::as_str)
            .and_then(|order| match order {
                "ascending" => Some(RecordFieldOrder::Ascending),
                "descending" => Some(RecordFieldOrder::Descending),
                "ignore" => Some(RecordFieldOrder::Ignore),
                _ => None,
            })
            .unwrap_or_else(|| RecordFieldOrder::Ascending);

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            schema,
            order,
            position,
            index: field.index(),
        })
    }
}

impl From<(String, Schema, usize)> for RecordField {
    fn from((name, schema, position): (String, Schema, usize)) -> Self {
        RecordField {
            name,
            doc: None,
            default: None,
            schema,
            order: RecordFieldOrder::Ascending,
            position,
            index: None,
        }
    }
}

fn record_schema_from_fields(
    name: &str,
    fields: Vec<(String, Schema, Option<ValueSetting>)>,
) -> Schema {
    let mut record_fields: Vec<RecordField> = Vec::with_capacity(fields.len());
    let mut lookup = HashMap::with_capacity(fields.len());
    for (position, (name, schema, value_settings)) in fields.into_iter().enumerate() {
        let mut record_field: RecordField = (name.clone(), schema, position).into();
        record_field.index = value_settings.map(|v| v.index);
        record_fields.push(record_field);
        lookup.insert(name, position);
    }

    Schema::Record {
        name: Name::new(name),
        doc: None,
        fields: record_fields,
        lookup,
    }
}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: HashMap<SchemaKind, usize>,
}

impl UnionSchema {
    pub(crate) fn new(schemas: Vec<Schema>) -> Result<Self, Error> {
        let mut vindex = HashMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = schema {
                Err(ParseSchemaError::new(
                    "Unions may not directly contain a union",
                ))?;
            }
            let kind = SchemaKind::from(schema);
            if vindex.insert(kind, i).is_some() {
                Err(ParseSchemaError::new(
                    "Unions cannot contain duplicate types",
                ))?;
            }
        }
        Ok(UnionSchema {
            schemas,
            variant_index: vindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        !self.schemas.is_empty() && self.schemas[0] == Schema::Null
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this enum.
    pub fn find_schema(&self, value: &crate::types::Value) -> Option<(usize, &Schema)> {
        let kind = SchemaKind::from(value);
        self.variant_index
            .get(&kind)
            .cloned()
            .map(|i| (i, &self.schemas[i]))
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

#[derive(Debug, Clone)]
pub struct UnionRecordSchema {
    //_type: Option<String>, // Not needed
    /// schema type to index mapping
    schemas_by_type: HashMap<String, usize>,
    /// schema_by_index
    schemas: Vec<Schema>,
}

impl UnionRecordSchema {
    pub(crate) fn new(schemas: Vec<Schema>) -> Result<Self, Error> {
        let mut schemas_by_type = HashMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Record { name, .. } = schema {
                if schemas_by_type.insert(name.name.clone(), i).is_some() {
                    Err(ParseSchemaError::new(
                        "UnionRecord cannot contain record schema with duplicate name",
                    ))?;
                }
            } else {
                Err(ParseSchemaError::new(
                    "Complex Unions are only supported for record type",
                ))?;
            }
        }
        Ok(UnionRecordSchema {
            schemas_by_type,
            schemas,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    pub fn type_indexes(&self) -> &HashMap<String, usize> {
        &self.schemas_by_type
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        !self.schemas.is_empty() && self.schemas[0] == Schema::Null
    }

    pub fn find_schema_by_type(&self, value: &str) -> Option<(usize, &Schema)> {
        self.schemas_by_type
            .get(value)
            .cloned()
            .map(|i| (i, &self.schemas[i]))
    }
}

impl PartialEq for UnionRecordSchema {
    fn eq(&self, other: &UnionRecordSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

impl Schema {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Self, Error> {
        let value = serde_json::from_str(input)?;
        Self::parse(&value)
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub fn parse(value: &JsonValue) -> Result<Self, Error> {
        match *value {
            JsonValue::String(ref t) => Schema::parse_primitive(t.as_str()),
            JsonValue::Object(ref data) => Schema::parse_complex(data),
            JsonValue::Array(ref data) => Schema::parse_union(data),
            _ => Err(ParseSchemaError::new("Must be a JSON string, object or array").into()),
        }
    }

    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self).unwrap();
        parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/spec.html#schema_fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.input(self.canonical_form());
        SchemaFingerprint {
            bytes: d.result().to_vec(),
        }
    }

    /// Parse a `serde_json::Value` representing a primitive Avro type into a
    /// `Schema`.
    fn parse_primitive(primitive: &str) -> Result<Self, Error> {
        match primitive {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            "set" => Ok(Schema::Set),
            "counter" => Ok(Schema::Counter),
            "date" => Ok(Schema::Date(None)),
            other => Err(ParseSchemaError::new(format!("Unknown type: {}", other)).into()),
        }
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        match complex.get("type") {
            Some(&JsonValue::String(ref t)) => match t.as_str() {
                "record" => Schema::parse_record(complex),
                "enum" => Schema::parse_enum(complex),
                "array" => Schema::parse_array(complex),
                "map" => Schema::parse_map(complex),
                "fixed" => Schema::parse_fixed(complex),
                "date" => Schema::parse_date(complex),
                "lru_set" => Schema::parse_lru_set(complex),
                "optional" => Schema::parse_optional(complex),
                "max" => Schema::parse_max(complex),
                "decay" => Schema::parse_decay(complex),
                VALUE_COMPARATOR => Schema::parse_value_comparator(complex),
                other => Schema::parse_primitive(other),
            },
            Some(&JsonValue::Object(ref data)) => Schema::parse_complex(data) /*match data.get("type") {
                Some(ref value) => Schema::parse(value),
                None => Err(
                    ParseSchemaError::new(format!("Unknown complex type: {:?}", complex)).into(),
                ),
            }*/,
            Some(&JsonValue::Array(ref data)) => Schema::parse_union(data),
            _ => Err(ParseSchemaError::new(format!("No `type` in complex type: {:?}", complex).to_string()).into()),
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let mut lookup = HashMap::new();

        let fields: Vec<RecordField> = complex
            .get("fields")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| ParseSchemaError::new("No `fields` in record").into())
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(JsonValue::as_object)
                    .enumerate()
                    .map(|(position, field)| RecordField::parse(field, position))
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            lookup.insert(field.name.clone(), field.position);
        }

        Ok(Schema::Record {
            name,
            doc: complex.doc(),
            fields,
            lookup,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let symbols = complex
            .get("symbols")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| ParseSchemaError::new("No `symbols` field in enum"))
            .and_then(|symbols| {
                symbols
                    .iter()
                    .map(|symbol| symbol.as_str().map(ToString::to_string))
                    .collect::<Option<_>>()
                    .ok_or_else(|| ParseSchemaError::new("Unable to parse `symbols` in enum"))
            })?;

        Ok(Schema::Enum {
            name,
            doc: complex.doc(),
            symbols,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        complex
            .get("items")
            .ok_or_else(|| ParseSchemaError::new("No `items` in array").into())
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Array(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        complex
            .get("values")
            .ok_or_else(|| ParseSchemaError::new("No `values` in map").into())
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Map(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(items: &[JsonValue]) -> Result<Self, Error> {
        items
            .iter()
            .map(Schema::parse)
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| {
                if Self::all_schemas_are_record_kind(&schemas) {
                    Ok(Schema::UnionRecord(UnionRecordSchema::new(schemas)?))
                } else {
                    Ok(Schema::Union(UnionSchema::new(schemas)?))
                }
            })
    }

    fn all_schemas_are_record_kind(schemas: &[Schema]) -> bool {
        for schema in schemas {
            if SchemaKind::from(schema) != SchemaKind::Record {
                return false;
            }
        }
        true
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    /*    fn parse_union_record(items: &[JsonValue]) -> Result<Self, Error> {
        items
            .iter()
            .map(Schema::parse)
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| Ok(Schema::UnionRecord(UnionRecordSchema::new(schemas)?)))
    }*/

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let size = complex
            .get("size")
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| ParseSchemaError::new("No `size` in fixed"))?;

        Ok(Schema::Fixed {
            name,
            size: size as usize,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_date(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        match complex.get("index_kind") {
            Some(v) => {
                let v = v.as_str().ok_or_else(|| {
                    failure::err_msg(format!("Not a valid index_kind value for date type: {}", v))
                })?;

                let kind = match v {
                    "day" => Ok(DateIndexKind::Day),
                    "hour" => Ok(DateIndexKind::Hour),
                    "minute" => Ok(DateIndexKind::Minute),
                    "second" => Ok(DateIndexKind::Second),
                    _ => {
                        Err(failure::err_msg(format!("Not a valid index_kind value for date type: {}. Allowed values: day, hour, minute, second", v)))
                    }
                }?;

                Ok(Schema::Date(Some(kind)))
            }
            None => Ok(Schema::Date(None)),
        }
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_lru_set(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        complex
            .get("limit")
            .ok_or_else(|| ParseSchemaError::new("No `limit` specified for lru_set").into())
            .and_then(Schema::parse_lru_limit)
            .map(Schema::LruSet)
    }

    fn parse_lru_limit(v: &JsonValue) -> Result<LruLimit, Error> {
        v.as_str()
            .ok_or_else(|| {
                failure::err_msg(format!("Not a valid limit value for lru_set type: {}", v))
            })
            .map(|s| LRU_LIMIT_REGEX.captures(s))
            .and_then(|r| {
                r.ok_or_else(|| {
                    failure::err_msg(format!("Not a valid limit value for lru_set type: {}", v))
                })
                .and_then(|caps| {
                    let value = caps.name("value").unwrap().as_str();
                    let value = value.parse::<i64>()?;

                    let count = caps
                        .name("count")
                        .map_or(Ok(0), |m| m.as_str().parse::<i32>())?;

                    caps.name("type")
                        .map_or(Err(failure::err_msg("No limit type provided")), |r| match r
                            .as_str()
                        {
                            "days" => Ok(LruLimit::Days(Duration::days(value), count)),
                            "hour" => Ok(LruLimit::Hour(Duration::hours(value), count)),
                            "minute" => Ok(LruLimit::Minute(Duration::minutes(value), count)),
                            other => Err(failure::err_msg(format!(
                                "Not a valid limit value for lru_set type: {}",
                                other
                            ))),
                        })
                })
            })
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_optional(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        complex
            .get("value")
            .ok_or_else(|| ParseSchemaError::new("No `value` defined for optional").into())
            .and_then(|value| Schema::parse(value))
            .map(|schema| Schema::Optional(Box::new(schema)))
    }

    fn parse_max(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        complex
            .get("value")
            .ok_or_else(|| ParseSchemaError::new("No `value` defined for max").into())
            .and_then(|value| {
                let value = Schema::parse(value)?;
                let kind = SchemaKind::from(&value);

                if let SchemaKind::Int | SchemaKind::Long | SchemaKind::Float | SchemaKind::Date =
                    kind
                {
                    Ok(value)
                } else {
                    Err(ParseSchemaError::new(
                        "Either of int, long, float, or date is allowed for max",
                    )
                    .into())
                }
            })
            .map(|schema| Schema::Max(Box::new(schema)))
    }

    fn parse_value_comparator(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let condition = complex
            .get("condition")
            .ok_or_else(|| ParseSchemaError::new("No `condition` define for 'value_comparator'"))?;
        let condition =
            serde_json::from_value::<ValueComparison>(condition.clone()).map_err(|e| {
                failure::err_msg(format!("invalid condition value provided. Error {} ", e))
            })?;

        let data_schema = complex
            .get("value")
            .ok_or_else(|| {
                ParseSchemaError::new("No `value` defined for 'value_comparator'").into()
            })
            .and_then(|value| Schema::parse(value))?;

        let mut fields = Vec::with_capacity(2);
        fields.push((
            String::from("_value"),
            Schema::Long,
            Some(ValueSetting { index: false }),
        ));
        fields.push((String::from("data"), data_schema, None));

        let inner = record_schema_from_fields(VALUE_COMPARATOR, fields);
        let schema = Schema::ValueComparator(Box::new(inner), condition);

        Ok(schema)
    }

    fn parse_decay(complex: &Map<String, JsonValue>) -> Result<Self, Error> {
        let decay_type = complex
            .get("decay_type")
            .ok_or_else(|| ParseSchemaError::new("No `decay_type` defined for decay"))?;
        let decay_rate = complex
            .get("decay_rates")
            .ok_or_else(|| ParseSchemaError::new("No `decay_rates` defined for decay"))?;
        let fields = complex
            .get("fields")
            .ok_or_else(|| ParseSchemaError::new("No `fields` defined for decay"))?;

        let decay_type =
            string_from_json_value(decay_type, "Invalid `decay_type` defined for decay")?;
        let decay_rates =
            vec_from_json_value(decay_rate, "Invalid `decay_rates` defined for decay")?;
        let fields = vec_from_json_value(fields, "Invalid `fields` defined for decay")?;

        let decay_meta = DecayMeta {
            decay_rates: decay_rates.clone(),
            decay_type: decay_type.clone(),
            fields: fields.clone(),
        };

        let mut record_fields = Vec::with_capacity(fields.len());
        let mut lookup = HashMap::new();

        for index in 0..fields.len() {
            let field = &fields[index];
            let record = Self::get_decay_record(&decay_rates, field, index);
            lookup.insert(field.clone(), index);
            record_fields.push(record);
        }

        let name = format!("{}_decay", decay_type);
        let record = Schema::Record {
            name: Name::new(name.as_str()), // Todo : Sohan
            doc: None,
            fields: record_fields,
            lookup,
        };

        Ok(Schema::Decay(Box::new(record), decay_meta))
    }

    fn get_decay_record(decay_rates: &Vec<String>, name: &String, index: usize) -> RecordField {
        let mut lookup_map = HashMap::with_capacity(2 + decay_rates.len());
        let mut record_fields = Vec::with_capacity(2 + decay_rates.len());

        //timestamp field
        let timestamp_field: RecordField = ("timestamp".to_string(), Schema::Long, 0).into();
        lookup_map.insert("timestamp".to_string(), 0);
        record_fields.push(timestamp_field);

        //count field
        let count_field: RecordField = ("count".to_string(), Schema::Long, 1).into();
        lookup_map.insert("count".to_string(), 1);
        record_fields.push(count_field);

        let bound_index = decay_rates.len().min(2);
        for i in 0..bound_index {
            let decay_rate = &decay_rates[i];
            let formatted_decay_rate = decay_rate.trim().replace(" ", "_");
            let field_name = format!("score_{}", formatted_decay_rate);

            let score_field: RecordField = (
                field_name.clone(),
                Schema::Optional(Box::new(Schema::Double)),
                2 + i,
            )
                .into();
            lookup_map.insert(field_name.clone(), 2 + i);
            record_fields.push(score_field)
        }

        (
            name.clone(),
            Schema::Optional(Box::new(Schema::Record {
                name: Name::new(name),
                doc: None,
                fields: record_fields,
                lookup: lookup_map,
            })),
            index,
        )
            .into()
    }

    pub fn get_type(&self) -> String {
        match self {
            Schema::Null => String::from("null"),
            Schema::Boolean => String::from("bool"),
            Schema::Int => String::from("int"),
            Schema::Long => String::from("long"),
            Schema::Float => String::from("float"),
            Schema::Double => String::from("double"),
            Schema::Bytes => String::from("bytes"),
            Schema::String => String::from("string"),
            Schema::Array(_) => String::from("array"),
            Schema::Map(_) => String::from("map"),
            Schema::Union(_) => String::from("union"),
            Schema::Record { .. } => String::from("record"),
            Schema::Enum { .. } => String::from("enum"),
            Schema::Fixed { .. } => String::from("fixed"),
            Schema::Date(_) => String::from("date"),
            Schema::Set => String::from("set"),
            Schema::LruSet(_) => String::from("lru_set"),
            Schema::Optional(_) => String::from("optional"),
            Schema::Counter => String::from("counter"),
            Schema::Max(_) => String::from("max"),
            Schema::UnionRecord(_) => String::from("union_record"),
            Schema::Decay(_, _) => String::from("decay"),
            Schema::ValueComparator(_, _) => VALUE_COMPARATOR.to_string(),
        }
    }
}

impl<'de> Deserialize<'de> for Schema {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Schema, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        JsonValue::deserialize(deserializer).and_then(|value| {
            Schema::parse(&value).map_err(|e| {
                serde::de::Error::custom(format!(
                    "Error in parsing Spec json ({}) ==> {}",
                    value, e
                ))
            })
        })
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Counter => serializer.serialize_str("counter"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            }
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            }
            Schema::Union(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Record {
                ref name,
                ref doc,
                ref fields,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = name.aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;

                if let Some(ref index) = name.index {
                    map.serialize_entry("index", &index)?;
                }

                map.end()
            }
            Schema::Enum {
                ref name,
                ref symbols,
                ..
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;
                map.end()
            }
            Schema::Fixed { ref name, ref size } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("size", size)?;
                map.end()
            }
            Schema::Set => serializer.serialize_str("set"),
            Schema::Date(ref index_kind) => match index_kind {
                Some(index_kind) => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "date")?;

                    let index_kind_str = match index_kind {
                        DateIndexKind::Day => "day",
                        DateIndexKind::Hour => "hour",
                        DateIndexKind::Minute => "minute",
                        DateIndexKind::Second => "second",
                    };

                    map.serialize_entry("index_kind", index_kind_str)?;

                    map.end()
                }
                None => serializer.serialize_str("date"),
            },
            Schema::LruSet(ref limit) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "lru_set")?;
                map.serialize_entry("limit", limit)?;
                map.end()
            }
            Schema::Optional(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "optional")?;
                map.serialize_entry("value", &*inner.clone())?;
                map.end()
            }
            Schema::Max(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "max")?;
                map.serialize_entry("value", &*inner.clone())?;
                map.end()
            }
            Schema::UnionRecord(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Decay(ref _inner_schema, ref decay_meta) => {
                // this will used while reading (should be same as json when creating write schema)
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "decay")?;
                map.serialize_entry("decay_type", &decay_meta.decay_type)?;
                map.serialize_entry("decay_rates", &decay_meta.decay_rates)?;
                map.serialize_entry("fields", &decay_meta.fields)?;
                map.end()
            }

            Schema::ValueComparator(ref inner, condition) => {
                // this will used while reading (should be same as json when creating write schema)
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", VALUE_COMPARATOR)?;
                map.serialize_entry("condition", &condition)?;
                if let Schema::Record { fields, lookup, .. } = &**inner {
                    if let Some(pos) = lookup.get("data") {
                        if let Some(field) = fields.get(pos.clone()) {
                            map.serialize_entry("value", &field.schema)?;
                        }
                    }
                }
                map.end()
            }
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(ref default) = self.default {
            map.serialize_entry("default", default)?;
        }

        if let Some(ref index) = self.index {
            map.serialize_entry("index", &index)?;
        }

        map.end()
    }
}

/// Parses a **valid** avro schema into the Parsing Canonical Form.
/// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
fn parsing_canonical_form(schema: &JsonValue) -> String {
    match schema {
        JsonValue::Object(map) => pcf_map(map),
        JsonValue::String(s) => pcf_string(s),
        JsonValue::Array(v) => pcf_array(v),
        _ => unreachable!(),
    }
}

fn pcf_map(schema: &Map<String, serde_json::Value>) -> String {
    // Look for the namespace variant up front.
    let ns = schema.get("namespace").and_then(JsonValue::as_str);
    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let serde_json::Value::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none() {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if !name.contains('.') => {
                    Cow::Owned(format!("{}.{}", namespace, name))
                }
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&*n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue;
        }

        if k == "index" {
            if pcf_is_indexed(v) {
                fields.push((k, format!("{}:{}", pcf_string(k), pcf_string("true"))));
            }
            continue;
        }

        // For anything else, recursively process the result.
        fields.push((
            k,
            format!("{}:{}", pcf_string(k), parsing_canonical_form(v)),
        ));
    }

    // Sort the fields by their canonical ordering ([ORDER] rule).
    fields.sort_unstable_by_key(|(k, _)| field_ordering_position(k).unwrap());
    let inter = fields
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", inter)
}

fn pcf_is_indexed(value: &JsonValue) -> bool {
    if let serde_json::Value::Bool(index) = value {
        if *index {
            return true;
        }
    }
    false
}

fn pcf_array(arr: &[serde_json::Value]) -> String {
    let inter = arr
        .iter()
        .map(parsing_canonical_form)
        .collect::<Vec<String>>()
        .join(",");
    format!("[{}]", inter)
}

fn pcf_string(s: &str) -> String {
    format!("\"{}\"", s)
}

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    let v = match field {
        "name" => 1,
        "type" => 2,
        "fields" => 3,
        "symbols" => 4,
        "items" => 5,
        "values" => 6,
        "size" => 7,
        "value" => 8,
        "index" => 9,
        "condition" => 10,
        _ => return None,
    };

    Some(v)
}

#[cfg(test)]
mod tests {
    extern crate md5;
    extern crate sha2;

    use super::*;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() {
        assert_eq!(Schema::Null, Schema::parse_str("\"null\"").unwrap());
        assert_eq!(Schema::Int, Schema::parse_str("\"int\"").unwrap());
        assert_eq!(Schema::Double, Schema::parse_str("\"double\"").unwrap());
    }

    #[test]
    fn test_array_schema() {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#).unwrap();
        assert_eq!(Schema::Array(Box::new(Schema::String)), schema);
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(Schema::Map(Box::new(Schema::Double)), schema);
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
            schema
        );
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        assert_eq!(SchemaKind::from(&schema), SchemaKind::Union);
        let union_schema = match schema {
            Schema::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);
        let mut variants = union_schema.variants().iter();
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Null);
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Int);
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Bytes
        );
        assert_eq!(variants.next(), None);
    }

    #[test]
    fn test_record_schema() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = Schema::Record {
            name: Name::new("test"),
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(JsonValue::Number(42i64.into())),
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    index: None,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    index: None,
                },
            ],
            lookup,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_enum_schema() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        ).unwrap();

        let expected = Schema::Enum {
            name: Name::new("Suit"),
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_fixed_schema() {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#).unwrap();

        let expected = Schema::Fixed {
            name: Name::new("test"),
            size: 16usize,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_no_documentation() {
        let schema =
            Schema::parse_str(r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#)
                .unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => return assert!(false),
        };

        assert!(doc.is_none());
    }

    #[test]
    fn test_documentation() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#
        ).unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = Schema::Null;
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = Schema::Null;
        sync(&schema);
        sync(schema);
    }

    #[test]
    fn test_schema_fingerprint() {
        use self::md5::Md5;
        use self::sha2::Sha256;

        let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
"#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        assert_eq!(
            "c4d97949770866dec733ae7afa3046757e901d0cfea32eb92a8faeadcc4de153",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "7bce8188f28e66480a45ffbdc3615b7d",
            format!("{}", schema.fingerprint::<Md5>())
        );
    }
}
