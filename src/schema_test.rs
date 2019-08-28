#![allow(unused_imports)]
use std::borrow::ToOwned;
#[allow(unused_imports)]
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};

use crate::reader::Reader;
use crate::schema::Schema;
use crate::types;
use crate::types::{LruValue, Record, Value};
use crate::{Codec, Writer};

use super::*;
use time::Duration;

lazy_static! {
    static ref ALL_TYPE_SCHEMA: &'static str = r#"
{
  "name": "UserProfile",
  "doc": "Filterable attributes",
  "index": true,
  "type": "record",
  "fields": [
    {
      "name": "int",
      "type": "int"
    },
    {
      "name": "string",
      "type": "string"
    },
    {
      "name": "enum",
      "type": {
        "type": "enum",
        "name": "status",
        "symbols": [
          "FOLLOW",
          "BLOCK",
          "UNFOLLOW"
        ]
      }
    },
    {
      "name": "array",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "map",
      "type": {
        "type": "map",
        "values": "string"
      }
    },
    {
      "name": "map1",
      "type": "map",
      "values": {
        "type": "enum",
        "name": "status1",
        "symbols": [
          "FOLLOW",
          "BLOCK",
          "UNFOLLOW"
        ]
      }
    },
    {
      "name": "set",
      "type": "set"
    },
    {
      "name": "lruSet",
      "type": "lru_set",
      "limit": "30 days"
    },
    {
      "name": "optionalStr",
      "type": "optional",
      "value": "string"
    },
    {
      "name": "dateExample",
      "type": "date"
    }
  ]
}
    "#;
}

/*#[test]
fn test_write_file_for_all_types() {
    let schema = Schema::parse_str(&ALL_TYPE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
    let mut record = Record::new(writer.schema()).unwrap();

    let arr_type = Value::Array(vec![Value::String("arr1".to_owned(), None)], None);

    let mut map = HashMap::new();
    map.insert("key1", "value");

    let mut map1 = HashMap::new();
    map1.insert("key1", "FOLLOW");

    let mut set = HashSet::new();
    set.insert("hello".to_owned());

    let mut lru_set: HashMap<String, LruValue> = HashMap::new();
    let lru_value = LruValue { access_time: 123455, count: 22 };
    lru_set.insert("ig1".to_owned(), lru_value);

    record.put("int", 123);
    record.put("string", "nrtavro");
    record.put("enum", "FOLLOW");
    record.put("array", arr_type);
    record.put("map", map);
    record.put("map1", map1);
    record.put("set", Value::Set(set, None));
    record.put("lruSet", Value::LruSet(lru_set, LruLimit::Days(Duration::days(30), 30), None));
    record.put("optionalStr", Value::Optional(None, None));


    // write
    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    println!("{}", input.len());

    let mut buffer = File::create("/tmp/data.avro").unwrap();
    buffer.write(&input);
}*/

#[test]
fn test_read_file_for_all_types() {
    // Given
    let schema = Schema::parse_str(&ALL_TYPE_SCHEMA).unwrap();
    let mut file = File::open("/Users/sohi/dailyhunt-git/dh-avro-java-extension/foo.avro").unwrap();
    let buffer_size = 100;

    let mut vec = vec![0u8; buffer_size];
    let count = file.read(&mut vec).unwrap();

    println!("total bytes read from data =  {} bytes.", count);

    let value = from_avro_datum(&schema, &mut vec.as_slice(), None).unwrap();
    println!("read full record = {:?}", value);

    match value {
        Value::Record(values, _) => {
            println!("\n===========  Type  : Value =================\n");
            for schema_type in values.iter() {
                println!("Type : {:?}  Value : {:?}", schema_type.0, schema_type.1)
            }
        }
        _ => println!("Some other type"),
    }
}

#[test]
fn test_write_file_enum_string_value() {
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
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");
    // record.put("c", "clubs");
    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    println!("{}", input.len());

    let mut reader = Reader::with_schema(&schema, &input[..]).unwrap();
    assert_eq!(
        reader.next().unwrap().unwrap(),
        Value::Record(
            vec![
                ("a".to_string(), Value::Long(27, None)),
                ("b".to_string(), Value::String("foo".to_string(), None)),
            ],
            None
        )
    );
    assert!(reader.next().is_none());
}
