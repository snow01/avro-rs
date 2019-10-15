extern crate serde_json;
use avro_rs::encode::encode;
use avro_rs::types::ToAvro;
use avro_rs::types::{create_decay_record, Record, Value};
use avro_rs::{from_avro_datum, Codec, Reader, Schema, Writer};
use avro_rs::{read_file, write_file};
use serde_json::Value as JsonValue;
use std::io::Cursor;

///  cargo test --test decay_record_test

const SIMPLE_SCHEMA: &str = r#"
{"type":"record","cf":"scores","name":"scores","fields":[{"name":"h1_relative","type":{"type":"decay","decay_type":"relative","decay_rates":["1 hours"],"fields":["click"]}}]}
"#;

const MULTI_FIELD_SCHEMA : &str = r#"
{"type":"record","cf":"decay","name":"scores","fields":[{"name":"h1_relative","type":{"type":"decay","decay_type":"relative","decay_rates":["1 hours"],"fields":["click","view"]}},{"name":"h4_relative","type":{"type":"decay","decay_type":"relative","decay_rates":["4 hours"],"fields":["click","view"]}}]}

"#;

const DECAY_SCHEMA: &str = r#"
  {
      "name": "h1_relative",
      "type": {
        "type": "decay",
        "decay_type": "relative",
        "decay_rates": ["1 hours"],
        "fields": [
          "click"
        ]
      }
    }
"#;

#[test]
fn test_simple_decay_schema() {
    let schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    println!("Schema is {:#?}", &schema);
}

#[test]
fn test_simple_decay_schema2() {
    let schema = Schema::parse_str(DECAY_SCHEMA).unwrap();
    println!("Schema is {:#?}", &schema);
}

#[test]
fn test_simple_decay_multi_fields_schema() {
    let schema = Schema::parse_str(MULTI_FIELD_SCHEMA).unwrap();
    let value = serde_json::to_string(&schema).unwrap();
    println!("Schema is {:#?} value {}", &schema, value);
}

//TODO: update test as per new decay schema changes.
//#[test]
//fn test_write_decay_record() {
//    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
//    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
//
//    let mut record = Record::new(&writer_schema).unwrap();
//    if let Schema::Decay(inner, decay_meta) = Schema::parse_str(DECAY_SCHEMA).unwrap() {
//        let decay_setting = decay_meta.to_decay_settings();
//        println!("decay_score {:?}", inner);
//        let mut decay_record = Record::new(&inner).unwrap();
//        let ts = Value::Long(chrono::Local::now().timestamp_millis(), None);
//        let click = Value::Double(2.2, None);
//        let view = Value::Double(5.5, None);
//        decay_record.put("timestamp", ts);
//        decay_record.put("click", Value::Optional(Some(Box::new(click)), None));
//        decay_record.put("view", Value::Optional(Some(Box::new(view)), None));
//        record.put(
//            "h1_relative",
//            create_decay_record(decay_record, decay_setting),
//        );
//    }
//
//    writer.append(record).unwrap();
//    writer.flush().unwrap();
//    let input = writer.into_inner();
//
//    println!("Done data len {:?}", input.len());
//}

//TODO: update test as per new decay schema changes.
//#[test]
//fn test_write_and_read_decay_record() {
//    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
//    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
//
//    let mut record = Record::new(&writer_schema).unwrap();
//    if let Schema::Decay(inner, decay_meta) = Schema::parse_str(DECAY_SCHEMA).unwrap() {
//        let decay_setting = decay_meta.to_decay_settings();
//        let mut decay_record = Record::new(&inner).unwrap();
//        let ts = Value::Long(chrono::Local::now().timestamp_millis(), None);
//        let click = Value::Double(2.2, None);
//        let view = Value::Double(5.5, None);
//        decay_record.put("timestamp", ts);
//        decay_record.put("click", Value::Optional(Some(Box::new(click)), None));
//        decay_record.put("view", Value::Optional(Some(Box::new(view)), None));
//        record.put(
//            "h1_relative",
//            create_decay_record(decay_record, decay_setting),
//        );
//    }
//
//    writer.append(record).unwrap();
//    writer.flush().unwrap();
//    let input = writer.into_inner();
//
//    let mut reader = Reader::with_schema(&writer_schema, &input[..]).unwrap();
//    match reader.next() {
//        Some(v) => {
//            let v = v.unwrap();
//            println!("Value read is {:?}", &v);
//            println!(
//                "Value read  as json is {:?}",
//                serde_json::to_string(&v.json()).unwrap()
//            );
//        }
//        None => println!("Got nothing"),
//    }
//    println!("Done data len {:?}", input.len());
//}

#[test]
fn test_read_map_as_decay_record() {
    let reader_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let decay_schema = Schema::parse_str(DECAY_SCHEMA).unwrap();
    let json_str = r#"
             {
              "h1_relative": {
                "click": {
                  "timestamp": 1566904664982,
                  "count": 2
                }
              }
            }
    "#;
    //println!("reader_schema {:?}", reader_schema);
    let json_val: JsonValue = serde_json::from_str(json_str).unwrap();
    println!("json_val {:?}", json_val);

    let avro: Value = json_val.avro();
    println!(
        "Avro as json {}",
        serde_json::to_string(&avro.json()).unwrap()
    );

    let resolved = avro.resolve(&reader_schema);

    match resolved {
        Ok(value) => {
            println!("value {:?}", value);
            println!(
                "Avro as json {}",
                serde_json::to_string(&value.json()).unwrap()
            );

            let mut writer = Writer::with_codec(&reader_schema, Vec::new(), Codec::Null);
            writer.append(value.clone()).unwrap();
            writer.flush().unwrap();

            let bytes = writer.into_inner();
            println!("len {}", bytes.len());
            let mut reader = Reader::with_schema(&reader_schema, &bytes[..]).unwrap();
            match reader.next() {
                Some(v) => {
                    println!("v {:?}", v);
                }
                None => {}
            }
            let mut buffer = Vec::new();
            encode(&value, &reader_schema, &mut buffer);
            println!("buffer_len {}", buffer.len());

            let mut cursor = Cursor::new(buffer);
            let decoded = from_avro_datum(&reader_schema, &mut cursor, Some(&reader_schema))
                .expect("failed to decode value");
            println!("decoded value {:?}", decoded);
        }
        Err(err) => {
            println!("error ====> {:?}", err);
        }
    }
}

#[test]
fn read_avro_bytes_by_java() {
    let reader_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let path = "/tmp/decay-w.avro";
    match read_file(path) {
        Ok(avro_bytes) => {
            let mut cursor = Cursor::new(avro_bytes);
            let decoded = from_avro_datum(&reader_schema, &mut cursor, Some(&reader_schema))
                .expect("failed to decode value");
            println!("Decoded value {:?}", decoded);
            println!(
                "Decoded json value {:?}",
                serde_json::to_string(&decoded.json()).unwrap()
            );
        }
        Err(e) => println!("Error while reading file {:?}", e),
    }
}

//TODO: update test as per new decay schema changes.
//#[test]
//fn write_avro_bytes_for_java() {
//    let path = "/tmp/decay-r.avro";
//    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
//    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
//
//    let mut record = Record::new(&writer_schema).unwrap();
//    if let Schema::Decay(inner, decay_meta) = Schema::parse_str(DECAY_SCHEMA).unwrap() {
//        let decay_setting = decay_meta.to_decay_settings();
//        let mut decay_record = Record::new(&inner).unwrap();
//        let ts = Value::Long(chrono::Local::now().timestamp_millis(), None);
//        let click = Value::Double(2.2, None);
//        let view = Value::Double(5.5, None);
//        decay_record.put("timestamp", ts);
//        decay_record.put("click", Value::Optional(Some(Box::new(click)), None));
//        decay_record.put("view", Value::Optional(Some(Box::new(view)), None));
//        record.put(
//            "h1_relative",
//            create_decay_record(decay_record, decay_setting),
//        );
//    }
//
//    writer.append(record).unwrap();
//    writer.flush().unwrap();
//    let input = writer.into_inner();
//    assert!(write_file(path, &input).is_ok());
//}
