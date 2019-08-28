#[macro_use]
extern crate serde_json;
use avro_rs::{Schema, Writer, Codec, Reader};
use avro_rs::types::{Record, Value, create_decay_record};
use serde_json::Value as JsonValue ;
use avro_rs::types::ToAvro;


const SIMPLE_SCHEMA: &str = r#"
{"type":"record","name":"scores","fields":[{"name":"h1_relative","type":{"type":"decay","decay_type":"relative","decay_rate":"1 hour","fields":["click","view"]}}]}
"#;

const MULTI_FIELD_SCHEMA : &str = r#"
{"type":"record","name":"scores","cf":"decay","fields":[{"name":"h1_relative","type":{"type":"decay","decay_type":"relative","decay_rate":"1 hour","fields":["click","view"]}},{"name":"h4_relative","type":{"type":"decay","decay_type":"relative","decay_rate":"4 hour","fields":["click","view"]}}]}

"#;

const DECAY_SCHEMA: &str = r#"
  {
      "name": "h1_relative",
      "type": {
        "type": "decay",
        "decay_type": "relative",
        "decay_rate": "1 hour",
        "fields": [
          "click",
          "view"
        ]
      }
    }
"#;


#[test]
fn test_simple_decay_schema() {
    let schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    println!("Schema is {:?}", &schema);
}

#[test]
fn test_simple_decay_schema2() {
    let schema = Schema::parse_str(DECAY_SCHEMA).unwrap();
    println!("Schema is {:?}", &schema);
}

#[test]
fn test_simple_decay_multi_fields_schema() {
    let schema = Schema::parse_str(MULTI_FIELD_SCHEMA).unwrap();
    println!("Schema is {:?}", &schema);
}


#[test]
fn test_write_decay_record() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut record = Record::new(&writer_schema).unwrap();
    if let Schema::Decay(inner,  decay_meta) = Schema::parse_str(DECAY_SCHEMA).unwrap() {
        let decay_setting = decay_meta.to_decay_settings();
        let mut decay_record = Record::new(&inner).unwrap();
        let ts = Value::Long(chrono::Local::now().timestamp_millis(), None);
        let click = Value::Double(2.2, None);
        let view = Value::Double(5.5, None);
        decay_record.put("timestamp", ts);
        decay_record.put("click", Value::Optional(Some(Box::new(click)),None));
        decay_record.put("view", Value::Optional(Some(Box::new(view)),None));
        record.put("h1_relative", create_decay_record(decay_record,decay_setting));
    }


    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    println!("Done data len {:?}", input.len());
}

#[test]
fn test_write_and_read_decay_record() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut record = Record::new(&writer_schema).unwrap();
    if let Schema::Decay(inner,  decay_meta) = Schema::parse_str(DECAY_SCHEMA).unwrap() {
        let decay_setting = decay_meta.to_decay_settings();
        let mut decay_record = Record::new(&inner).unwrap();
        let ts = Value::Long(chrono::Local::now().timestamp_millis(), None);
        let click = Value::Double(2.2, None);
        let view = Value::Double(5.5, None);
        decay_record.put("timestamp", ts);
        decay_record.put("click", Value::Optional(Some(Box::new(click)),None));
        decay_record.put("view", Value::Optional(Some(Box::new(view)),None));
        record.put("h1_relative", create_decay_record(decay_record,decay_setting));
    }


    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    let mut reader = Reader::with_schema(&writer_schema, &input[..]).unwrap();
    match reader.next() {
        Some(v) => {
            let v = v.unwrap();
            println!("Value read is {:?}", &v);
            println!("Value read  as json is {:?}", serde_json::to_string(&v.json()).unwrap());
        },
        None => println!("Got nothing"),
    }
    println!("Done data len {:?}", input.len());
}

#[test]
fn test_read_map_as_decay_record() {
    let reader_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let json_str = r#"
        {
              "h1_relative": {
                "click": 2.2,
                "timestamp": 1566904664982,
                "view": 5.5
              }
        }
    "#;
    let json_val: JsonValue = serde_json::from_str(json_str).unwrap();
    let avro: Value = json_val.avro();
    let resolved = avro.resolve(&reader_schema);
    assert!(resolved.is_ok());
    println!("Final =========> {:?}", resolved);
    println!("Final Json =========> {:?}", resolved.unwrap().json());
}