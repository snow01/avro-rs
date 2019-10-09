use avro_rs::{Schema, Writer, Codec, Reader};
use avro_rs::types::{Record, Value as AvroValue, ToAvro, Value, ValueComparison};
use serde_json::value::Value as JsonValue;

///  cargo test --test value_comparator_test

const SIMPLE_SCHEMA: &str = r#"

{
  "name": "Test",
  "type": "record",
  "index": true,
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "num_days",
      "type": "value_comparator",
      "condition": "GT",
      "value": {
        "name": "num_days_max",
        "type": "max",
        "value":  {
          "type": "long"
        }

      }
    }
  ]
}

"#;

const VALUE_COMPARATOR_SCHEMA: &str = r#"
 {
      "name": "num_days",
      "type": "value_comparator",
      "condition": "GT",
      "value": {
        "name": "num_days_max",
        "type": "max",
        "value":  {
          "type": "long",
          "index": "false"
        }

      }
    }
"#;


#[test]
fn parse_date_check_schema(){
    let schema = Schema::parse_str(SIMPLE_SCHEMA);
    assert!(schema.is_ok());
    println!("Schema is {:?}",schema.unwrap());
}

#[test]
fn test_schema_validate() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let date_schema = Schema::parse_str(VALUE_COMPARATOR_SCHEMA).expect("valid date schema is expected");

    let max = AvroValue::Max(Box::new(AvroValue::Long(2,None)), None);

    if let Schema::ValueComparator(ref inner, _) = date_schema {
        let mut date_check_record = Record::new(inner).unwrap();
        date_check_record.put("_value", 1569888000 as i64);
        date_check_record.put("data", max);
        let date_check_record = date_check_record.avro();
        let date_check_record = Value::ValueComparator(Box::new(date_check_record), ValueComparison::GT, None);

        let mut record = Record::new(&writer_schema).unwrap();
        record.put("name", AvroValue::String("num_days_field".to_string(),None));
        record.put("num_days", date_check_record);

        let record = record.avro();

        let is_valid = record.validate(&writer_schema);
        assert!(is_valid,"record is not valid with schema");
    }else {
        assert!(false,"schema is not date check schema");
    }
}

#[test]
fn test_schema_resolve() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let date_schema = Schema::parse_str(VALUE_COMPARATOR_SCHEMA).expect("valid date schema is expected");

    let max = AvroValue::Max(Box::new(AvroValue::Long(2,None)), None);

    if let Schema::ValueComparator(ref inner, _) = date_schema {
        let mut date_check_record = Record::new(inner).unwrap();
        date_check_record.put("_value", 1569888000 as i64);
        date_check_record.put("data", max);
        let date_check_record = date_check_record.avro();
        let date_check_record = Value::ValueComparator(Box::new(date_check_record), ValueComparison::GT, None);

        let mut record = Record::new(&writer_schema).unwrap();
        record.put("name", AvroValue::String("obelix".to_string(),None));
        record.put("num_days", date_check_record);

        let record = record.avro();

        let resolved = record.resolve(&writer_schema);
        assert!(resolved.is_ok(),"record could not be resolved with schema");
        println!("Resolved value is \n{:?}",resolved.unwrap());
    }else {
        assert!(false,"schema is not date check schema");
    }
}

#[test]
fn test_schema_write_read() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let date_schema = Schema::parse_str(VALUE_COMPARATOR_SCHEMA).expect("valid date schema is expected");

    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
    let max = AvroValue::Max(Box::new(AvroValue::Long(2,None)), None);

    if let Schema::ValueComparator(ref inner, _) = date_schema {
        let mut date_check_record = Record::new(inner).unwrap();
        date_check_record.put("_value", 1569888000 as i64);
        date_check_record.put("data", max);
        let date_check_record = date_check_record.avro();
        let date_check_record = Value::ValueComparator(Box::new(date_check_record), ValueComparison::GT, None);

        let mut record = Record::new(&writer_schema).unwrap();
        record.put("name", AvroValue::String("obelix".to_string(),None));
        record.put("num_days", date_check_record);

        let record = record.avro();

        record.validate(&writer_schema);
        let resolved = record.resolve(&writer_schema);
        assert!(resolved.is_ok(),"record could not be resolved with schema");

        let record = resolved.unwrap();
        println!("json {:?}",serde_json::to_string(&record.clone().json()).unwrap());

        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();

        let mut reader = Reader::with_schema(&writer_schema, &input[..]).unwrap();
        match reader.next() {
            Some(v) => println!("Value read is {:?}", v),
            None => println!("Got nothing"),
        }
    }else {
        assert!(false,"schema is not date check schema");
    }
}


#[test]
fn test_read_map_as_value_comparator() {
    let reader_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let json_str = r#"
        {
          "name": "obelix",
          "num_days": {
            "_value": 1569888000,
            "data": 2
          }
        }
    "#;
    let json_val: JsonValue = serde_json::from_str(json_str).unwrap();
    let avro: Value = json_val.avro();
    let resolved = avro.resolve(&reader_schema);
    assert!(resolved.is_ok());
    println!("Final Avro =========> {:?}", resolved);
    println!("Final Json =========> {:?}", resolved.unwrap().json());
}


#[test]
fn test_string_schema(){
    let schema_str = r#"
{
  "cf": "t",
  "name": "t",
  "table": "user",
  "doc": "Basic info",
  "type": "record",
  "index": true,
  "fields": [
    {
      "name": "platform",
      "type": "value_comparator",
      "condition": "GT",
      "value": {
        "name": "inner_record",
        "type": "record",
        "fields": [
          {
            "name": "platform2",
            "type": "string"
          }
        ]
      }
    }
  ]
}
    "#;

    let schema = Schema::parse_str(schema_str).unwrap();
    println!("Schema {:?}",schema);




}

/*
#[test]
fn test_write_date_condition_record() {
    let writer_schema = Schema::parse_str(SIMPLE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let max = AvroValue::Max(Box::new(AvroValue::Long(2,None)), None);

    let mut record = Record::new(&writer_schema).unwrap();
    record.put("_time", 1569888000);
    record.put("value", max);

    let record = record.avro();
    record.validate(&writer_schema);



    *//*
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();

        println!("Done data len {:?}", input.len());
    *//*

    *//*    let mut reader = Reader::with_schema(&writer_schema, &input[..]).unwrap();
        match reader.next() {
            Some(v) => println!("Value read is {:?}", v),
            None => println!("Got nothing"),
        }*//*
}*/
