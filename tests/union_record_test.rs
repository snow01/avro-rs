extern crate avro_rs;
#[macro_use]
extern crate serde_json;

use avro_rs::types::{Record, ToAvro, Value as AvroValue, Value};
use avro_rs::{from_avro_datum, Codec, Reader, Schema, Writer};
use avro_rs::{read_file, write_file};
use serde_json::value::Value as JsonValue;
use std::io::Cursor;

///  cargo test --test union_record_test

const FULL_WRITE_SCHEMA_BIG_SCHEMA: &str = r#"
{"cf":"post_detailed_asset","name":"post_detailed_asset","doc":"Post Details Assets details","index":true,"domain":"user","type":"record","fields":[{"name":"html_chunk_2","type":"optional","value":"string","index":false},{"name":"embedded_assets","type":"optional","index":false,"value":{"type":"array","items":[{"type":"record","name":"image_asset","fields":[{"name":"_type","type":"string"},{"name":"id","type":"optional","value":"string","index":false},{"name":"height","type":"optional","value":"int","index":false},{"name":"width","type":"optional","value":"int","index":false},{"name":"format","type":"optional","value":"string","index":false},{"name":"url","type":"optional","value":"string","index":false}]},{"type":"record","name":"video_asset","fields":[{"name":"_type","type":"string"},{"name":"id","type":"string"},{"name":"video_type","type":"string"},{"name":"video_duration_in_secs","type":"optional","value":"int"},{"name":"https_enabled","type":"optional","value":"boolean"},{"name":"autoplay_enabled","type":"optional","value":"boolean"},{"name":"url","type":"string"},{"name":"src_video_id","type":"optional","value":"string"},{"name":"max_quality","type":"optional","value":"int"},{"name":"max_quality_index","type":"optional","value":"int"},{"name":"live_stream","type":"optional","value":"boolean"},{"name":"apply_pre_buffer_setting","type":"optional","value":"boolean"},{"name":"video_source","type":"optional","value":{"type":"record","name":"video_source","fields":[{"name":"key","type":"string"},{"name":"name","type":"string"},{"name":"add_client_info","type":"boolean"}]}}]},{"type":"record","name":"gallary_asset","fields":[{"name":"_type","type":"string"},{"name":"gallary","type":"array","items":{"name":"photo_gallery_info","type":"record","fields":[{"name":"id","type":"string"},{"name":"share_url","type":"optional","value":"string"},{"name":"type","type":"optional","value":"string"},{"name":"has_thumbnail","type":"optional","value":"string"},{"name":"photo_url","type":"optional","value":"string"},{"name":"description","type":"optional","value":"string"},{"name":"caption","type":"optional","value":"string"}]}}]}]}}]}"#;

const COMPLEX_SCHEMA: &str = r#"
{"cf":"post_detailed_asset","name":"post_detailed_asset","doc":"Post Details Assets details","index":true,"domain":"user","type":"record","fields":[{"name":"html_chunk_2","type":"optional","value":"string","index":false},{"name":"embedded_assets","type":"optional","index":false,"value":{"type":"array","items":[{"type":"record","name":"image_asset","fields":[{"name":"_type","type":"string"},{"name":"id","type":"optional","value":"string","index":false},{"name":"height","type":"optional","value":"int","index":false},{"name":"width","type":"optional","value":"int","index":false},{"name":"format","type":"optional","value":"string","index":false},{"name":"url","type":"optional","value":"string","index":false}]},{"type":"record","name":"video_asset","fields":[{"name":"_type","type":"string"},{"name":"id","type":"string"},{"name":"video_type","type":"string"},{"name":"video_duration_in_secs","type":"optional","value":"int"},{"name":"https_enabled","type":"optional","value":"boolean"},{"name":"autoplay_enabled","type":"optional","value":"boolean"},{"name":"url","type":"string"},{"name":"src_video_id","type":"optional","value":"string"},{"name":"max_quality","type":"optional","value":"int"},{"name":"max_quality_index","type":"optional","value":"int"},{"name":"live_stream","type":"optional","value":"boolean"},{"name":"apply_pre_buffer_setting","type":"optional","value":"boolean"},{"name":"video_source","type":"optional","value":{"type":"record","name":"video_source","fields":[{"name":"key","type":"string"},{"name":"name","type":"string"},{"name":"add_client_info","type":"boolean"}]}}]},{"type":"record","name":"gallary_asset","fields":[{"name":"_type","type":"string"},{"name":"gallary","type":{"type":"array","items":{"name":"photo_gallery_info","type":"record","fields":[{"name":"id","type":"string"},{"name":"share_url","type":"optional","value":"string"},{"name":"type","type":"optional","value":"string"},{"name":"has_thumbnail","type":"optional","value":"string"},{"name":"photo_url","type":"optional","value":"string"},{"name":"description","type":"optional","value":"string"},{"name":"caption","type":"optional","value":"string"}]}}}]}]}}]}"#;

const FULL_WRITE_SCHEMA: &str = r#"
{"name":"new_type","type":"record","index":true,
"fields":[
{"name":"asset_source","type":"string"},
{"name":"asset_type","type":[
    {"name":"video","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_id","type":"int"},{"name":"asset_type","type":"int","index":false}]},
    {"name":"image","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_uid","type":"int"},{"name":"asset_type","type":"int","index":false}]}
    ]}]}
"#;

const VIDEO_WRITE_SCHEMA: &str = r#"{"name":"video","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_id","type":"int"},{"name":"asset_type","type":"int"}]}"#;
const IMAGE_WRITE_SCHEMA: &str = r#"{"name":"image","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_uid","type":"int"},{"name":"asset_type","type":"int"}]}"#;

const FULL_VIDEO_ONLY_SCHEMA: &str = r#"
{"name":"new_type","type":"record",
"fields":[
{"name":"asset_source","type":"string"},
{"name":"asset_type","type":[
        {"name":"video","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_id","type":"int"},{"name":"asset_type","type":"int"}]}]}]}
"#;

const FULL_IMAGE_ONLY_SCHEMA: &str = r#"
{"name":"new_type","type":"record",
"fields":[
{"name":"asset_source","type":"string"},
{"name":"asset_type","type":[
    {"name":"image","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_uid","type":"int"},{"name":"asset_type","type":"int"}]}
    ]}]}
"#;

const PARTIAL_READ_SCHEMA: &str = r#"
{"name":"new_type","type":"record","index":true,
"fields":[
{"name":"asset_source","type":"string"},
{"name":"asset_type","type":[
    {"name":"image","type":"record","fields":[{"name": "_type","type": "string"},{"name":"asset_uid","type":"int"}]}
    ]}]}
"#;

const ASSET_SOURCE: &str = "1234-src";

#[test]
fn test_big_complex_union_schema_parsing() {
    let schema = Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    println!("{:?}", &schema);
    println!("{:?}", serde_json::to_string(&schema).unwrap());
}

#[test]
fn test_big_union_schema_parsing() {
    let schema = Schema::parse_str(FULL_WRITE_SCHEMA_BIG_SCHEMA).unwrap();
    println!("{:?}", &schema);
    println!("{:?}", serde_json::to_string(&schema).unwrap());
}

#[test]
fn test_union_schema_parsing() {
    let schema = Schema::parse_str(FULL_WRITE_SCHEMA);
    assert!(schema.is_ok())
}

#[test]
fn test_write_union_record() {
    let writer_schema = Schema::parse_str(FULL_VIDEO_ONLY_SCHEMA).unwrap();
    let video_schema = Schema::parse_str(VIDEO_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut video_asset = Record::new(&video_schema).unwrap();
    video_asset.put("_type", "video");
    video_asset.put("asset_id", 1);
    video_asset.put("asset_type", 2);

    let video_record = Value::UnionRecord(
        Box::new(Value::Record(video_asset.fields, None)),
        "video".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", video_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    println!("Done data len {:?}", input.len());
}

#[test]
fn test_write_and_read_union_record() {
    let writer_schema = Schema::parse_str(FULL_VIDEO_ONLY_SCHEMA).unwrap();
    let reader_schema = Schema::parse_str(FULL_VIDEO_ONLY_SCHEMA).unwrap();

    let video_schema = Schema::parse_str(VIDEO_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut asset_type = Record::new(&video_schema).unwrap();
    asset_type.put("_type", "video");
    asset_type.put("asset_id", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "video".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
    match reader.next() {
        Some(v) => println!("Value read is {:?}", v),
        None => println!("Got nothing"),
    }

    println!("Done data len {:?}", input.len());
}

#[test]
fn test_incompatible_write_and_read_union_record() {
    let writer_schema = Schema::parse_str(FULL_VIDEO_ONLY_SCHEMA).unwrap();
    let reader_schema = Schema::parse_str(FULL_IMAGE_ONLY_SCHEMA).unwrap();

    let video_schema = Schema::parse_str(VIDEO_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut asset_type = Record::new(&video_schema).unwrap();
    asset_type.put("_type", "video");
    asset_type.put("asset_id", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "video".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
    if let Some(v) = reader.next() {
        assert!(v.is_err());
    } else {
        assert!(false)
    }
}

#[test]
fn test_resolve_write_and_read_union_record() {
    let writer_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let reader_schema = Schema::parse_str(FULL_VIDEO_ONLY_SCHEMA).unwrap();

    let video_schema = Schema::parse_str(VIDEO_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut asset_type = Record::new(&video_schema).unwrap();
    asset_type.put("_type", "video");
    asset_type.put("asset_id", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "video".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
    match reader.next() {
        Some(v) => {
            println!("Value read is {:?}", v);
            assert!(v.is_ok());
        }
        None => assert!(false),
    }
}

#[test]
fn test_resolve_with_partial_schema() {
    let writer_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let reader_schema = Schema::parse_str(PARTIAL_READ_SCHEMA).unwrap();

    let image_schema = Schema::parse_str(IMAGE_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut asset_type = Record::new(&image_schema).unwrap();
    asset_type.put("_type", "image");
    asset_type.put("asset_uid", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "image".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();

    let mut reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
    match reader.next() {
        Some(v) => {
            println!("Value read is {:?}", v);
            assert!(v.is_ok());
        }
        None => assert!(false),
    }
}

#[test]
fn test_resolve_json_map() {
    let reader_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let json_val: JsonValue = json!(
        {
          "asset_source": "12345-src",
          "asset_type": {
            "_type": "video",
            "asset_id": 101,
            "asset_type": 102
          }
        }
    );

    let avro: AvroValue = json_val.avro();
    let resolved = avro.resolve(&reader_schema);
    println!("Final =========> {:?}", resolved);
    assert!(resolved.is_ok());
}

#[test]
fn test_union_record_avro_to_json() {
    let writer_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let image_schema = Schema::parse_str(IMAGE_WRITE_SCHEMA).unwrap();

    let mut asset_type = Record::new(&image_schema).unwrap();
    asset_type.put("_type", "image");
    asset_type.put("asset_uid", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "image".to_string(),
        None,
    );

    let mut record = Record::new(&writer_schema).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    let val = record.avro().json();
    println!("Json {:?}", serde_json::to_string(&val).unwrap());
}

#[test]
fn read_avro_bytes_by_java() {
    let reader_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let path = "/tmp/union-r.avro";
    match read_file(path) {
        Ok(avro_bytes) => {
            let mut cursor = Cursor::new(avro_bytes);
            let decoded = from_avro_datum(&reader_schema, &mut cursor, Some(&reader_schema))
                .expect("failed to decode value");
            println!("Decoded value {:?}", decoded);
        }
        Err(e) => println!("Error while reading file {:?}", e),
    }
}

#[test]
fn write_avro_bytes_for_java() {
    let path = "/tmp/union-w.avro";
    let writer_schema = Schema::parse_str(FULL_WRITE_SCHEMA).unwrap();
    let video_schema = Schema::parse_str(VIDEO_WRITE_SCHEMA).unwrap();
    let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);

    let mut asset_type = Record::new(&video_schema).unwrap();
    asset_type.put("_type", "video");
    asset_type.put("asset_id", 1);
    asset_type.put("asset_type", 2);
    let union_record = Value::UnionRecord(
        Box::new(Value::Record(asset_type.fields, None)),
        "video".to_string(),
        None,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("asset_source", ASSET_SOURCE);
    record.put("asset_type", union_record);

    writer.append(record).unwrap();
    writer.flush().unwrap();
    let input = writer.into_inner();
    assert!(write_file(path, &input).is_ok());
}


