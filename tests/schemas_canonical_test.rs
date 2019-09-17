use avro_rs::Schema;
use md5::Md5;

///  cargo test --test schemas_canonical_test

pub const OPTIONAL_SCHEMA: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "fields": [
    {
      "name": "main_asset",
      "type": "optional",
      "value": {
        "type": "record",
        "name": "main_asset",
        "fields": [
          {
            "name": "main_asset_type",
            "type": "string"
          }
        ]
      }
    }
  ]
}
"#;

const CUSTOM_TYPE_SCHEMA: &str = r#"
{
  "cf": "basic_assets",
  "name": "basic_assets",
  "type": "record",
  "fields": [
    {
      "name": "main_asset",
      "type": {
        "type": "record",
        "name": "main_asset",
        "fields": [
          {
            "name": "main_asset_type",
            "type": "max",
            "value": "long"
          }
        ]
      }
    }
  ]
}

"#;

pub const SCHEMA_WITH_INDEX_TRUE: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "fields": [
    {
      "name": "main_asset",
      "type": "string",
      "index": true
    }
  ]
}
"#;

pub const SCHEMA_WITH_INDEX_FALSE: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "fields": [
    {
      "name": "main_asset",
      "type": "string",
      "index": false
    }
  ]
}
"#;

pub const SCHEMA_WITH_NO_INDEX: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "fields": [
    {
      "name": "main_asset",
      "type": "string"
    }
  ]
}
"#;

pub const SCHEMA_WITH_TOP_LEVEL_INDEX_TRUE: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "index": true,
  "fields": [
    {
      "name": "main_asset",
      "type": "string",
      "index": true
    }
  ]
}
"#;

pub const SCHEMA_WITH_TOP_LEVEL_INDEX_FALSE: &str = r#"
{
  "name": "basic_assets",
  "type": "record",
  "index": false,
  "fields": [
    {
      "name": "main_asset",
      "type": "string",
      "index": true
    }
  ]
}
"#;

#[test]
fn test_canonical_optional() {
    let schema = Schema::parse_str(OPTIONAL_SCHEMA).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form => {}", canonical_form);
}

#[test]
fn test_canonical_custom_type() {
    let schema = Schema::parse_str(CUSTOM_TYPE_SCHEMA).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form => {}", canonical_form);
}

fn get_finger_print(schema: &str) -> String {
    let schema = Schema::parse_str(schema).unwrap();
    format!("{}",schema.fingerprint::<Md5>())
}

#[test]
fn test_fingerprint_for_index_true_vs_false() {
    let fp_index_false = get_finger_print(SCHEMA_WITH_INDEX_FALSE);
    let fp_index_true = get_finger_print(SCHEMA_WITH_INDEX_TRUE);

    assert_ne!(fp_index_false,fp_index_true,"Fingerprint must be different")
}

#[test]
fn test_fingerprint_for_index_false_vs_field_absent() {
    let fp_index_false = get_finger_print(SCHEMA_WITH_INDEX_FALSE);
    let fp_with_no_index = get_finger_print(SCHEMA_WITH_NO_INDEX);

    assert_eq!(fp_index_false,fp_with_no_index,"Fingerprint must be same for index false and index absent")
}


#[test]
fn test_fingerprint_for_top_level_index_true() {
    let fp_index_true = get_finger_print(SCHEMA_WITH_TOP_LEVEL_INDEX_TRUE);
    let fp_index_false_for_top_true_for_inner = get_finger_print(SCHEMA_WITH_TOP_LEVEL_INDEX_FALSE);
    let fp_index_false_for_top = get_finger_print(SCHEMA_WITH_INDEX_TRUE);
    assert_ne!(fp_index_true,fp_index_false_for_top_true_for_inner);
    assert_eq!(fp_index_false_for_top,fp_index_false_for_top_true_for_inner);
}

#[test]
fn test_canonical_schema() {
    let schema = Schema::parse_str(SCHEMA_WITH_TOP_LEVEL_INDEX_TRUE).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form index true => {}", canonical_form);

    let schema = Schema::parse_str(SCHEMA_WITH_TOP_LEVEL_INDEX_FALSE).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form index false => {}", canonical_form);
}
