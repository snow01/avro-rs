use avro_rs::Schema;

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


#[test]
fn test_canonical_optional() {
    let schema = Schema::parse_str(OPTIONAL_SCHEMA).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form => {}",canonical_form);

}

#[test]
fn test_canonical_custom_type() {
    let schema = Schema::parse_str(CUSTOM_TYPE_SCHEMA).unwrap();
    let canonical_form = schema.canonical_form();
    println!("canonical_form => {}",canonical_form);

}