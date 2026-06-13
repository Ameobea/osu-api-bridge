use serde::Serialize;

/// Serializes a column holding raw JSON text (as stored in the DB) by embedding it directly into
/// the output rather than as a quoted string. As of sqlx 0.9 these text columns decode to `String`
/// rather than `Vec<u8>`, hence the `Option<String>` input.
pub fn serialize_json_str_opt<S>(
  value: &Option<String>,
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: serde::Serializer,
{
  match value {
    Some(json_str) => {
      let json_value: serde_json::Value = serde_json::from_str(json_str).unwrap();
      json_value.serialize(serializer)
    },
    None => serde_json::Value::Null.serialize(serializer),
  }
}
