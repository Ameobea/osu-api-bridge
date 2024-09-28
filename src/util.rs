use serde::Serialize;

pub fn serialize_json_bytes_opt<S>(
  value: &Option<Vec<u8>>,
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: serde::Serializer,
{
  match value {
    Some(bytes) => {
      let json_str = std::str::from_utf8(bytes).unwrap();
      let json_value: serde_json::Value = serde_json::from_str(json_str).unwrap();
      json_value.serialize(serializer)
    },
    None => serde_json::Value::Null.serialize(serializer),
  }
}
