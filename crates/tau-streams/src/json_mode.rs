//! JSON-mode helpers for durable streams.
//!
//! When a stream is created with `content-type: application/json`, each append
//! is parsed as JSON and arrays are flattened into individual messages (§7.1).

use serde_json::Value;

/// Flattens one level of JSON arrays for batch operations.
///
/// - Object → single message
/// - Array → each element is a separate message
/// - Empty array → error
///
/// # Errors
///
/// Returns `"invalid JSON"` if the bytes are not valid JSON, or
/// `"empty JSON array"` if the array has no elements.
pub fn flatten_json_messages(body: &[u8]) -> std::result::Result<Vec<Value>, &'static str> {
    let value: Value = serde_json::from_slice(body).map_err(|_| "invalid JSON")?;

    match value {
        Value::Array(arr) => {
            if arr.is_empty() {
                Err("empty JSON array")
            } else {
                Ok(arr)
            }
        }
        other => Ok(vec![other]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_array() {
        let msgs = flatten_json_messages(b"[{\"a\":1},{\"b\":2}]").unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn single_object() {
        let msgs = flatten_json_messages(b"{\"a\":1}").unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn rejects_empty_array() {
        assert!(flatten_json_messages(b"[]").is_err());
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(flatten_json_messages(b"not json").is_err());
    }
}
