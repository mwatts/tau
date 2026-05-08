//! Interval-based cursor generation for CDN collapsing (§8.1).

use std::time::{SystemTime, UNIX_EPOCH};

const INTERVAL_SECS: u64 = 20;
// Epoch: October 9, 2024 00:00:00 UTC
const CURSOR_EPOCH: u64 = 1_728_432_000;

/// Generates a cursor value based on the current time interval.
///
/// If `client_cursor` is provided and >= current interval,
/// adds jitter to guarantee monotonic progression.
pub fn generate_cursor(client_cursor: Option<u64>) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let interval = now.saturating_sub(CURSOR_EPOCH) / INTERVAL_SECS;

    let cursor = match client_cursor {
        Some(c) if c >= interval => {
            let jitter = (now % 3600) + 1;
            c + jitter
        }
        _ => interval,
    };

    cursor.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_returns_string() {
        let c = generate_cursor(None);
        assert!(!c.is_empty());
        assert!(c.parse::<u64>().is_ok());
    }

    #[test]
    fn cursor_advances_past_client() {
        let large = 999_999_999u64;
        let c = generate_cursor(Some(large));
        let n: u64 = c.parse().unwrap();
        assert!(n > large);
    }

    #[test]
    fn cursor_without_client_is_deterministic_in_same_interval() {
        let c1 = generate_cursor(None);
        let c2 = generate_cursor(None);
        assert_eq!(c1, c2);
    }
}
