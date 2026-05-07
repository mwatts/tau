//! Monotonic offset generation for durable streams.
//!
//! [`OffsetGenerator`] produces offsets of the form `{timestamp_micros:016}_{seq:04}`.
//! The timestamp component ensures global ordering across restarts; the sequence
//! component ensures uniqueness within a single microsecond.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::Offset;

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct State {
    last_ts: u64,
    last_seq: u16,
}

// ---------------------------------------------------------------------------
// OffsetGenerator
// ---------------------------------------------------------------------------

/// Generates monotonically increasing, fixed-width offsets.
///
/// Offsets have the format `{timestamp_micros:016}_{seq:04}`.  When multiple
/// offsets are generated within the same microsecond the sequence counter is
/// incremented to guarantee strict ordering.
#[derive(Debug, Default)]
pub struct OffsetGenerator {
    state: Mutex<State>,
}

impl OffsetGenerator {
    /// Creates a new generator initialised to the current wall clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Generates the next monotonically increasing offset.
    ///
    /// This method is safe to call from multiple threads simultaneously.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex has been poisoned (i.e. a previous call
    /// panicked while holding the lock).
    pub fn next(&self) -> Offset {
        let now_micros = current_micros();
        let mut state = self.state.lock().expect("mutex poisoned");

        let (ts, seq) = if now_micros > state.last_ts {
            // Clock advanced — reset sequence.
            state.last_ts = now_micros;
            state.last_seq = 0;
            (now_micros, 0u16)
        } else {
            // Same or regressed clock — advance sequence on the last timestamp.
            state.last_seq = state.last_seq.saturating_add(1);
            (state.last_ts, state.last_seq)
        };

        Offset(format!("{ts:016}_{seq:04}"))
    }
}

/// Returns microseconds since the Unix epoch.
fn current_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_format() {
        let offset_gen = OffsetGenerator::new();
        let off = offset_gen.next();
        // Should be at least 16 digits, underscore, 4 digits  → total ≥ 21 chars.
        // The timestamp component uses :016 (min-width 16); current epoch in
        // microseconds is ~16 digits, so length is 21 bytes.
        let s = &off.0;
        assert!(s.len() >= 21, "unexpected offset length: {s}");
        let parts: Vec<&str> = s.splitn(2, '_').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 16);
        assert_eq!(parts[1].len(), 4);
        assert!(parts[0].chars().all(|c| c.is_ascii_digit()));
        assert!(parts[1].chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn monotonic_ordering() {
        let offset_gen = OffsetGenerator::new();
        let offsets: Vec<Offset> = (0..100).map(|_| offset_gen.next()).collect();
        let mut sorted = offsets.clone();
        sorted.sort();
        assert_eq!(offsets, sorted, "offsets are not in monotonic order");
    }

    #[test]
    fn rapid_calls_use_sequence_numbers() {
        // Generate a large batch quickly; some pairs must share a timestamp
        // prefix and differ only in sequence.
        let offset_gen = OffsetGenerator::new();
        let offsets: Vec<Offset> = (0..1000).map(|_| offset_gen.next()).collect();

        // All offsets must be unique.
        let mut unique = offsets.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 1000, "duplicate offsets detected");

        // Check that at least one pair shares a timestamp (sequence was used).
        let has_sequence = offsets
            .windows(2)
            .any(|w| w[0].0[..16] == w[1].0[..16]);
        assert!(
            has_sequence,
            "expected some offsets to share a timestamp prefix"
        );
    }
}
