use std::collections::HashMap;

use crate::types::*;

pub struct ToolStats {
    pub total_calls: i64,
    pub error_count: i64,
    pub errors_by_name: HashMap<String, i64>,
}

/// Compute tool call statistics from a session's message history.
pub fn compute_tool_stats(messages: &[Message]) -> ToolStats {
    let mut total_calls: i64 = 0;
    let mut error_count: i64 = 0;
    let mut errors_by_name: HashMap<String, i64> = HashMap::new();

    for msg in messages {
        if let Message::ToolResult(tr) = msg {
            total_calls += 1;
            if tr.is_error {
                error_count += 1;
                *errors_by_name.entry(tr.tool_name.clone()).or_insert(0) += 1;
            }
        }
    }

    ToolStats {
        total_calls,
        error_count,
        errors_by_name,
    }
}

/// Compute a SHA256 hash of the system prompt for fingerprinting.
pub fn prompt_hash(system_prompt: Option<&str>) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(system_prompt.unwrap_or("").as_bytes());
    hasher.finalize().iter().fold(String::new(), |mut acc, b| {
        use std::fmt::Write;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

/// Record metrics for a completed session.
pub fn record_session_metrics(
    db: &crate::db::Db,
    project_name: &str,
    session_id: &str,
    task_id: Option<i64>,
    outcome: &str,
    system_prompt: Option<&str>,
    messages: &[Message],
    active_skills: &[String],
) {
    let stats = compute_tool_stats(messages);
    let hash = prompt_hash(system_prompt);
    let errors_json = serde_json::to_string(&stats.errors_by_name).ok();
    let skills_json = serde_json::to_string(active_skills).ok();

    if let Err(e) = db.insert_prompt_metric(
        project_name,
        session_id,
        task_id,
        outcome,
        &hash,
        stats.error_count,
        stats.total_calls,
        errors_json.as_deref(),
        skills_json.as_deref(),
        None, // effectiveness_score — filled later by assessment
        None, // inefficiency_notes
        None, // prompt_suggestions
        None, // optimization_id
    ) {
        tracing::warn!(%e, "failed to record prompt metrics");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_tool_error_stats_from_messages() {
        let messages = vec![
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "tc1".into(),
                tool_name: "bash".into(),
                content: vec![],
                details: None,
                is_error: true,
                timestamp: 0,
                duration_ms: None,
                summary: None,
                post_persist_actions: Vec::new(),
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "tc2".into(),
                tool_name: "bash".into(),
                content: vec![],
                details: None,
                is_error: false,
                timestamp: 0,
                duration_ms: None,
                summary: None,
                post_persist_actions: Vec::new(),
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "tc3".into(),
                tool_name: "edit".into(),
                content: vec![],
                details: None,
                is_error: true,
                timestamp: 0,
                duration_ms: None,
                summary: None,
                post_persist_actions: Vec::new(),
            }),
        ];
        let stats = compute_tool_stats(&messages);
        assert_eq!(stats.total_calls, 3);
        assert_eq!(stats.error_count, 2);
        assert_eq!(stats.errors_by_name["bash"], 1);
        assert_eq!(stats.errors_by_name["edit"], 1);
    }
}
