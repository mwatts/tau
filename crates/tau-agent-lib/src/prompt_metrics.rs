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

/// Build the LLM assessment prompt from session context.
pub fn build_assessment_prompt(
    messages: &[Message],
    active_guidelines: &[String],
    outcome: &str,
) -> String {
    let mut prompt = String::new();
    prompt.push_str("You are evaluating the effectiveness of an AI agent's prompt configuration.\n\n");
    prompt.push_str(&format!("Task outcome: {}\n\n", outcome));

    prompt.push_str("Active guidelines:\n");
    for g in active_guidelines {
        prompt.push_str(&format!("- {}\n", g));
    }
    prompt.push_str("\nLast messages from the session:\n");

    // Include last 5 messages (truncated)
    let tail = if messages.len() > 5 { &messages[messages.len() - 5..] } else { messages };
    for msg in tail {
        match msg {
            Message::User(u) => {
                let text: String = u.content.iter().filter_map(|c| match c {
                    UserContent::Text(t) => Some(t.text.as_str()),
                    _ => None,
                }).collect::<Vec<_>>().join(" ");
                prompt.push_str(&format!("[User]: {}\n", crate::truncate_str(&text, 200)));
            }
            Message::Assistant(a) => {
                let text: String = a.content.iter().filter_map(|c| match c {
                    AssistantContent::Text(t) => Some(t.text.as_str()),
                    _ => None,
                }).collect::<Vec<_>>().join(" ");
                prompt.push_str(&format!("[Assistant]: {}\n", crate::truncate_str(&text, 200)));
            }
            Message::ToolResult(tr) => {
                let err_marker = if tr.is_error { " [ERROR]" } else { "" };
                prompt.push_str(&format!("[Tool:{}{}]\n", tr.tool_name, err_marker));
            }
            _ => {}
        }
    }

    prompt.push_str("\nRespond with JSON only:\n");
    prompt.push_str(r#"{"effectiveness_score": <1-5>, "inefficiency_notes": "<text>", "prompt_suggestions": "<text>"}"#);
    prompt.push_str("\n");
    prompt
}

/// Parsed assessment response from the LLM.
#[derive(Debug, serde::Deserialize)]
pub struct Assessment {
    pub effectiveness_score: i64,
    pub inefficiency_notes: String,
    pub prompt_suggestions: String,
}

/// Parse the LLM's JSON response into an Assessment.
pub fn parse_assessment(response: &str) -> Option<Assessment> {
    // Try to extract JSON from the response (LLM might wrap in markdown)
    let json_str = if let Some(start) = response.find('{') {
        if let Some(end) = response.rfind('}') {
            &response[start..=end]
        } else {
            response
        }
    } else {
        response
    };
    serde_json::from_str(json_str).ok()
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

    #[test]
    fn build_assessment_prompt_includes_messages_and_guidelines() {
        let messages = vec![
            Message::User(UserMessage::text("fix the auth bug")),
            Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::Text(TextContent { text: "I'll look at auth.rs".into(), text_signature: None })],
                api: "test".into(),
                provider: "test".into(),
                model: "test".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: 0,
            }),
        ];
        let guidelines = vec!["Always run tests after changes".to_string()];
        let prompt = build_assessment_prompt(&messages, &guidelines, "merged");
        assert!(prompt.contains("fix the auth bug"));
        assert!(prompt.contains("Always run tests after changes"));
        assert!(prompt.contains("merged"));
    }

    #[test]
    fn parse_assessment_from_json() {
        let response = r#"```json
{"effectiveness_score": 4, "inefficiency_notes": "took extra steps", "prompt_suggestions": "add hint"}
```"#;
        let assessment = parse_assessment(response).unwrap();
        assert_eq!(assessment.effectiveness_score, 4);
        assert_eq!(assessment.inefficiency_notes, "took extra steps");
    }
}
