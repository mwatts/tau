use std::sync::Arc;

use async_trait::async_trait;

use crate::db::{OptimizationRow, PromptMetricRow};
use crate::server::bg_tasks::{BgJob, BgTaskScheduler, BgTrigger};
use crate::server::state::{SharedState, lock_state};

pub const OPTIMIZATION_THRESHOLD: i64 = 10;

pub(crate) struct PromptOptimizerJob;

#[async_trait]
impl BgJob for PromptOptimizerJob {
    fn name(&self) -> &'static str {
        "prompt-optimizer"
    }

    async fn run(&self, state: &SharedState) {
        if let Err(e) = run_optimizer_tick(state).await {
            tracing::warn!(%e, "prompt-optimizer tick error");
        }
    }
}

/// Register the optimizer as a periodic BgJob (checks every 5 minutes).
pub(crate) async fn register(sched: &Arc<BgTaskScheduler>) {
    sched
        .register(
            BgTrigger::Periodic {
                delay: std::time::Duration::from_secs(120),
                interval: std::time::Duration::from_secs(300),
            },
            Arc::new(PromptOptimizerJob),
        )
        .await;
}

async fn run_optimizer_tick(state: &SharedState) -> crate::Result<()> {
    let projects_to_optimize = {
        let st = lock_state(state);
        let projects = st.db.list_projects()?;
        let mut ready = Vec::new();
        for p in projects {
            let count = st.db.count_prompt_metrics(&p.name)?;
            if count >= OPTIMIZATION_THRESHOLD {
                ready.push(p.name);
            }
        }
        ready
    };

    for project in projects_to_optimize {
        tracing::info!(project = %project, "prompt-optimizer: running for project");
        // Full LLM integration will be added in Task 7
        let _metrics = {
            let st = lock_state(state);
            st.db.get_prompt_metrics(&project, OPTIMIZATION_THRESHOLD as usize)?
        };
    }

    Ok(())
}

/// Build the optimizer prompt context from collected metrics.
pub fn build_optimizer_context(
    metrics: &[PromptMetricRow],
    tool_guidelines: &[(String, Vec<String>)],
    auto_skills: &[(String, String)],
) -> String {
    let total = metrics.len();
    let successes = metrics
        .iter()
        .filter(|m| m.outcome == "merged" || m.outcome == "completed")
        .count();
    let failures = metrics
        .iter()
        .filter(|m| m.outcome == "failed" || m.outcome == "closed")
        .count();
    let success_rate = if total > 0 {
        successes as f64 / total as f64 * 100.0
    } else {
        0.0
    };

    let total_tool_errors: i64 = metrics.iter().map(|m| m.tool_error_count).sum();
    let total_tool_calls: i64 = metrics.iter().map(|m| m.tool_call_count).sum();
    let tool_error_rate = if total_tool_calls > 0 {
        total_tool_errors as f64 / total_tool_calls as f64 * 100.0
    } else {
        0.0
    };

    let mut ctx = String::new();
    ctx.push_str("You are optimizing an AI coding agent's prompts based on performance data.\n\n");

    ctx.push_str(&format!(
        "## Aggregated Stats (last {} tasks)\n- success_rate: {:.0}%\n- tool_error_rate: {:.1}%\n- successes: {}\n- failures: {}\n\n",
        total, success_rate, tool_error_rate, successes, failures
    ));

    ctx.push_str("## Current Tool Guidelines\n");
    for (name, guidelines) in tool_guidelines {
        ctx.push_str(&format!("[{}]\n", name));
        for g in guidelines {
            ctx.push_str(&format!("- {}\n", g));
        }
        ctx.push_str("\n");
    }

    if !auto_skills.is_empty() {
        ctx.push_str("## Active Auto-Skills\n");
        for (name, content) in auto_skills {
            ctx.push_str(&format!(
                "### {}\n{}\n\n",
                name,
                crate::truncate_str(content, 300)
            ));
        }
    }

    ctx.push_str("## Per-Task Assessments\n");
    for m in metrics.iter().take(10) {
        ctx.push_str(&format!(
            "- session={} outcome={} errors={}/{} score={} notes={}\n",
            m.session_id,
            m.outcome,
            m.tool_error_count,
            m.tool_call_count,
            m.effectiveness_score
                .map(|s| s.to_string())
                .unwrap_or_else(|| "n/a".into()),
            m.inefficiency_notes.as_deref().unwrap_or("none"),
        ));
    }

    ctx.push_str("\n## Instructions\n");
    ctx.push_str("Analyze the data and propose prompt improvements. For each proposal, output JSON:\n");
    ctx.push_str(r#"[{"target": "tool_guideline|skill|system_prompt", "target_name": "<name>", "risk": "low|medium|high", "action": "replace|patch", "content": "<new content>", "reasoning": "<why>"}]"#);
    ctx.push_str("\n\nOnly propose changes that address observed failures or inefficiencies. If everything looks good, respond with an empty array: []\n");

    ctx
}

/// Parsed optimizer proposal from LLM response.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct OptimizationProposal {
    pub target: String,
    pub target_name: String,
    pub risk: String,
    pub action: String,
    pub content: String,
    pub reasoning: String,
}

/// Parse the LLM's JSON array response into proposals.
pub fn parse_proposals(response: &str) -> Vec<OptimizationProposal> {
    let json_str = if let Some(start) = response.find('[') {
        if let Some(end) = response.rfind(']') {
            &response[start..=end]
        } else {
            response
        }
    } else {
        response
    };
    serde_json::from_str(json_str).unwrap_or_default()
}

/// Check if active optimizations have regressed based on new metrics.
/// Returns list of optimization IDs to revert with reason.
pub fn check_for_regressions(
    active_optimizations: &[OptimizationRow],
    metrics_before: &[PromptMetricRow],
    metrics_after: &[PromptMetricRow],
) -> Vec<(i64, String)> {
    let success_rate = |m: &[PromptMetricRow]| -> f64 {
        let total = m.len();
        if total == 0 {
            return 0.0;
        }
        let s = m
            .iter()
            .filter(|r| r.outcome == "merged" || r.outcome == "completed")
            .count();
        s as f64 / total as f64
    };
    let tool_error_rate = |m: &[PromptMetricRow]| -> f64 {
        let total_calls: i64 = m.iter().map(|r| r.tool_call_count).sum();
        if total_calls == 0 {
            return 0.0;
        }
        let total_errors: i64 = m.iter().map(|r| r.tool_error_count).sum();
        total_errors as f64 / total_calls as f64
    };

    let before_sr = success_rate(metrics_before);
    let after_sr = success_rate(metrics_after);
    let before_ter = tool_error_rate(metrics_before);
    let after_ter = tool_error_rate(metrics_after);

    let mut to_revert = Vec::new();

    let sr_dropped = before_sr - after_sr > 0.20;
    let ter_increased = after_ter - before_ter > 0.30;

    if sr_dropped || ter_increased {
        let reason = format!(
            "regression: success_rate {:.0}%→{:.0}%, tool_error_rate {:.1}%→{:.1}%",
            before_sr * 100.0,
            after_sr * 100.0,
            before_ter * 100.0,
            after_ter * 100.0
        );
        for opt in active_optimizations {
            to_revert.push((opt.id, reason.clone()));
        }
    }

    to_revert
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn build_optimizer_context_includes_metrics_and_stats() {
        let db = Db::open_memory().unwrap();
        for (i, (outcome, errors)) in [("merged", 0i64), ("merged", 1), ("failed", 3)]
            .iter()
            .enumerate()
        {
            db.insert_prompt_metric(
                "proj",
                &format!("s-{}", i),
                None,
                outcome,
                "hash1",
                *errors,
                5,
                Some(r#"{"bash":1}"#),
                None,
                Some(4),
                Some("notes"),
                Some("suggestions"),
                None,
            )
            .unwrap();
        }

        let metrics = db.get_prompt_metrics("proj", 10).unwrap();
        let ctx = build_optimizer_context(&metrics, &[], &[]);
        assert!(ctx.contains("success_rate"));
        assert!(ctx.contains("merged"));
        assert!(ctx.contains("failed"));
    }

    #[test]
    fn parse_proposals_from_llm_response() {
        let response = r#"```json
[{"target": "tool_guideline", "target_name": "bash", "risk": "low", "action": "replace", "content": "Always use set -e", "reasoning": "reduces silent failures"}]
```"#;
        let proposals = parse_proposals(response);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].target, "tool_guideline");
        assert_eq!(proposals[0].target_name, "bash");
    }

    #[test]
    fn check_regressions_triggers_on_success_rate_drop() {
        let before = vec![PromptMetricRow {
            id: 1,
            project_name: "p".into(),
            session_id: "s1".into(),
            task_id: None,
            outcome: "merged".into(),
            prompt_hash: "h".into(),
            tool_error_count: 0,
            tool_call_count: 5,
            tool_errors_by_name: None,
            active_skills: None,
            effectiveness_score: None,
            inefficiency_notes: None,
            prompt_suggestions: None,
            optimization_id: None,
            created_at: 0,
        }];
        let after = vec![PromptMetricRow {
            id: 2,
            project_name: "p".into(),
            session_id: "s2".into(),
            task_id: None,
            outcome: "failed".into(),
            prompt_hash: "h".into(),
            tool_error_count: 4,
            tool_call_count: 5,
            tool_errors_by_name: None,
            active_skills: None,
            effectiveness_score: None,
            inefficiency_notes: None,
            prompt_suggestions: None,
            optimization_id: None,
            created_at: 0,
        }];
        let active = vec![OptimizationRow {
            id: 1,
            project_name: "p".into(),
            target: "tool_guideline".into(),
            target_name: "bash".into(),
            old_hash: "o".into(),
            new_hash: "n".into(),
            risk: "low".into(),
            status: "active".into(),
            reverted_reason: None,
            applied_at: 0,
            reverted_at: None,
        }];
        let reverts = check_for_regressions(&active, &before, &after);
        assert_eq!(reverts.len(), 1);
        assert!(reverts[0].1.contains("regression"));
    }
}
