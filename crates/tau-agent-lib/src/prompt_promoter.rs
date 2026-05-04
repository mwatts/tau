use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::db::OptimizationRow;
use crate::server::bg_tasks::{BgJob, BgTaskScheduler, BgTrigger};
use crate::server::state::{SharedState, lock_state};

pub(crate) struct PromptPromoterJob;

#[async_trait]
impl BgJob for PromptPromoterJob {
    fn name(&self) -> &'static str {
        "prompt-promoter"
    }

    async fn run(&self, state: &SharedState) {
        if let Err(e) = run_promoter_tick(state) {
            tracing::warn!(%e, "prompt-promoter tick error");
        }
    }
}

/// Register promoter — runs every 24 hours (conservative schedule).
pub(crate) async fn register(sched: &Arc<BgTaskScheduler>) {
    sched
        .register(
            BgTrigger::Periodic {
                delay: std::time::Duration::from_secs(3600),
                interval: std::time::Duration::from_secs(24 * 3600),
            },
            Arc::new(PromptPromoterJob),
        )
        .await;
}

/// Find optimizations that appear in N+ projects with the same (target, target_name, new_hash).
pub fn find_promotion_candidates(
    all_active: &[OptimizationRow],
    min_projects: usize,
) -> Vec<OptimizationRow> {
    let mut groups: HashMap<(String, String, String), Vec<&OptimizationRow>> = HashMap::new();
    for opt in all_active {
        let key = (opt.target.clone(), opt.target_name.clone(), opt.new_hash.clone());
        groups.entry(key).or_default().push(opt);
    }

    let mut candidates = Vec::new();
    for (_, opts) in groups {
        let unique_projects: std::collections::HashSet<&str> =
            opts.iter().map(|o| o.project_name.as_str()).collect();
        if unique_projects.len() >= min_projects {
            if let Some(first) = opts.into_iter().next() {
                candidates.push(first.clone());
            }
        }
    }
    candidates
}

fn run_promoter_tick(state: &SharedState) -> crate::Result<()> {
    let all_active = {
        let st = lock_state(state);
        let projects = st.db.list_projects()?;
        let mut all = Vec::new();
        for p in projects {
            all.extend(st.db.get_active_optimizations(&p.name)?);
        }
        all
    };

    if all_active.is_empty() {
        return Ok(());
    }

    let candidates = find_promotion_candidates(&all_active, 2);
    if candidates.is_empty() {
        return Ok(());
    }

    tracing::info!(count = candidates.len(), "prompt-promoter: promoting optimizations");

    for candidate in &candidates {
        match candidate.target.as_str() {
            "tool_guideline" => {
                let global_path = tau_agent_base::paths::config_dir().join("tool_prompts.toml");
                let mut table: toml::Table = std::fs::read_to_string(&global_path)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_default();

                // Read guidelines from source project
                let source_project_path = {
                    let st = lock_state(state);
                    st.db.get_project(&candidate.project_name)
                        .ok()
                        .flatten()
                        .map(|p| p.path)
                };
                if let Some(path) = source_project_path {
                    let overrides = crate::tool_prompt_overrides::load_overrides(Some(&path));
                    if let Some(guidelines) = overrides.get(&candidate.target_name) {
                        let mut tool_table = toml::Table::new();
                        let arr: Vec<toml::Value> = guidelines.iter()
                            .map(|s| toml::Value::String(s.clone()))
                            .collect();
                        tool_table.insert("guidelines".into(), toml::Value::Array(arr));
                        table.insert(candidate.target_name.clone(), toml::Value::Table(tool_table));
                    }
                }

                if let Some(parent) = global_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&global_path, toml::to_string_pretty(&table).unwrap_or_default());
                tracing::info!(
                    target_name = %candidate.target_name,
                    "prompt-promoter: promoted tool guideline to global"
                );
            }
            "skill" => {
                let global_skills = tau_agent_base::paths::config_dir()
                    .join("skills").join("auto");
                let _ = std::fs::create_dir_all(&global_skills);

                let source_project_path = {
                    let st = lock_state(state);
                    st.db.get_project(&candidate.project_name).ok().flatten().map(|p| p.path)
                };
                if let Some(path) = source_project_path {
                    let src = std::path::Path::new(&path)
                        .join(".tau").join("skills").join("auto").join(&candidate.target_name);
                    let dst = global_skills.join(&candidate.target_name);
                    let _ = std::fs::copy(&src, &dst);
                    tracing::info!(
                        target_name = %candidate.target_name,
                        "prompt-promoter: promoted skill to global"
                    );
                }
            }
            _ => {
                tracing::debug!(target = %candidate.target, "prompt-promoter: skipping non-promotable target");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_promotable_optimizations() {
        let active_opts = vec![
            OptimizationRow {
                id: 1, project_name: "proj-a".into(), target: "tool_guideline".into(),
                target_name: "bash".into(), old_hash: "o".into(), new_hash: "abc".into(),
                risk: "low".into(), status: "active".into(), reverted_reason: None,
                applied_at: 1000, reverted_at: None,
            },
            OptimizationRow {
                id: 2, project_name: "proj-b".into(), target: "tool_guideline".into(),
                target_name: "bash".into(), old_hash: "o".into(), new_hash: "abc".into(),
                risk: "low".into(), status: "active".into(), reverted_reason: None,
                applied_at: 1000, reverted_at: None,
            },
        ];
        let candidates = find_promotion_candidates(&active_opts, 2);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].target_name, "bash");
        assert_eq!(candidates[0].new_hash, "abc");
    }

    #[test]
    fn does_not_promote_single_project() {
        let active_opts = vec![
            OptimizationRow {
                id: 1, project_name: "proj-a".into(), target: "tool_guideline".into(),
                target_name: "bash".into(), old_hash: "o".into(), new_hash: "abc".into(),
                risk: "low".into(), status: "active".into(), reverted_reason: None,
                applied_at: 1000, reverted_at: None,
            },
        ];
        let candidates = find_promotion_candidates(&active_opts, 2);
        assert!(candidates.is_empty());
    }
}
