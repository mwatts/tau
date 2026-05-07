//! Loads agent definitions from config+DB and spawns persistent background sessions.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::bg_tasks::{BgJob, BgTaskScheduler, BgTrigger};
use super::state::{SessionLocks, SharedState, lock_state};
use super::{SharedTestOverrides, ShutdownHandle};

struct AgentRunner {
    agent_id: i64,
    agent_name: String,
    prompt: String,
    model: Option<String>,
    system_prompt: Option<String>,
    project_name: Option<String>,
    plugins: Arc<Mutex<crate::plugin::PluginManager>>,
    shutdown: ShutdownHandle,
    session_locks: SessionLocks,
    throttle: crate::throttle::ProviderThrottle,
}

#[async_trait]
impl BgJob for AgentRunner {
    fn name(&self) -> &'static str {
        // Can't return &self.agent_name because lifetime is 'static.
        // Use a leaked string — there are only a handful of agents.
        Box::leak(format!("agent:{}", self.agent_name).into_boxed_str())
    }

    async fn run(&self, state: &SharedState) {
        // Create session
        let resp = super::dispatch::create_session_impl(
            state,
            &self.model,
            &None,
            &self.system_prompt,
            &None, // cwd
            &None, // parent_id
            0,     // child_budget
            &Some(format!("[agent] {}", self.agent_name)),
            true,  // auto_archive
            false, // notify_parent
            &self.project_name,
            true,  // is_agent
        );

        let session_id = match resp {
            crate::protocol::Response::SessionCreated { ref session_id } => session_id.clone(),
            crate::protocol::Response::Error { message } => {
                tracing::warn!(agent = %self.agent_name, %message, "agent-manager: session create failed");
                return;
            }
            _ => return,
        };

        // Record session_id in agents table
        {
            let st = lock_state(state);
            if let Err(e) = st.db.set_agent_session_id(self.agent_id, Some(&session_id)) {
                tracing::warn!(%e, agent = %self.agent_name, "agent-manager: failed to record session_id");
            }
        }

        tracing::info!(
            agent = %self.agent_name,
            session_id = %session_id,
            "agent-manager: starting agent session"
        );

        if let Err(e) = super::agent_runner::run_child_chat(
            state.clone(),
            self.plugins.clone(),
            self.shutdown.clone(),
            self.session_locks.clone(),
            self.throttle.clone(),
            session_id.clone(),
            self.prompt.clone(),
            Vec::new(),
            SharedTestOverrides::default(),
        )
        .await
        {
            tracing::warn!(
                agent = %self.agent_name,
                session_id = %session_id,
                %e,
                "agent-manager: agent session error"
            );
        }

        // Clear session_id when done
        {
            let st = lock_state(state);
            let _ = st.db.set_agent_session_id(self.agent_id, None);
        }
    }
}

/// Load agents from config + DB, upsert config agents into DB, register enabled ones.
pub(super) async fn register(
    sched: &Arc<BgTaskScheduler>,
    plugins: Arc<Mutex<crate::plugin::PluginManager>>,
    shutdown: ShutdownHandle,
    session_locks: SessionLocks,
    throttle: crate::throttle::ProviderThrottle,
    state: &SharedState,
) {
    // Load config-defined agents and upsert into DB
    let config = tau_agent_base::agent_config::load_agents_config(None, None);
    if !config.agent.is_empty() {
        let st = lock_state(state);
        for def in &config.agent {
            // Check if agent with this name already exists
            match st.db.list_agents() {
                Ok(existing) => {
                    if existing.iter().any(|a| a.name == def.name) {
                        continue; // already in DB, skip
                    }
                }
                Err(e) => {
                    tracing::warn!(%e, "agent-manager: failed to list agents");
                    continue;
                }
            }
            if let Err(e) = st.db.create_agent(
                &def.name,
                &def.prompt,
                def.model.as_deref(),
                &def.trigger_type,
                def.trigger_config.as_deref(),
                def.system_prompt.as_deref(),
                def.project_name.as_deref(),
                def.budget_usd,
                def.assignment_rules.as_deref(),
                def.max_concurrent_tasks,
            ) {
                tracing::warn!(%e, agent = %def.name, "agent-manager: failed to upsert agent");
            }
        }
    }

    // Load all enabled agents from DB and register persistent jobs
    let agents = {
        let st = lock_state(state);
        match st.db.list_agents() {
            Ok(agents) => agents,
            Err(e) => {
                tracing::warn!(%e, "agent-manager: failed to load agents");
                return;
            }
        }
    };

    let mut count = 0;
    for agent in agents {
        if !agent.enabled {
            continue;
        }
        if agent.trigger_type != "persistent" {
            continue; // only persistent agents are managed here
        }
        let runner = Arc::new(AgentRunner {
            agent_id: agent.id,
            agent_name: agent.name.clone(),
            prompt: agent.prompt,
            model: agent.model,
            system_prompt: agent.system_prompt,
            project_name: agent.project_name,
            plugins: plugins.clone(),
            shutdown: shutdown.clone(),
            session_locks: session_locks.clone(),
            throttle: throttle.clone(),
        });
        sched
            .register(
                BgTrigger::Persistent {
                    initial_delay: Duration::from_secs(5),
                    restart_delay: Duration::from_secs(10),
                    max_backoff: Duration::from_secs(300),
                },
                runner,
            )
            .await;
        count += 1;
    }

    if count > 0 {
        tracing::info!(count, "agent-manager: registered persistent agents");
    }
}
