//! Periodic background job that fires due scheduled automations.
//!
//! Ticks every 60 seconds. For each schedule whose `next_run_at` has
//! passed, creates a session and sends a Chat request, then updates
//! `last_run_at` and computes the next fire time.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::bg_tasks::{BgJob, BgTaskScheduler, BgTrigger};
use super::state::{SessionLocks, SharedState, lock_state};
use super::{SharedTestOverrides, ShutdownHandle};

struct ScheduleRunner {
    plugins: Arc<Mutex<crate::plugin::PluginManager>>,
    shutdown: ShutdownHandle,
    session_locks: SessionLocks,
    throttle: crate::throttle::ProviderThrottle,
}

#[async_trait]
impl BgJob for ScheduleRunner {
    fn name(&self) -> &'static str {
        "schedule-runner"
    }

    async fn run(&self, state: &SharedState) {
        if let Err(e) = tick(
            state,
            &self.plugins,
            &self.shutdown,
            &self.session_locks,
            &self.throttle,
        )
        .await
        {
            tracing::warn!(%e, "schedule-runner tick error");
        }
    }
}

/// Register the schedule runner as a periodic background job (60s interval).
pub(super) async fn register(
    sched: &Arc<BgTaskScheduler>,
    plugins: Arc<Mutex<crate::plugin::PluginManager>>,
    shutdown: ShutdownHandle,
    session_locks: SessionLocks,
    throttle: crate::throttle::ProviderThrottle,
) {
    sched
        .register(
            BgTrigger::Periodic {
                delay: std::time::Duration::from_secs(30),
                interval: std::time::Duration::from_secs(60),
            },
            Arc::new(ScheduleRunner {
                plugins,
                shutdown,
                session_locks,
                throttle,
            }),
        )
        .await;
}

async fn tick(
    state: &SharedState,
    plugins: &Arc<Mutex<crate::plugin::PluginManager>>,
    shutdown: &ShutdownHandle,
    session_locks: &SessionLocks,
    throttle: &crate::throttle::ProviderThrottle,
) -> crate::Result<()> {
    use cron::Schedule;
    use std::str::FromStr;

    let now_secs = chrono::Utc::now().timestamp();
    let due = {
        let st = lock_state(state);
        st.db.get_due_schedules(now_secs)?
    };

    if due.is_empty() {
        return Ok(());
    }

    tracing::info!(count = due.len(), "schedule-runner: firing due schedules");

    for sched_info in &due {
        let full_expr = format!("0 {} *", sched_info.cron_expr);
        let next_run_at = Schedule::from_str(&full_expr)
            .ok()
            .and_then(|s| s.upcoming(chrono::Utc).next())
            .map(|dt| dt.timestamp());

        // Create session
        let resp = super::dispatch::create_session_impl(
            state,
            &sched_info.model,
            &None,
            &sched_info.system_prompt,
            &sched_info.cwd,
            &None, // parent_id
            0,     // child_budget
            &Some(format!("[scheduled] {}", sched_info.name)),
            true,  // auto_archive
            false, // notify_parent
            &sched_info.project_name,
        );

        let session_id = match resp {
            crate::protocol::Response::SessionCreated { ref session_id } => session_id.clone(),
            crate::protocol::Response::Error { message } => {
                tracing::warn!(schedule = %sched_info.name, %message, "schedule-runner: session create failed");
                continue;
            }
            _ => continue,
        };

        tracing::info!(
            schedule = %sched_info.name,
            session_id = %session_id,
            "schedule-runner: firing"
        );

        // Spawn child chat (fire-and-forget)
        let s = state.clone();
        let p = plugins.clone();
        let sh = shutdown.clone();
        let sl = session_locks.clone();
        let th = throttle.clone();
        let prompt = sched_info.prompt.clone();
        let sid = session_id.clone();
        smol::spawn(async move {
            if let Err(e) = super::agent_runner::run_child_chat(
                s,
                p,
                sh,
                sl,
                th,
                sid.clone(),
                prompt,
                Vec::new(),
                SharedTestOverrides::default(),
            )
            .await
            {
                tracing::warn!(session_id = %sid, %e, "schedule-runner: child chat error");
            }
        })
        .detach();

        // Update last_run_at and next_run_at
        {
            let st = lock_state(state);
            if let Err(e) = st.db.update_schedule_run(sched_info.id, now_secs, next_run_at) {
                tracing::warn!(%e, schedule = %sched_info.name, "schedule-runner: update_schedule_run failed");
            }
        }
    }

    Ok(())
}
