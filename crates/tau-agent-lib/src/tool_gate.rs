//! Smart tool approval — static risk classification for tool calls.
//!
//! Classifies tool calls by risk level and blocks dangerous operations
//! without requiring user interaction for safe ones.

use tau_agent_base::types::ToolCall;
use tau_agent_engine::agent::ToolGateVerdict;

/// Risk level for a tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLevel {
    /// Read-only or informational — always safe.
    Safe,
    /// Normal mutation — allow by default.
    Normal,
    /// Dangerous or irreversible — deny.
    Dangerous,
}

/// Evaluate risk of a tool call based on tool name and arguments.
pub fn assess(tc: &ToolCall) -> ToolGateVerdict {
    match classify(tc) {
        RiskLevel::Dangerous => ToolGateVerdict::Deny(reason(tc)),
        _ => ToolGateVerdict::Allow,
    }
}

fn classify(tc: &ToolCall) -> RiskLevel {
    match tc.name.as_str() {
        // Read-only tools — always safe
        "read" | "diagnostics" | "session_search" | "memory" | "list_sessions"
        | "session_read" | "skill_patch" => RiskLevel::Safe,

        "bash" => classify_bash(tc),
        "write" | "edit" => RiskLevel::Normal,

        // Default: allow unknown tools (plugins define their own)
        _ => RiskLevel::Normal,
    }
}

fn classify_bash(tc: &ToolCall) -> RiskLevel {
    let cmd = tc
        .arguments
        .get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    for pattern in DANGEROUS_PATTERNS {
        if pattern.matches(cmd) {
            return RiskLevel::Dangerous;
        }
    }
    RiskLevel::Normal
}

fn reason(tc: &ToolCall) -> String {
    let cmd = tc
        .arguments
        .get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    for pattern in DANGEROUS_PATTERNS {
        if pattern.matches(cmd) {
            return format!("{}: {}", pattern.label, cmd);
        }
    }
    format!("dangerous tool call: {}", tc.name)
}

struct DangerousPattern {
    label: &'static str,
    fragments: &'static [&'static str],
}

impl DangerousPattern {
    fn matches(&self, cmd: &str) -> bool {
        self.fragments.iter().all(|f| cmd.contains(f))
    }
}

const DANGEROUS_PATTERNS: &[DangerousPattern] = &[
    DangerousPattern {
        label: "recursive force delete at root",
        fragments: &["rm ", "-rf", " /"],
    },
    DangerousPattern {
        label: "recursive force delete at root",
        fragments: &["rm ", "-fr", " /"],
    },
    DangerousPattern {
        label: "force push",
        fragments: &["git", "push", "--force"],
    },
    DangerousPattern {
        label: "force push",
        fragments: &["git", "push", "-f"],
    },
    DangerousPattern {
        label: "destructive reset",
        fragments: &["git", "reset", "--hard"],
    },
    DangerousPattern {
        label: "drop database",
        fragments: &["DROP DATABASE"],
    },
    DangerousPattern {
        label: "drop database",
        fragments: &["drop database"],
    },
    DangerousPattern {
        label: "format disk",
        fragments: &["mkfs"],
    },
    DangerousPattern {
        label: "format disk",
        fragments: &["dd ", "of=/dev/"],
    },
    DangerousPattern {
        label: "recursive chmod world-writable",
        fragments: &["chmod", "-R", "777"],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    fn tc(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "tc-1".into(),
            name: name.into(),
            arguments: args,
        }
    }

    #[test]
    fn read_is_safe() {
        let t = tc("read", serde_json::json!({"path": "/etc/passwd"}));
        assert_eq!(classify(&t), RiskLevel::Safe);
    }

    #[test]
    fn normal_bash_allowed() {
        let t = tc("bash", serde_json::json!({"command": "ls -la"}));
        assert_eq!(classify(&t), RiskLevel::Normal);
    }

    #[test]
    fn rm_rf_root_blocked() {
        let t = tc("bash", serde_json::json!({"command": "rm -rf /"}));
        assert_eq!(classify(&t), RiskLevel::Dangerous);
        assert!(matches!(assess(&t), ToolGateVerdict::Deny(_)));
    }

    #[test]
    fn rm_fr_root_blocked() {
        let t = tc("bash", serde_json::json!({"command": "rm -fr /"}));
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }

    #[test]
    fn git_force_push_blocked() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "git push --force origin main"}),
        );
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }

    #[test]
    fn git_push_f_blocked() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "git push -f origin main"}),
        );
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }

    #[test]
    fn git_reset_hard_blocked() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "git reset --hard HEAD~5"}),
        );
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }

    #[test]
    fn normal_git_push_allowed() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "git push origin feature-branch"}),
        );
        assert_eq!(classify(&t), RiskLevel::Normal);
    }

    #[test]
    fn rm_rf_non_root_allowed() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "rm -rf ./build"}),
        );
        assert_eq!(classify(&t), RiskLevel::Normal);
    }

    #[test]
    fn drop_database_blocked() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "psql -c 'DROP DATABASE production'"}),
        );
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }

    #[test]
    fn dd_to_dev_blocked() {
        let t = tc(
            "bash",
            serde_json::json!({"command": "dd if=/dev/zero of=/dev/sda bs=1M"}),
        );
        assert_eq!(classify(&t), RiskLevel::Dangerous);
    }
}
