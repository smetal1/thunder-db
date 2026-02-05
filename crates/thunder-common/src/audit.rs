//! Audit Logging for ThunderDB
//!
//! Provides structured audit events for security-relevant operations:
//! - Authentication attempts (success/failure)
//! - Authorization checks
//! - Schema changes (DDL)
//! - Administrative actions
//! - Configuration changes
//!
//! Audit events are emitted via `tracing` with target `"audit"` so they
//! can be filtered and routed independently (e.g., to a dedicated log file).

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ============================================================================
// Audit Event Types
// ============================================================================

/// Classification of audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    /// Login / logout / token validation
    Authentication,
    /// Permission check (granted or denied)
    Authorization,
    /// SQL query execution
    QueryExecution,
    /// DDL statement (CREATE, ALTER, DROP)
    SchemaChange,
    /// Admin endpoint or maintenance operation
    AdminAction,
    /// Configuration reload or change
    ConfigChange,
}

/// Outcome of the audited action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
}

// ============================================================================
// Audit Event
// ============================================================================

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Event classification.
    pub event_type: AuditEventType,
    /// Username that triggered the event (empty for unauthenticated).
    pub user: String,
    /// Client IP address (if available).
    pub source_ip: Option<IpAddr>,
    /// Object being acted upon (table name, endpoint, etc.).
    pub object: String,
    /// Human-readable action description.
    pub action: String,
    /// Whether the action succeeded or was denied.
    pub outcome: AuditOutcome,
    /// Optional structured details.
    pub details: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event with the current timestamp.
    pub fn new(
        event_type: AuditEventType,
        user: impl Into<String>,
        action: impl Into<String>,
        outcome: AuditOutcome,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            event_type,
            user: user.into(),
            source_ip: None,
            object: String::new(),
            action: action.into(),
            outcome,
            details: None,
        }
    }

    /// Set the source IP.
    pub fn with_source_ip(mut self, ip: IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Set the object being acted upon.
    pub fn with_object(mut self, object: impl Into<String>) -> Self {
        self.object = object.into();
        self
    }

    /// Set additional details.
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

// ============================================================================
// Audit Logger
// ============================================================================

/// Trait for emitting audit events.
pub trait AuditLogger: Send + Sync {
    fn log(&self, event: &AuditEvent);
}

/// Audit logger implementation that emits events via `tracing` with target `"audit"`.
///
/// Events are logged at INFO level as structured JSON so they can be
/// filtered with tracing directives like `audit=info`.
#[derive(Debug, Clone, Default)]
pub struct TracingAuditLogger;

impl AuditLogger for TracingAuditLogger {
    fn log(&self, event: &AuditEvent) {
        tracing::info!(
            target: "audit",
            event_type = ?event.event_type,
            user = %event.user,
            source_ip = ?event.source_ip,
            object = %event.object,
            action = %event.action,
            outcome = ?event.outcome,
            details = ?event.details,
            "audit_event"
        );
    }
}

/// Global convenience function to emit an audit event via the tracing-based logger.
pub fn audit_log(event: &AuditEvent) {
    static LOGGER: TracingAuditLogger = TracingAuditLogger;
    LOGGER.log(event);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_creation() {
        let event = AuditEvent::new(
            AuditEventType::Authentication,
            "admin",
            "login",
            AuditOutcome::Success,
        )
        .with_source_ip("192.168.1.1".parse().unwrap())
        .with_object("session")
        .with_details("Basic auth");

        assert_eq!(event.event_type, AuditEventType::Authentication);
        assert_eq!(event.user, "admin");
        assert_eq!(event.source_ip, Some("192.168.1.1".parse().unwrap()));
        assert_eq!(event.object, "session");
        assert_eq!(event.outcome, AuditOutcome::Success);
        assert_eq!(event.details.as_deref(), Some("Basic auth"));
    }

    #[test]
    fn test_audit_event_serialization() {
        let event = AuditEvent::new(
            AuditEventType::SchemaChange,
            "admin",
            "CREATE TABLE users",
            AuditOutcome::Success,
        );
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("schema_change"));
        assert!(json.contains("admin"));

        // Deserialize back
        let _parsed: AuditEvent = serde_json::from_str(&json).unwrap();
    }
}
