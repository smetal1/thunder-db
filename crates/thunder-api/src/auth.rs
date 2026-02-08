//! Authentication middleware for ThunderDB REST API
//!
//! Provides:
//! - Bearer token authentication
//! - API key authentication (for admin endpoints)
//! - Basic auth support
//! - Request authentication extraction

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use tracing::{debug, warn};

use thunder_common::rbac::{AuthContext, AuthResult, Permission, ObjectType, RbacManager};

use crate::ApiError;

// ============================================================================
// Auth State Extension
// ============================================================================

/// Extension to store authenticated context in request
#[derive(Clone)]
pub struct AuthenticatedUser(pub AuthContext);

// ============================================================================
// Auth Middleware
// ============================================================================

/// Authentication middleware for protected endpoints
pub async fn auth_middleware(
    State(rbac): State<Arc<RbacManager>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Try to extract authentication
    let auth_result = extract_auth(&request, &rbac);

    match auth_result {
        Ok(context) => {
            // Store auth context in request extensions
            request.extensions_mut().insert(AuthenticatedUser(context));
            next.run(request).await
        }
        Err(response) => response,
    }
}

/// Authentication middleware that requires admin permissions
pub async fn admin_auth_middleware(
    State(rbac): State<Arc<RbacManager>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Try to extract authentication
    let auth_result = extract_auth(&request, &rbac);

    match auth_result {
        Ok(context) => {
            // Check for admin permissions
            if !context.user.is_superuser
                && !context.has_permission(Permission::Vacuum, ObjectType::Server, "*")
                && !context.has_permission(Permission::Analyze, ObjectType::Server, "*")
            {
                warn!(
                    user = %context.user.username,
                    "Unauthorized admin access attempt"
                );
                return unauthorized_response("Admin privileges required");
            }

            request.extensions_mut().insert(AuthenticatedUser(context));
            next.run(request).await
        }
        Err(response) => response,
    }
}

/// Optional authentication middleware (allows unauthenticated access)
pub async fn optional_auth_middleware(
    State(rbac): State<Arc<RbacManager>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Try to extract authentication, but don't fail if not present
    if let Ok(context) = extract_auth(&request, &rbac) {
        request.extensions_mut().insert(AuthenticatedUser(context));
    }
    next.run(request).await
}

// ============================================================================
// Auth Extraction
// ============================================================================

/// Extract authentication from request headers
fn extract_auth(request: &Request, rbac: &RbacManager) -> Result<AuthContext, Response> {
    // First, try API key (X-API-Key header)
    if let Some(api_key) = request
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(context) = rbac.authenticate_api_key(api_key) {
            debug!("Authenticated via API key");
            return Ok(context);
        }
        return Err(unauthorized_response("Invalid API key"));
    }

    // Try Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let auth_header = match auth_header {
        Some(h) => h,
        None => return Err(unauthorized_response("Authentication required")),
    };

    // Try Bearer token
    if let Some(token) = auth_header.strip_prefix("Bearer ") {
        // For now, treat bearer token as API key
        // In future, could support JWT
        if let Some(context) = rbac.authenticate_api_key(token) {
            debug!("Authenticated via Bearer token");
            return Ok(context);
        }
        return Err(unauthorized_response("Invalid bearer token"));
    }

    // Try Basic auth
    if let Some(credentials) = auth_header.strip_prefix("Basic ") {
        return authenticate_basic(credentials, rbac);
    }

    Err(unauthorized_response("Unsupported authentication method"))
}

/// Authenticate using Basic auth
fn authenticate_basic(credentials: &str, rbac: &RbacManager) -> Result<AuthContext, Response> {
    // Decode base64
    let decoded = match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, credentials) {
        Ok(d) => d,
        Err(_) => return Err(unauthorized_response("Invalid Basic auth encoding")),
    };

    let decoded_str = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(_) => return Err(unauthorized_response("Invalid Basic auth encoding")),
    };

    // Split username:password
    let parts: Vec<&str> = decoded_str.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(unauthorized_response("Invalid Basic auth format"));
    }

    let username = parts[0];
    let password = parts[1];

    // Authenticate
    match rbac.authenticate(username, password) {
        AuthResult::Success(context) => {
            debug!(user = %username, "Authenticated via Basic auth");
            Ok(context)
        }
        AuthResult::InvalidCredentials => {
            warn!(user = %username, "Invalid credentials");
            Err(unauthorized_response("Invalid username or password"))
        }
        AuthResult::AccountLocked { until } => {
            warn!(user = %username, until = %until, "Account locked");
            Err(locked_response(until))
        }
        AuthResult::AccountDisabled => {
            warn!(user = %username, "Account disabled");
            Err(forbidden_response("Account is disabled"))
        }
        AuthResult::PasswordChangeRequired => {
            warn!(user = %username, "Password change required");
            Err(password_change_required_response())
        }
        AuthResult::UserNotFound => {
            warn!(user = %username, "User not found");
            Err(unauthorized_response("Invalid username or password"))
        }
    }
}

// ============================================================================
// Error Responses
// ============================================================================

fn unauthorized_response(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"ThunderDB\"")],
        Json(ApiError::unauthorized(message)),
    )
        .into_response()
}

fn forbidden_response(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError::new("FORBIDDEN", message)),
    )
        .into_response()
}

fn locked_response(until: chrono::DateTime<chrono::Utc>) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ApiError::new("ACCOUNT_LOCKED", format!("Account locked until {}", until))),
    )
        .into_response()
}

fn password_change_required_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError::new(
            "PASSWORD_CHANGE_REQUIRED",
            "You must change your password before continuing",
        )),
    )
        .into_response()
}

// ============================================================================
// Permission Checking
// ============================================================================

/// Check if the authenticated user has a specific permission
pub fn require_permission(
    auth: &AuthenticatedUser,
    permission: Permission,
    object_type: ObjectType,
    object_name: &str,
) -> Result<(), Response> {
    if auth.0.has_permission(permission, object_type, object_name) {
        Ok(())
    } else {
        Err(forbidden_response(&format!(
            "Permission denied: {:?} on {:?} '{}'",
            permission, object_type, object_name
        )))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use thunder_common::config::SecurityConfig;

    fn create_test_rbac() -> Arc<RbacManager> {
        let config = SecurityConfig::development();
        Arc::new(RbacManager::new(config))
    }

    #[test]
    fn test_basic_auth_parsing() {
        let rbac = create_test_rbac();

        // Test valid credentials (admin:admin_dev_only in base64)
        let credentials = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "admin:admin_dev_only",
        );

        let result = authenticate_basic(&credentials, &rbac);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_basic_auth() {
        let rbac = create_test_rbac();

        let credentials = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "admin:wrong_password",
        );

        let result = authenticate_basic(&credentials, &rbac);
        assert!(result.is_err());
    }
}
