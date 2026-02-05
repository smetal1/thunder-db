//! Role-Based Access Control (RBAC) for ThunderDB
//!
//! Provides:
//! - User management with secure password storage
//! - Role definitions with hierarchical permissions
//! - Permission grants on databases, schemas, and tables
//! - Session-based authentication context

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::config::{AuthRuleMethod, SecurityConfig};

// ============================================================================
// Permissions
// ============================================================================

/// Database object types that can have permissions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectType {
    /// Server-level (global)
    Server,
    /// Database
    Database,
    /// Schema within a database
    Schema,
    /// Table
    Table,
    /// Index
    Index,
    /// Sequence
    Sequence,
    /// Function/Procedure
    Function,
}

/// Permission types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    // Data permissions
    Select,
    Insert,
    Update,
    Delete,
    Truncate,

    // Schema permissions
    Create,
    Alter,
    Drop,

    // Administrative permissions
    Grant,      // Can grant permissions to others
    Revoke,     // Can revoke permissions from others

    // Server-level permissions
    CreateDatabase,
    CreateUser,
    CreateRole,
    Superuser,  // Full access to everything

    // Operational permissions
    Vacuum,
    Analyze,
    Reindex,
    Backup,
    Restore,

    // Connection permissions
    Connect,
    Disconnect,
}

impl Permission {
    /// Check if this is a superuser-only permission
    pub fn requires_superuser(&self) -> bool {
        matches!(
            self,
            Permission::Superuser
                | Permission::CreateUser
                | Permission::CreateRole
                | Permission::Backup
                | Permission::Restore
        )
    }
}

/// A grant of permissions on a specific object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionGrant {
    /// Object type
    pub object_type: ObjectType,
    /// Object name (database.schema.table or "*" for all)
    pub object_name: String,
    /// Granted permissions
    pub permissions: HashSet<Permission>,
    /// Can this user grant these permissions to others?
    pub with_grant_option: bool,
}

// ============================================================================
// Roles
// ============================================================================

/// Built-in roles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinRole {
    /// Full superuser access
    Superuser,
    /// Can create databases and users
    Admin,
    /// Can read all data
    ReadOnly,
    /// Can read and write data
    ReadWrite,
    /// Can only connect, no data access
    Connect,
}

impl BuiltinRole {
    /// Get the permissions for this built-in role
    pub fn permissions(&self) -> Vec<PermissionGrant> {
        match self {
            BuiltinRole::Superuser => vec![PermissionGrant {
                object_type: ObjectType::Server,
                object_name: "*".to_string(),
                permissions: HashSet::from([Permission::Superuser]),
                with_grant_option: true,
            }],
            BuiltinRole::Admin => vec![
                PermissionGrant {
                    object_type: ObjectType::Server,
                    object_name: "*".to_string(),
                    permissions: HashSet::from([
                        Permission::CreateDatabase,
                        Permission::CreateUser,
                        Permission::CreateRole,
                        Permission::Vacuum,
                        Permission::Analyze,
                    ]),
                    with_grant_option: false,
                },
                PermissionGrant {
                    object_type: ObjectType::Database,
                    object_name: "*".to_string(),
                    permissions: HashSet::from([
                        Permission::Create,
                        Permission::Alter,
                        Permission::Drop,
                        Permission::Grant,
                    ]),
                    with_grant_option: true,
                },
            ],
            BuiltinRole::ReadOnly => vec![PermissionGrant {
                object_type: ObjectType::Table,
                object_name: "*".to_string(),
                permissions: HashSet::from([Permission::Select]),
                with_grant_option: false,
            }],
            BuiltinRole::ReadWrite => vec![PermissionGrant {
                object_type: ObjectType::Table,
                object_name: "*".to_string(),
                permissions: HashSet::from([
                    Permission::Select,
                    Permission::Insert,
                    Permission::Update,
                    Permission::Delete,
                ]),
                with_grant_option: false,
            }],
            BuiltinRole::Connect => vec![PermissionGrant {
                object_type: ObjectType::Server,
                object_name: "*".to_string(),
                permissions: HashSet::from([Permission::Connect]),
                with_grant_option: false,
            }],
        }
    }
}

/// A custom role definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Built-in role this extends (if any)
    pub extends: Option<BuiltinRole>,
    /// Direct permission grants
    pub grants: Vec<PermissionGrant>,
    /// Other roles this role inherits from
    pub inherits: Vec<String>,
    /// Is this role a system role (cannot be dropped)
    pub is_system: bool,
}

// ============================================================================
// Users
// ============================================================================

/// User account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Username
    pub username: String,
    /// Password hash (argon2id or sha256)
    pub password_hash: String,
    /// Assigned roles
    pub roles: Vec<String>,
    /// Direct permission grants (in addition to roles)
    pub grants: Vec<PermissionGrant>,
    /// Is this user active?
    pub is_active: bool,
    /// Is this user a superuser?
    pub is_superuser: bool,
    /// Must change password on next login?
    pub must_change_password: bool,
    /// Account creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last login timestamp
    pub last_login: Option<chrono::DateTime<chrono::Utc>>,
    /// Failed login attempts (reset on successful login)
    pub failed_login_attempts: u32,
    /// Lockout until (if locked out)
    pub locked_until: Option<chrono::DateTime<chrono::Utc>>,
    /// Default database
    pub default_database: Option<String>,
    /// Connection limit (-1 for unlimited)
    pub connection_limit: i32,
}

impl User {
    /// Create a new user
    pub fn new(username: String, password_hash: String) -> Self {
        Self {
            username,
            password_hash,
            roles: vec![],
            grants: vec![],
            is_active: true,
            is_superuser: false,
            must_change_password: false,
            created_at: chrono::Utc::now(),
            last_login: None,
            failed_login_attempts: 0,
            locked_until: None,
            default_database: None,
            connection_limit: -1,
        }
    }

    /// Check if the user is currently locked out
    pub fn is_locked(&self) -> bool {
        if let Some(locked_until) = self.locked_until {
            chrono::Utc::now() < locked_until
        } else {
            false
        }
    }
}

// ============================================================================
// Authentication Context
// ============================================================================

/// Authentication result
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Authentication successful
    Success(AuthContext),
    /// Invalid credentials
    InvalidCredentials,
    /// Account is locked
    AccountLocked { until: chrono::DateTime<chrono::Utc> },
    /// Account is disabled
    AccountDisabled,
    /// Password must be changed
    PasswordChangeRequired,
    /// User not found
    UserNotFound,
}

/// Authenticated session context
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Authenticated user
    pub user: User,
    /// Effective permissions (computed from roles + direct grants)
    pub effective_permissions: Vec<PermissionGrant>,
    /// Session start time
    pub session_start: Instant,
    /// Current database
    pub current_database: Option<String>,
}

impl AuthContext {
    /// Check if this context has a specific permission on an object
    pub fn has_permission(
        &self,
        permission: Permission,
        object_type: ObjectType,
        object_name: &str,
    ) -> bool {
        // Superusers have all permissions
        if self.user.is_superuser {
            return true;
        }

        // Check effective permissions
        for grant in &self.effective_permissions {
            // Check if grant applies to this object type
            if grant.object_type != object_type && grant.object_type != ObjectType::Server {
                continue;
            }

            // Check if grant applies to this object (wildcard or exact match)
            if grant.object_name != "*" && grant.object_name != object_name {
                // Check prefix match for hierarchical objects (database.schema.table)
                if !object_name.starts_with(&format!("{}.", grant.object_name)) {
                    continue;
                }
            }

            // Check if permission is granted
            if grant.permissions.contains(&permission) {
                return true;
            }

            // Superuser permission grants all
            if grant.permissions.contains(&Permission::Superuser) {
                return true;
            }
        }

        false
    }

    /// Check if this context can grant a permission
    pub fn can_grant(
        &self,
        permission: Permission,
        object_type: ObjectType,
        object_name: &str,
    ) -> bool {
        if self.user.is_superuser {
            return true;
        }

        for grant in &self.effective_permissions {
            if !grant.with_grant_option {
                continue;
            }

            if grant.object_type != object_type && grant.object_type != ObjectType::Server {
                continue;
            }

            if grant.object_name != "*" && grant.object_name != object_name {
                continue;
            }

            if grant.permissions.contains(&permission) || grant.permissions.contains(&Permission::Grant) {
                return true;
            }
        }

        false
    }
}

// ============================================================================
// RBAC Manager
// ============================================================================

/// RBAC Manager for managing users, roles, and permissions
pub struct RbacManager {
    /// Security configuration
    config: SecurityConfig,
    /// Users by username
    users: RwLock<HashMap<String, User>>,
    /// Roles by name
    roles: RwLock<HashMap<String, Role>>,
    /// Active sessions
    sessions: RwLock<HashMap<uuid::Uuid, AuthContext>>,
}

impl RbacManager {
    /// Create a new RBAC manager
    pub fn new(config: SecurityConfig) -> Self {
        let manager = Self {
            config,
            users: RwLock::new(HashMap::new()),
            roles: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
        };

        // Initialize built-in roles
        manager.init_builtin_roles();

        // Initialize superuser if configured
        manager.init_superuser();

        manager
    }

    /// Initialize built-in roles
    fn init_builtin_roles(&self) {
        let mut roles = self.roles.write();

        for builtin in [
            BuiltinRole::Superuser,
            BuiltinRole::Admin,
            BuiltinRole::ReadOnly,
            BuiltinRole::ReadWrite,
            BuiltinRole::Connect,
        ] {
            let role = Role {
                name: format!("{:?}", builtin).to_lowercase(),
                description: Some(format!("Built-in {:?} role", builtin)),
                extends: Some(builtin),
                grants: builtin.permissions(),
                inherits: vec![],
                is_system: true,
            };
            roles.insert(role.name.clone(), role);
        }
    }

    /// Initialize superuser from config
    fn init_superuser(&self) {
        if let (Some(username), Some(password_hash)) = (
            self.config.superuser.as_ref(),
            self.config.superuser_password_hash.as_ref(),
        ) {
            let mut user = User::new(username.clone(), password_hash.clone());
            user.is_superuser = true;
            user.must_change_password = self.config.require_password_change;
            user.roles.push("superuser".to_string());

            self.users.write().insert(username.clone(), user);
        }
    }

    /// Check per-database authentication rules.
    ///
    /// Iterates `auth_rules` in order; the first matching rule wins.
    /// If no rule matches, defaults to `AuthRuleMethod::Password`.
    pub fn check_auth_rule(&self, user: &str, database: &str, _source_ip: &str) -> AuthRuleMethod {
        for rule in &self.config.auth_rules {
            let db_match = rule.database == "*" || rule.database == database;
            let user_match = rule.user == "*" || rule.user == user;
            let source_match = rule.source == "*"; // CIDR matching deferred; "*" only for now

            if db_match && user_match && source_match {
                return rule.method.clone();
            }
        }
        // Default: require password authentication
        AuthRuleMethod::Password
    }

    /// Authenticate a user
    pub fn authenticate(&self, username: &str, password: &str) -> AuthResult {
        let mut users = self.users.write();

        let user = match users.get_mut(username) {
            Some(u) => u,
            None => return AuthResult::UserNotFound,
        };

        // Check if account is locked
        if user.is_locked() {
            return AuthResult::AccountLocked {
                until: user.locked_until.unwrap(),
            };
        }

        // Check if account is active
        if !user.is_active {
            return AuthResult::AccountDisabled;
        }

        // Verify password
        if !SecurityConfig::verify_password(password, &user.password_hash) {
            // Increment failed attempts
            user.failed_login_attempts += 1;

            // Lock account if too many failures
            if user.failed_login_attempts >= self.config.max_login_attempts {
                user.locked_until = Some(
                    chrono::Utc::now()
                        + chrono::Duration::seconds(self.config.lockout_duration_secs as i64),
                );
            }

            return AuthResult::InvalidCredentials;
        }

        // Reset failed attempts on success
        user.failed_login_attempts = 0;
        user.locked_until = None;
        user.last_login = Some(chrono::Utc::now());

        // Check if password change required
        if user.must_change_password {
            return AuthResult::PasswordChangeRequired;
        }

        // Build auth context
        let effective_permissions = self.compute_effective_permissions(user);

        AuthResult::Success(AuthContext {
            user: user.clone(),
            effective_permissions,
            session_start: Instant::now(),
            current_database: user.default_database.clone(),
        })
    }

    /// Hash an API key with SHA-256 for secure storage.
    pub fn hash_api_key(key: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(b"thunderdb_api_key:");
        hasher.update(key.as_bytes());
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }

    /// Authenticate with API key (for admin endpoints).
    /// Supports both plaintext keys (legacy) and hashed keys.
    /// Comparison is always constant-time.
    pub fn authenticate_api_key(&self, api_key: &str) -> Option<AuthContext> {
        if let Some(ref expected_key) = self.config.admin_api_key {
            use subtle::ConstantTimeEq;

            let matches = if expected_key.starts_with("sha256:") {
                // Compare against hashed key
                let provided_hash = Self::hash_api_key(api_key);
                provided_hash.as_bytes().ct_eq(expected_key.as_bytes()).into()
            } else {
                // Legacy plaintext comparison (constant-time)
                api_key.as_bytes().ct_eq(expected_key.as_bytes()).into()
            };

            if matches {
                // Create an admin context
                let admin_user = User {
                    username: "_api_key_user".to_string(),
                    password_hash: String::new(),
                    roles: vec!["admin".to_string()],
                    grants: vec![],
                    is_active: true,
                    is_superuser: false,
                    must_change_password: false,
                    created_at: chrono::Utc::now(),
                    last_login: None,
                    failed_login_attempts: 0,
                    locked_until: None,
                    default_database: None,
                    connection_limit: 1,
                };

                let effective_permissions = self.compute_effective_permissions(&admin_user);

                return Some(AuthContext {
                    user: admin_user,
                    effective_permissions,
                    session_start: Instant::now(),
                    current_database: None,
                });
            }
        }
        None
    }

    /// Compute effective permissions for a user
    fn compute_effective_permissions(&self, user: &User) -> Vec<PermissionGrant> {
        let mut permissions = user.grants.clone();
        let roles = self.roles.read();

        // Add permissions from roles
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                permissions.extend(role.grants.clone());

                // Add permissions from inherited roles
                for inherited in &role.inherits {
                    if let Some(inherited_role) = roles.get(inherited) {
                        permissions.extend(inherited_role.grants.clone());
                    }
                }

                // Add permissions from built-in role if extends one
                if let Some(builtin) = role.extends {
                    permissions.extend(builtin.permissions());
                }
            }
        }

        permissions
    }

    /// Create a new user
    pub fn create_user(
        &self,
        auth: &AuthContext,
        username: String,
        password: &str,
    ) -> Result<(), String> {
        // Check permission
        if !auth.has_permission(Permission::CreateUser, ObjectType::Server, "*") {
            return Err("Permission denied: CREATE USER requires CreateUser permission".to_string());
        }

        // Validate password length
        if password.len() < self.config.min_password_length {
            return Err(format!(
                "Password must be at least {} characters",
                self.config.min_password_length
            ));
        }

        let mut users = self.users.write();

        if users.contains_key(&username) {
            return Err(format!("User '{}' already exists", username));
        }

        let password_hash = SecurityConfig::hash_password(password);
        let user = User::new(username.clone(), password_hash);
        users.insert(username, user);

        Ok(())
    }

    /// Grant a role to a user
    pub fn grant_role(
        &self,
        auth: &AuthContext,
        username: &str,
        role_name: &str,
    ) -> Result<(), String> {
        // Check permission
        if !auth.has_permission(Permission::Grant, ObjectType::Server, "*") {
            return Err("Permission denied: GRANT requires Grant permission".to_string());
        }

        let roles = self.roles.read();
        if !roles.contains_key(role_name) {
            return Err(format!("Role '{}' does not exist", role_name));
        }
        drop(roles);

        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| format!("User '{}' does not exist", username))?;

        if !user.roles.contains(&role_name.to_string()) {
            user.roles.push(role_name.to_string());
        }

        Ok(())
    }

    /// Revoke a role from a user
    pub fn revoke_role(
        &self,
        auth: &AuthContext,
        username: &str,
        role_name: &str,
    ) -> Result<(), String> {
        // Check permission
        if !auth.has_permission(Permission::Revoke, ObjectType::Server, "*") {
            return Err("Permission denied: REVOKE requires Revoke permission".to_string());
        }

        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| format!("User '{}' does not exist", username))?;

        user.roles.retain(|r| r != role_name);

        Ok(())
    }

    /// Register a session
    pub fn register_session(&self, session_id: uuid::Uuid, context: AuthContext) {
        self.sessions.write().insert(session_id, context);
    }

    /// Get session context
    pub fn get_session(&self, session_id: &uuid::Uuid) -> Option<AuthContext> {
        self.sessions.read().get(session_id).cloned()
    }

    /// Remove a session
    pub fn remove_session(&self, session_id: &uuid::Uuid) {
        self.sessions.write().remove(session_id);
    }

    /// List all users (names only)
    pub fn list_users(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    /// List all roles (names only)
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.read().keys().cloned().collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> RbacManager {
        let config = SecurityConfig::development();
        RbacManager::new(config)
    }

    #[test]
    fn test_password_hashing() {
        let password = "test_password_123";
        let hash = SecurityConfig::hash_password(password);

        assert!(hash.starts_with("argon2:"));
        assert!(SecurityConfig::verify_password(password, &hash));
        assert!(!SecurityConfig::verify_password("wrong_password", &hash));
    }

    #[test]
    fn test_password_hashing_legacy_sha256() {
        // Legacy SHA256 hashes should still verify for backward compatibility
        use sha2::{Sha256, Digest};
        let salt = "thunderdb_salt_v1";
        let mut hasher = Sha256::new();
        hasher.update(salt.as_bytes());
        hasher.update("admin_dev_only".as_bytes());
        let legacy_hash = format!("sha256:{}", hex::encode(hasher.finalize()));
        assert!(SecurityConfig::verify_password("admin_dev_only", &legacy_hash));
        assert!(!SecurityConfig::verify_password("wrong", &legacy_hash));
    }

    #[test]
    fn test_builtin_roles() {
        let manager = create_test_manager();
        let roles = manager.list_roles();

        assert!(roles.contains(&"superuser".to_string()));
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"readonly".to_string()));
        assert!(roles.contains(&"readwrite".to_string()));
    }

    #[test]
    fn test_superuser_authentication() {
        let manager = create_test_manager();

        // Should authenticate with dev credentials
        let result = manager.authenticate("admin", "admin_dev_only");
        assert!(matches!(result, AuthResult::Success(_)));
    }

    #[test]
    fn test_invalid_credentials() {
        let manager = create_test_manager();

        let result = manager.authenticate("admin", "wrong_password");
        assert!(matches!(result, AuthResult::InvalidCredentials));
    }

    #[test]
    fn test_user_not_found() {
        let manager = create_test_manager();

        let result = manager.authenticate("nonexistent", "password");
        assert!(matches!(result, AuthResult::UserNotFound));
    }

    #[test]
    fn test_permission_check() {
        let manager = create_test_manager();

        if let AuthResult::Success(context) = manager.authenticate("admin", "admin_dev_only") {
            // Superuser should have all permissions
            assert!(context.has_permission(Permission::Select, ObjectType::Table, "any_table"));
            assert!(context.has_permission(Permission::CreateDatabase, ObjectType::Server, "*"));
        } else {
            panic!("Authentication should succeed");
        }
    }

    #[test]
    fn test_api_key_authentication() {
        let manager = create_test_manager();

        // Should authenticate with dev API key
        let context = manager.authenticate_api_key("dev-api-key-not-for-production");
        assert!(context.is_some());

        // Should fail with wrong key
        let context = manager.authenticate_api_key("wrong-key");
        assert!(context.is_none());
    }

    #[test]
    fn test_account_lockout() {
        let mut config = SecurityConfig::development();
        config.max_login_attempts = 2;
        config.lockout_duration_secs = 60;
        let manager = RbacManager::new(config);

        // First two attempts fail
        let _ = manager.authenticate("admin", "wrong");
        let _ = manager.authenticate("admin", "wrong");

        // Third attempt should show locked
        let result = manager.authenticate("admin", "wrong");
        assert!(matches!(result, AuthResult::AccountLocked { .. }));
    }

    #[test]
    fn test_config_validation() {
        let config = SecurityConfig::default();
        let result = config.validate_for_production();
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("Superuser username")));
        assert!(errors.iter().any(|e| e.contains("Superuser password")));
    }
}
