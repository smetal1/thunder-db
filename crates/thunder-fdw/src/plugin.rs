//! FDW Plugin System
//!
//! Provides infrastructure for loading custom FDW plugins:
//! - Dynamic library loading
//! - Plugin registry
//! - Version compatibility checking
//! - Hot reloading support

use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use libloading::{Library, Symbol};
use tracing::{debug, error, info, warn};

use crate::{
    FdwCapabilities, ForeignDataWrapper, ForeignScan, ForeignServer, ForeignTableDef,
    ModifyOperation, Qual,
};
use thunder_common::prelude::*;

// ============================================================================
// Plugin API
// ============================================================================

/// Plugin API version
pub const PLUGIN_API_VERSION: u32 = 1;

/// Plugin descriptor returned by plugin initialization
#[repr(C)]
pub struct PluginDescriptor {
    /// API version (must match PLUGIN_API_VERSION)
    pub api_version: u32,
    /// Plugin name
    pub name: *const std::os::raw::c_char,
    /// Plugin version
    pub version: *const std::os::raw::c_char,
    /// Plugin description
    pub description: *const std::os::raw::c_char,
    /// Connector type (e.g., "oracle", "sqlserver")
    pub connector_type: *const std::os::raw::c_char,
    /// Factory function to create FDW instances
    pub create_fdw: CreateFdwFn,
    /// Cleanup function (optional)
    pub cleanup: Option<CleanupFn>,
}

/// Function type for creating FDW instances
pub type CreateFdwFn = unsafe extern "C" fn(
    server_name: *const std::os::raw::c_char,
    options: *const c_void,
    options_count: usize,
) -> *mut c_void;

/// Function type for cleanup
pub type CleanupFn = unsafe extern "C" fn();

/// Plugin initialization function signature
pub type PluginInitFn = unsafe extern "C" fn() -> *const PluginDescriptor;

// ============================================================================
// Plugin Loader
// ============================================================================

/// Loads and manages FDW plugins
pub struct PluginLoader {
    /// Loaded plugins by connector type
    plugins: DashMap<String, LoadedPlugin>,
    /// Plugin search paths
    search_paths: Vec<PathBuf>,
    /// Plugin file extension
    extension: String,
}

/// A loaded plugin
struct LoadedPlugin {
    /// The dynamic library
    #[allow(dead_code)]
    library: Arc<Library>,
    /// Plugin descriptor
    descriptor: PluginDescriptor,
    /// Plugin path
    path: PathBuf,
}

impl PluginLoader {
    /// Create a new plugin loader
    pub fn new() -> Self {
        let extension = if cfg!(target_os = "macos") {
            "dylib"
        } else if cfg!(target_os = "windows") {
            "dll"
        } else {
            "so"
        };

        Self {
            plugins: DashMap::new(),
            search_paths: vec![
                PathBuf::from("./plugins"),
                PathBuf::from("/usr/local/lib/thunderdb/plugins"),
                PathBuf::from("/opt/thunderdb/plugins"),
            ],
            extension: extension.to_string(),
        }
    }

    /// Add a search path for plugins
    pub fn add_search_path<P: AsRef<Path>>(&mut self, path: P) {
        self.search_paths.push(path.as_ref().to_path_buf());
    }

    /// Load a plugin from a file path
    pub fn load_plugin<P: AsRef<Path>>(&self, path: P) -> Result<String> {
        let path = path.as_ref();

        // Load the dynamic library
        let library = unsafe {
            Library::new(path)
                .map_err(|e| Error::Internal(format!("Failed to load plugin {}: {}", path.display(), e)))?
        };

        // Get the init function
        let init_fn: Symbol<PluginInitFn> = unsafe {
            library.get(b"thunderdb_plugin_init\0")
                .map_err(|e| Error::Internal(format!("Plugin missing init function: {}", e)))?
        };

        // Initialize the plugin
        let descriptor_ptr = unsafe { init_fn() };
        if descriptor_ptr.is_null() {
            return Err(Error::Internal("Plugin init returned null".to_string()));
        }

        let descriptor = unsafe { std::ptr::read(descriptor_ptr) };

        // Check API version
        if descriptor.api_version != PLUGIN_API_VERSION {
            return Err(Error::Internal(format!(
                "Plugin API version mismatch: expected {}, got {}",
                PLUGIN_API_VERSION, descriptor.api_version
            )));
        }

        // Get connector type
        let connector_type = unsafe {
            CStr::from_ptr(descriptor.connector_type)
                .to_string_lossy()
                .to_string()
        };

        let plugin_name = unsafe {
            CStr::from_ptr(descriptor.name)
                .to_string_lossy()
                .to_string()
        };

        info!(
            plugin = %plugin_name,
            connector_type = %connector_type,
            path = %path.display(),
            "Loaded FDW plugin"
        );

        // Store the plugin
        self.plugins.insert(
            connector_type.clone(),
            LoadedPlugin {
                library: Arc::new(library),
                descriptor,
                path: path.to_path_buf(),
            },
        );

        Ok(connector_type)
    }

    /// Load all plugins from a directory
    pub fn load_plugins_from_dir<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<String>> {
        let dir = dir.as_ref();

        if !dir.exists() {
            return Ok(vec![]);
        }

        let mut loaded = Vec::new();

        for entry in std::fs::read_dir(dir)
            .map_err(|e| Error::Internal(format!("Failed to read directory: {}", e)))?
        {
            let entry = entry.map_err(|e| Error::Internal(format!("Dir entry error: {}", e)))?;
            let path = entry.path();

            if path.extension().map(|e| e == self.extension.as_str()).unwrap_or(false) {
                match self.load_plugin(&path) {
                    Ok(connector_type) => loaded.push(connector_type),
                    Err(e) => warn!(path = %path.display(), error = %e, "Failed to load plugin"),
                }
            }
        }

        Ok(loaded)
    }

    /// Load plugins from all search paths
    pub fn load_all_plugins(&self) -> Result<Vec<String>> {
        let mut loaded = Vec::new();

        for path in &self.search_paths {
            match self.load_plugins_from_dir(path) {
                Ok(plugins) => loaded.extend(plugins),
                Err(e) => debug!(path = %path.display(), error = %e, "Failed to load plugins from path"),
            }
        }

        Ok(loaded)
    }

    /// Check if a connector type is available
    pub fn has_connector(&self, connector_type: &str) -> bool {
        self.plugins.contains_key(connector_type)
    }

    /// Get plugin info
    pub fn get_plugin_info(&self, connector_type: &str) -> Option<PluginInfo> {
        self.plugins.get(connector_type).map(|p| {
            let desc = &p.descriptor;
            PluginInfo {
                name: unsafe { CStr::from_ptr(desc.name).to_string_lossy().to_string() },
                version: unsafe { CStr::from_ptr(desc.version).to_string_lossy().to_string() },
                description: unsafe { CStr::from_ptr(desc.description).to_string_lossy().to_string() },
                connector_type: unsafe { CStr::from_ptr(desc.connector_type).to_string_lossy().to_string() },
                path: p.path.clone(),
            }
        })
    }

    /// List all loaded plugins
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.plugins
            .iter()
            .map(|entry| {
                let p = entry.value();
                let desc = &p.descriptor;
                PluginInfo {
                    name: unsafe { CStr::from_ptr(desc.name).to_string_lossy().to_string() },
                    version: unsafe { CStr::from_ptr(desc.version).to_string_lossy().to_string() },
                    description: unsafe { CStr::from_ptr(desc.description).to_string_lossy().to_string() },
                    connector_type: unsafe { CStr::from_ptr(desc.connector_type).to_string_lossy().to_string() },
                    path: p.path.clone(),
                }
            })
            .collect()
    }

    /// Create an FDW instance from a plugin
    pub fn create_fdw(&self, server: &ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> {
        let plugin = self.plugins.get(&server.connector_type).ok_or_else(|| {
            Error::not_found("Plugin connector", &server.connector_type)
        })?;

        // Convert server name to C string
        let server_name = CString::new(server.name.as_str())
            .map_err(|e| Error::Internal(format!("Invalid server name: {}", e)))?;

        // Convert options to C format (simplified)
        let options_data: Vec<(CString, CString)> = server
            .options
            .iter()
            .filter_map(|(k, v)| {
                let key = CString::new(k.as_str()).ok()?;
                let value = CString::new(v.as_str()).ok()?;
                Some((key, value))
            })
            .collect();

        // Call the plugin's create function
        let fdw_ptr = unsafe {
            (plugin.descriptor.create_fdw)(
                server_name.as_ptr(),
                options_data.as_ptr() as *const c_void,
                options_data.len(),
            )
        };

        if fdw_ptr.is_null() {
            return Err(Error::Internal("Plugin failed to create FDW".to_string()));
        }

        // Wrap in a PluginFdw
        Ok(Box::new(PluginFdw {
            fdw_ptr,
            connector_type: server.connector_type.clone(),
        }))
    }

    /// Unload a plugin
    pub fn unload_plugin(&self, connector_type: &str) -> Result<()> {
        if let Some((_, plugin)) = self.plugins.remove(connector_type) {
            // Call cleanup if available
            if let Some(cleanup) = plugin.descriptor.cleanup {
                unsafe { cleanup() };
            }
            info!(connector_type = %connector_type, "Unloaded FDW plugin");
        }
        Ok(())
    }

    /// Unload all plugins
    pub fn unload_all(&self) {
        for entry in self.plugins.iter() {
            if let Some(cleanup) = entry.value().descriptor.cleanup {
                unsafe { cleanup() };
            }
        }
        self.plugins.clear();
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PluginLoader {
    fn drop(&mut self) {
        self.unload_all();
    }
}

/// Plugin information
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub connector_type: String,
    pub path: PathBuf,
}

// ============================================================================
// Plugin FDW Wrapper
// ============================================================================

/// Wrapper for plugin-provided FDW
struct PluginFdw {
    fdw_ptr: *mut c_void,
    connector_type: String,
}

// Safety: The plugin is responsible for thread safety
unsafe impl Send for PluginFdw {}
unsafe impl Sync for PluginFdw {}

#[async_trait::async_trait]
impl ForeignDataWrapper for PluginFdw {
    fn name(&self) -> &str {
        &self.connector_type
    }

    fn estimate_size(&self, _quals: &[Qual]) -> Result<(usize, f64)> {
        // Plugin FDWs need to implement this via FFI
        // For now, return default estimate
        Ok((10000, 1.0))
    }

    fn get_statistics(&self, _column: &str) -> Option<crate::ColumnStatistics> {
        None
    }

    async fn begin_scan(
        &self,
        _columns: &[String],
        _quals: &[Qual],
        _limit: Option<usize>,
    ) -> Result<Box<dyn ForeignScan>> {
        Err(Error::Internal(
            "Plugin FDW scanning not yet implemented".to_string(),
        ))
    }

    async fn modify(&self, _op: ModifyOperation) -> Result<u64> {
        Err(Error::Internal(
            "Plugin FDW modification not yet implemented".to_string(),
        ))
    }

    async fn import_schema(&self, _schema: &str) -> Result<Vec<ForeignTableDef>> {
        Err(Error::Internal(
            "Plugin FDW schema import not yet implemented".to_string(),
        ))
    }

    fn capabilities(&self) -> FdwCapabilities {
        FdwCapabilities {
            supports_predicate_pushdown: false,
            supports_limit_pushdown: false,
            supports_aggregation_pushdown: false,
            supports_modification: false,
            supports_transactions: false,
            max_connections: 10,
        }
    }
}

// ============================================================================
// Built-in Plugin Registry
// ============================================================================

/// Registry for built-in FDW connectors
pub struct BuiltinRegistry {
    factories: HashMap<String, Box<dyn Fn(&ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> + Send + Sync>>,
}

impl BuiltinRegistry {
    /// Create a new registry with built-in connectors
    pub fn new() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };

        // Register built-in connectors
        registry.register("postgres", |server| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                crate::postgres::PostgresFdw::new(server.clone())
                    .await
                    .map(|fdw| Box::new(fdw) as Box<dyn ForeignDataWrapper>)
            })
        });

        registry.register("mysql", |server| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                crate::mysql::MySqlFdw::new(server.clone())
                    .await
                    .map(|fdw| Box::new(fdw) as Box<dyn ForeignDataWrapper>)
            })
        });

        registry.register("mongodb", |server| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                crate::mongodb::MongoFdw::new(server.clone())
                    .await
                    .map(|fdw| Box::new(fdw) as Box<dyn ForeignDataWrapper>)
            })
        });

        registry.register("redis", |server| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                crate::redis::RedisFdw::new(server.clone())
                    .await
                    .map(|fdw| Box::new(fdw) as Box<dyn ForeignDataWrapper>)
            })
        });

        registry
    }

    /// Register a connector factory
    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(&ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> + Send + Sync + 'static,
    {
        self.factories.insert(name.to_string(), Box::new(factory));
    }

    /// Check if a connector type is registered
    pub fn has_connector(&self, connector_type: &str) -> bool {
        self.factories.contains_key(connector_type)
    }

    /// Create an FDW instance
    pub fn create_fdw(&self, server: &ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> {
        let factory = self.factories.get(&server.connector_type).ok_or_else(|| {
            Error::not_found("Connector", &server.connector_type)
        })?;
        factory(server)
    }

    /// List available connector types
    pub fn list_connectors(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for BuiltinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Combined Registry
// ============================================================================

/// Combined registry that checks both built-in and plugin connectors
pub struct CombinedRegistry {
    builtin: BuiltinRegistry,
    plugins: PluginLoader,
}

impl CombinedRegistry {
    /// Create a new combined registry
    pub fn new() -> Self {
        Self {
            builtin: BuiltinRegistry::new(),
            plugins: PluginLoader::new(),
        }
    }

    /// Load plugins from default search paths
    pub fn load_plugins(&self) -> Result<Vec<String>> {
        self.plugins.load_all_plugins()
    }

    /// Check if a connector is available
    pub fn has_connector(&self, connector_type: &str) -> bool {
        self.builtin.has_connector(connector_type) || self.plugins.has_connector(connector_type)
    }

    /// Create an FDW instance
    pub fn create_fdw(&self, server: &ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> {
        // Try built-in first
        if self.builtin.has_connector(&server.connector_type) {
            return self.builtin.create_fdw(server);
        }

        // Try plugins
        if self.plugins.has_connector(&server.connector_type) {
            return self.plugins.create_fdw(server);
        }

        Err(Error::not_found("Connector", &server.connector_type))
    }

    /// List all available connectors
    pub fn list_connectors(&self) -> Vec<String> {
        let mut connectors: Vec<String> = self
            .builtin
            .list_connectors()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        for info in self.plugins.list_plugins() {
            if !connectors.contains(&info.connector_type) {
                connectors.push(info.connector_type);
            }
        }

        connectors
    }
}

impl Default for CombinedRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_loader_creation() {
        let loader = PluginLoader::new();
        assert!(loader.plugins.is_empty());
    }

    #[test]
    fn test_builtin_registry() {
        let registry = BuiltinRegistry::new();
        assert!(registry.has_connector("postgres"));
        assert!(registry.has_connector("mysql"));
        assert!(registry.has_connector("mongodb"));
        assert!(registry.has_connector("redis"));
        assert!(!registry.has_connector("oracle"));
    }

    #[test]
    fn test_combined_registry() {
        let registry = CombinedRegistry::new();
        assert!(registry.has_connector("postgres"));
        assert!(!registry.has_connector("unknown"));
    }

    #[test]
    fn test_list_connectors() {
        let registry = BuiltinRegistry::new();
        let connectors = registry.list_connectors();
        assert!(connectors.contains(&"postgres"));
        assert!(connectors.contains(&"mysql"));
    }

    #[test]
    fn test_plugin_api_version() {
        assert_eq!(PLUGIN_API_VERSION, 1);
    }
}
