//! ThunderDB Server
//!
//! Main entry point for the ThunderDB database server.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use thunder_api::AppState;
use thunder_cluster::{
    ClusterCoordinator, CoordinatorConfig, DistributedCatalog, GossipConfig, GrpcTransport,
    TransportConfig,
};
use thunder_common::config::ServerConfig;
use thunder_common::prelude::NodeId;
use thunder_server::{DatabaseEngine, EngineConfig, WorkerConfig, WorkerManager};
use tracing::info;
use tracing_subscriber::prelude::*;

// Use jemalloc as the global allocator for better performance
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// ThunderDB - A high-performance distributed HTAP database
#[derive(Parser, Debug)]
#[command(name = "thunderdb")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/thunderdb.toml")]
    config: PathBuf,

    /// Override listen address
    #[arg(long)]
    listen: Option<String>,

    /// Override data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Hash a password and print the result, then exit.
    /// Use this to generate values for `superuser_password_hash` in config.
    #[arg(long, value_name = "PASSWORD")]
    hash_password: Option<String>,

    /// Restore from a backup to a specific point in time (RFC 3339 timestamp), then exit.
    /// Example: --restore-pitr "2025-01-15T12:30:00Z" --restore-backup /path/to/backup
    #[arg(long, value_name = "TIMESTAMP")]
    restore_pitr: Option<String>,

    /// Path to the backup directory for PITR restore.
    #[arg(long, value_name = "PATH")]
    restore_backup: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // --hash-password: print hash and exit immediately
    if let Some(ref password) = args.hash_password {
        use thunder_common::config::SecurityConfig;
        let hash = SecurityConfig::hash_password(password);
        println!("{}", hash);
        return Ok(());
    }

    // --restore-pitr: restore from backup to a specific timestamp and exit
    if let Some(ref pitr_timestamp) = args.restore_pitr {
        let backup_path = args.restore_backup.as_ref()
            .ok_or_else(|| anyhow::anyhow!(
                "--restore-pitr requires --restore-backup to specify the backup directory"
            ))?;

        // Parse the timestamp
        let target_time: chrono::DateTime<chrono::Utc> = pitr_timestamp.parse()
            .map_err(|e| anyhow::anyhow!(
                "Invalid PITR timestamp '{}': {}. Use RFC 3339 format (e.g. 2025-01-15T12:30:00Z)",
                pitr_timestamp, e
            ))?;
        let target_ms = target_time.timestamp_millis() as u64;

        // Determine data and WAL dirs from config or defaults
        let config = load_config(&args).unwrap_or_default();
        let data_dir = args.data_dir.as_ref().unwrap_or(&config.data_dir);
        let wal_dir = &config.wal_dir;

        println!(
            "Restoring from backup {:?} to timestamp {} ({} ms)...",
            backup_path, pitr_timestamp, target_ms
        );

        DatabaseEngine::restore_to_timestamp(backup_path, data_dir, wal_dir, target_ms).await?;

        println!("PITR restore complete. You can now start the server normally.");
        return Ok(());
    }

    // Load configuration first (needed for logging setup)
    let mut config = load_config(&args)?;

    // Resolve plaintext superuser_password → superuser_password_hash
    config.security.resolve_password();

    // Initialize logging with config-driven levels, format, and optional file rotation
    let log_level = if args.verbose { "debug" } else { &config.logging.level };
    let env_filter = tracing_subscriber::filter::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::filter::EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| tracing_subscriber::filter::EnvFilter::new("info"));

    let use_json = config.logging.format == "json";

    // Build console layer (boxed to unify types)
    let console_layer: Box<dyn tracing_subscriber::Layer<_> + Send + Sync> = if use_json {
        Box::new(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
    } else {
        Box::new(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        )
    };

    // Build optional file layer with daily rotation
    let file_layer: Option<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>> =
        if let Some(ref log_file) = config.logging.file {
            let file_appender = tracing_appender::rolling::daily(
                log_file.parent().unwrap_or_else(|| std::path::Path::new(".")),
                log_file.file_name().unwrap_or_else(|| std::ffi::OsStr::new("thunderdb.log")),
            );
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            // Leak the guard so file logging stays active for the process lifetime
            std::mem::forget(guard);

            if use_json {
                Some(Box::new(
                    tracing_subscriber::fmt::layer()
                        .with_writer(non_blocking)
                        .with_ansi(false)
                        .json(),
                ))
            } else {
                Some(Box::new(
                    tracing_subscriber::fmt::layer()
                        .with_writer(non_blocking)
                        .with_ansi(false),
                ))
            }
        } else {
            None
        };

    // Optional OpenTelemetry layer
    let otel_layer: Option<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>> =
        if let Some(ref otel_endpoint) = config.logging.otel_endpoint {
            #[cfg(feature = "otel")]
            {
                use opentelemetry::trace::TracerProvider;
                use opentelemetry_otlp::WithExportConfig;

                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(otel_endpoint)
                    .build()
                    .expect("Failed to build OTLP span exporter");

                let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .build();

                let tracer = provider.tracer(config.logging.otel_service_name.clone());
                let layer = tracing_opentelemetry::layer().with_tracer(tracer);
                tracing::info!("OpenTelemetry tracing enabled, exporting to {}", otel_endpoint);
                Some(Box::new(layer))
            }
            #[cfg(not(feature = "otel"))]
            {
                tracing::warn!(
                    "OpenTelemetry endpoint configured ({}) but 'otel' feature is not enabled. \
                     Build with `--features otel` to enable OTLP export.",
                    otel_endpoint
                );
                None
            }
        } else {
            None
        };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(console_layer)
        .with(file_layer)
        .with(otel_layer)
        .init();

    // Print banner
    print_banner();

    info!("Loading configuration from {:?}", args.config);

    // Validate configuration
    if let Err(errors) = config.validate() {
        for err in &errors {
            tracing::error!("Config validation error: {}", err);
        }
        anyhow::bail!(
            "Configuration validation failed with {} error(s). \
             See log output above for details.",
            errors.len()
        );
    }

    info!("Starting ThunderDB server...");
    info!("Node ID: {}", config.node_id);
    info!("Data directory: {:?}", config.data_dir);
    info!("WAL directory: {:?}", config.wal_dir);

    // Create data directories if they don't exist
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.wal_dir).await?;

    // Initialize database engine
    info!("Initializing database engine...");
    let engine_config = EngineConfig::from(&config);
    let engine = Arc::new(DatabaseEngine::new(engine_config).await?);

    // Start background workers (vacuum, checkpoint, disk monitor)
    info!("Starting background workers...");
    let worker_config = WorkerConfig {
        checkpoint_interval: config.storage.checkpoint_interval,
        disk_check_interval: config.storage.disk_check_interval,
        disk_warn_percent: config.storage.disk_space_warn_percent,
        disk_critical_percent: config.storage.disk_space_critical_percent,
        backup_enabled: config.storage.backup_enabled,
        backup_interval: config.storage.backup_interval,
        backup_retention_count: config.storage.backup_retention_count,
        backup_dir: config.storage.backup_dir.clone(),
        ..WorkerConfig::default()
    };
    let worker_manager = Arc::new(WorkerManager::new(engine.clone(), worker_config));
    let _worker_handles = worker_manager.start();

    // Initialize cluster if peers or seeds are configured
    let cluster_coordinator: Option<Arc<ClusterCoordinator>> =
        if !config.cluster.peers.is_empty() || !config.cluster.seeds.is_empty() || config.cluster.is_seed {
            info!("Initializing cluster mode...");
            info!("Cluster name: {}", config.cluster.cluster_name);

            let grpc_addr: SocketAddr = format!("{}:{}", config.listen_addr, config.grpc_port)
                .parse()
                .expect("Invalid gRPC address");

            // Parse seed addresses
            let seeds: Vec<SocketAddr> = config
                .cluster
                .seeds
                .iter()
                .chain(config.cluster.peers.iter()) // Also include peers as potential seeds
                .filter_map(|s| s.parse().ok())
                .collect();

            info!("gRPC address: {}", grpc_addr);
            info!("Seeds: {:?}", seeds);

            // Create transport
            let transport = Arc::new(GrpcTransport::new(
                NodeId(config.node_id),
                TransportConfig::default(),
            ));

            // Create coordinator config
            let coordinator_config = CoordinatorConfig {
                node_id: NodeId(config.node_id),
                addr: grpc_addr,
                cluster_name: config.cluster.cluster_name.clone(),
                seeds: seeds.clone(),
                gossip_config: GossipConfig {
                    gossip_interval: config.cluster.gossip_interval,
                    phi_threshold: config.cluster.phi_threshold,
                    is_seed: config.cluster.is_seed,
                    ..GossipConfig::default()
                },
                target_replicas: config.cluster.replication_factor,
                ..CoordinatorConfig::default()
            };

            let coordinator = Arc::new(ClusterCoordinator::new(coordinator_config, transport));

            // Bootstrap or join the cluster
            match coordinator.bootstrap().await {
                Ok(()) => {
                    info!("Cluster bootstrap/join successful");

                    // Start coordinator background tasks
                    if let Err(e) = coordinator.start().await {
                        tracing::error!("Failed to start cluster coordinator: {}", e);
                    } else {
                        info!("Cluster coordinator started");

                        // Log discovered nodes if using gossip
                        if let Some(gossiper) = coordinator.gossiper() {
                            let nodes = gossiper.discovered_nodes();
                            if !nodes.is_empty() {
                                info!("Discovered {} cluster nodes via gossip:", nodes.len());
                                for (node_id, addr) in &nodes {
                                    info!("  - Node {:?} at {}", node_id, addr);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to bootstrap cluster: {}", e);
                    tracing::warn!("Continuing in standalone mode");
                }
            }

            Some(coordinator)
        } else {
            info!("Running in standalone mode (no cluster configured)");
            None
        };

    // Wire cluster coordinator and distributed catalog to engine
    if let Some(ref coordinator) = cluster_coordinator {
        if let Some(dist_catalog) = coordinator.distributed_catalog() {
            info!("Wiring distributed catalog to engine");
            engine.set_cluster(coordinator.clone(), dist_catalog.clone());

            // Register existing local tables in the distributed catalog
            // (tables recovered from WAL are already in sql_catalog)
        }
    }

    // Start gRPC server for cluster communication if cluster is enabled
    if let Some(ref coordinator) = cluster_coordinator {
        let grpc_addr: SocketAddr = format!("{}:{}", config.listen_addr, config.grpc_port)
            .parse()
            .expect("Invalid gRPC address");

        info!("Starting cluster gRPC server on {}", grpc_addr);

        // Build ClusterServer with engine for forwarded queries
        let engine_for_grpc: Arc<dyn thunder_cluster::QueryExecutor> = engine.clone();
        let cluster_transport = thunder_cluster::ClusterTransport::new(
            coordinator.node_id(),
            grpc_addr,
            config.cluster.cluster_name.clone(),
            TransportConfig::default(),
        );
        let transport_arc = Arc::new(cluster_transport);
        let mut cluster_server = thunder_cluster::ClusterServer::new(
            coordinator.node_id(),
            config.cluster.cluster_name.clone(),
            transport_arc.get_raft_sender(),
            None,
            transport_arc,
        );
        cluster_server.set_engine(engine_for_grpc);

        // Spawn gRPC server
        let svc = thunder_cluster::generated::cluster_service_server::ClusterServiceServer::new(
            cluster_server,
        );
        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(svc)
                .serve(grpc_addr)
                .await
            {
                tracing::error!("gRPC cluster server failed: {}", e);
            }
        });

        info!("Cluster gRPC server started");
    }

    // Build addresses
    let pg_addr = format!("{}:{}", config.listen_addr, config.pg_port);
    let mysql_addr = format!("{}:{}", config.listen_addr, config.mysql_port);
    let resp_addr = format!("{}:{}", config.listen_addr, config.resp_port);
    let http_addr = format!("{}:{}", config.listen_addr, config.http_port);

    info!("PostgreSQL protocol: {}", pg_addr);
    info!("MySQL protocol: {}", mysql_addr);
    info!("RESP protocol: {}", resp_addr);
    info!("HTTP API: {}", http_addr);
    info!("Dashboard: http://{}/dashboard", http_addr);

    // Start HTTP API server with engine connected
    let app_state = AppState::with_engine(engine.clone());
    let rest_router = thunder_api::create_rest_router(app_state.clone());
    let dashboard_router = thunder_api::dashboard_router(app_state.clone());

    // Merge REST API and Dashboard routes
    let app = rest_router.merge(dashboard_router);
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;

    // Start protocol servers
    let pg_engine = engine.clone();
    let pg_listener = tokio::net::TcpListener::bind(&pg_addr).await?;
    info!("PostgreSQL protocol: {}", pg_addr);

    let mysql_engine = engine.clone();
    let mysql_listener = tokio::net::TcpListener::bind(&mysql_addr).await?;
    info!("MySQL protocol: {}", mysql_addr);

    // RESP uses InMemoryStore for Redis-compatible key-value operations
    let resp_store = Arc::new(thunder_protocol::InMemoryStore::new());
    let resp_listener = tokio::net::TcpListener::bind(&resp_addr).await?;
    info!("RESP protocol: {}", resp_addr);

    // Spawn protocol server tasks with cancellation support
    let protocol_cancel = tokio_util::sync::CancellationToken::new();

    let pg_config = thunder_protocol::ProtocolConfig {
        listen_addr: config.listen_addr.clone(), port: config.pg_port,
        max_connections: 1000, auth_method: thunder_protocol::AuthMethod::Trust, tls: None,
        auth_rules: config.security.auth_rules.clone(),
    };
    let pg_handler = Arc::new(thunder_protocol::PostgresHandler::new(pg_engine, pg_config.clone()));
    let pg_handler_for_reload = pg_handler.clone();

    let pg_cancel = protocol_cancel.clone();
    tokio::spawn(thunder_protocol::postgres::serve_handler_with_cancel(
        pg_listener, pg_handler, pg_config, pg_cancel,
    ));

    let mysql_cancel = protocol_cancel.clone();
    tokio::spawn(thunder_protocol::mysql::serve_with_cancel(
        mysql_listener, mysql_engine,
        thunder_protocol::ProtocolConfig {
            listen_addr: config.listen_addr.clone(), port: config.mysql_port,
            max_connections: 1000, auth_method: thunder_protocol::AuthMethod::Trust, tls: None,
            auth_rules: config.security.auth_rules.clone(),
        },
        mysql_cancel,
    ));

    let resp_cancel = protocol_cancel.clone();
    tokio::spawn(thunder_protocol::resp::serve_with_cancel(
        resp_listener, resp_store, 10000, resp_cancel,
    ));

    info!("ThunderDB is ready to accept connections!");

    // SIGHUP handler for config hot-reload
    #[cfg(unix)]
    {
        let reload_config_path = args.config.clone();
        let pg_handler_reload = pg_handler_for_reload.clone();
        tokio::spawn(async move {
            let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("Failed to listen for SIGHUP");
            loop {
                sighup.recv().await;
                info!("Received SIGHUP, reloading configuration...");
                match std::fs::read_to_string(&reload_config_path)
                    .map_err(|e| e.to_string())
                    .and_then(|content| toml::from_str::<ServerConfig>(&content).map_err(|e| e.to_string()))
                {
                    Ok(new_config) => {
                        // Only reload mutable settings (log level, rate limits, timeouts)
                        // Non-reloadable settings (ports, data_dir) require restart
                        info!("Configuration reloaded successfully");
                        info!("  log level: {}", new_config.logging.level);
                        info!("  query_timeout: {:?}", new_config.query_timeout);
                        info!("  Note: port/data_dir changes require restart");

                        // Update tracing filter dynamically if possible
                        // (Full dynamic filter reload requires tracing_subscriber::reload,
                        //  which would need upfront setup. Log the new level for now.)
                    }
                    Err(e) => {
                        tracing::error!("Failed to reload configuration: {}", e);
                    }
                }

                // Hot-reload TLS certificates for PostgreSQL protocol
                match pg_handler_reload.reload_tls() {
                    Ok(()) => {
                        if pg_handler_reload.tls_available() {
                            info!("PostgreSQL TLS certificates reloaded via SIGHUP");
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to reload PostgreSQL TLS certificates: {}. \
                             Previous certificates remain active.",
                            e
                        );
                    }
                }
            }
        });
    }

    // Graceful shutdown handler
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let engine_shutdown = engine.clone();
    let worker_manager_shutdown = worker_manager.clone();
    let cluster_shutdown = cluster_coordinator.clone();

    tokio::spawn(async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to listen for SIGTERM")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("Received SIGINT, initiating graceful shutdown..."),
            _ = terminate => info!("Received SIGTERM, initiating graceful shutdown..."),
        }

        // Stop accepting new protocol connections
        protocol_cancel.cancel();
        // Brief pause to let in-flight connections finish
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Shutdown cluster coordinator
        if let Some(coordinator) = cluster_shutdown {
            info!("Shutting down cluster coordinator...");
            if let Err(e) = coordinator.stop().await {
                tracing::error!("Error during cluster shutdown: {}", e);
            }
        }

        // Shutdown background workers (they'll write final checkpoint)
        worker_manager_shutdown.shutdown().await;

        // Then shutdown the engine
        if let Err(e) = engine_shutdown.shutdown().await {
            tracing::error!("Error during engine shutdown: {}", e);
        }

        let _ = shutdown_tx.send(());
    });

    // Run the HTTP server (with optional TLS)
    if config.security.tls_enabled {
        let cert_path = config.security.tls_cert_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but tls_cert_path not configured"))?;
        let key_path = config.security.tls_key_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but tls_key_path not configured"))?;

        let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            cert_path, key_path,
        ).await?;

        info!("Starting HTTPS server with TLS on {}", http_addr);
        let addr: std::net::SocketAddr = http_addr.parse()?;
        let handle = axum_server::Handle::new();
        let handle_shutdown = handle.clone();

        tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            handle_shutdown.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });

        axum_server::bind_rustls(addr, rustls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await?;
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await?;
    }

    info!("ThunderDB server stopped");
    Ok(())
}

fn print_banner() {
    println!(r#"
  _____ _                     _           ____  ____
 |_   _| |__  _   _ _ __   __| | ___ _ __|  _ \| __ )
   | | | '_ \| | | | '_ \ / _` |/ _ \ '__| | | |  _ \
   | | | | | | |_| | | | | (_| |  __/ |  | |_| | |_) |
   |_| |_| |_|\__,_|_| |_|\__,_|\___|_|  |____/|____/

  High-Performance Distributed HTAP Database
  Version: {}
"#, env!("CARGO_PKG_VERSION"));
}

fn load_config(args: &Args) -> anyhow::Result<ServerConfig> {
    let mut config = if args.config.exists() {
        let content = std::fs::read_to_string(&args.config)?;
        toml::from_str(&content)?
    } else {
        ServerConfig::default()
    };

    // Apply command line overrides
    if let Some(listen) = &args.listen {
        config.listen_addr = listen.clone();
    }
    if let Some(data_dir) = &args.data_dir {
        config.data_dir = data_dir.clone();
    }

    // Apply environment variable overrides (highest priority)
    if let Ok(val) = std::env::var("THUNDERDB_LISTEN_ADDR") {
        config.listen_addr = val;
    }
    if let Ok(val) = std::env::var("THUNDERDB_HTTP_PORT") {
        if let Ok(port) = val.parse() { config.http_port = port; }
    }
    if let Ok(val) = std::env::var("THUNDERDB_PG_PORT") {
        if let Ok(port) = val.parse() { config.pg_port = port; }
    }
    if let Ok(val) = std::env::var("THUNDERDB_MYSQL_PORT") {
        if let Ok(port) = val.parse() { config.mysql_port = port; }
    }
    if let Ok(val) = std::env::var("THUNDERDB_RESP_PORT") {
        if let Ok(port) = val.parse() { config.resp_port = port; }
    }
    if let Ok(val) = std::env::var("THUNDERDB_DATA_DIR") {
        config.data_dir = PathBuf::from(val);
    }
    if let Ok(val) = std::env::var("THUNDERDB_WAL_DIR") {
        config.wal_dir = PathBuf::from(val);
    }
    if let Ok(val) = std::env::var("THUNDERDB_LOG_LEVEL") {
        config.logging.level = val;
    }
    if let Ok(val) = std::env::var("THUNDERDB_TLS_ENABLED") {
        if let Ok(enabled) = val.parse() { config.security.tls_enabled = enabled; }
    }
    if let Ok(val) = std::env::var("THUNDERDB_TLS_CERT") {
        config.security.tls_cert_path = Some(PathBuf::from(val));
    }
    if let Ok(val) = std::env::var("THUNDERDB_TLS_KEY") {
        config.security.tls_key_path = Some(PathBuf::from(val));
    }
    if let Ok(val) = std::env::var("THUNDERDB_SUPERUSER_PASSWORD_HASH") {
        config.security.superuser_password_hash = Some(val);
    }
    if let Ok(val) = std::env::var("THUNDERDB_AUTH_ENABLED") {
        if let Ok(enabled) = val.parse() {
            config.security.authentication_enabled = enabled;
        }
    }

    // Cluster configuration overrides
    if let Ok(val) = std::env::var("THUNDERDB_NODE_ID") {
        if let Ok(id) = val.parse() {
            config.node_id = id;
        }
    }
    if let Ok(val) = std::env::var("THUNDERDB_CLUSTER_NAME") {
        config.cluster.cluster_name = val;
    }
    if let Ok(val) = std::env::var("THUNDERDB_SEEDS") {
        // Comma-separated list of seed addresses
        config.cluster.seeds = val.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(val) = std::env::var("THUNDERDB_IS_SEED") {
        if let Ok(is_seed) = val.parse() {
            config.cluster.is_seed = is_seed;
        }
    }
    if let Ok(val) = std::env::var("THUNDERDB_GRPC_PORT") {
        if let Ok(port) = val.parse() {
            config.grpc_port = port;
        }
    }

    Ok(config)
}
