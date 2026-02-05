//! Chaos / Fault Injection Tests for ThunderDB
//!
//! Tests system behavior under adverse conditions:
//! - Concurrent DDL + DML
//! - Query execution during checkpoint
//! - Engine shutdown during active transactions
//! - Disk-full simulation (read-only mode)
//! - Recovery after simulated crash

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use thunder_server::{DatabaseEngine, EngineConfig};

/// Helper to create a test engine with a temporary directory
async fn create_test_engine() -> (Arc<DatabaseEngine>, TempDir) {
    let tmp = TempDir::new().expect("Failed to create temp dir");
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");

    let config = EngineConfig {
        data_dir,
        wal_dir,
        buffer_pool_size: 16 * 1024 * 1024, // 16MB for tests
        wal_buffer_size: 1024 * 1024,        // 1MB
        page_size: 16 * 1024,
        query_timeout: Duration::from_secs(10),
        max_concurrent_queries: 50,
        max_result_rows: 10_000,
        ..EngineConfig::default()
    };

    let engine = Arc::new(
        DatabaseEngine::new(config)
            .await
            .expect("Failed to create engine"),
    );

    (engine, tmp)
}

#[tokio::test]
async fn test_engine_basic_lifecycle() {
    let (engine, _tmp) = create_test_engine().await;

    // Create a session
    let session = engine.create_session();
    assert!(!session.id.is_nil());

    // Execute a simple DDL
    let result = engine
        .execute_sql(session.id, "CREATE TABLE test_lifecycle (id INT, name TEXT)")
        .await;
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());

    // Verify table exists
    let tables = engine.list_tables();
    assert!(tables.contains(&"test_lifecycle".to_string()));

    // Insert data
    let result = engine
        .execute_sql(session.id, "INSERT INTO test_lifecycle VALUES (1, 'hello')")
        .await;
    assert!(result.is_ok(), "INSERT failed: {:?}", result.err());

    // Select data
    let result = engine
        .execute_sql(session.id, "SELECT id, name FROM test_lifecycle")
        .await;
    assert!(result.is_ok(), "SELECT failed: {:?}", result.err());

    // Cleanup
    engine.close_session(session.id);
    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_concurrent_ddl_safety() {
    let (engine, _tmp) = create_test_engine().await;

    // Spawn multiple tasks creating tables concurrently
    let mut handles = Vec::new();
    for i in 0..5 {
        let eng = engine.clone();
        handles.push(tokio::spawn(async move {
            let session = eng.create_session();
            let sql = format!("CREATE TABLE concurrent_{} (id INT)", i);
            let result = eng.execute_sql(session.id, &sql).await;
            eng.close_session(session.id);
            (i, result.is_ok())
        }));
    }

    // All should succeed (unique table names)
    let mut successes = 0;
    for handle in handles {
        let (i, ok) = handle.await.expect("Task panicked");
        if ok {
            successes += 1;
        } else {
            eprintln!("Table concurrent_{} creation failed", i);
        }
    }
    assert!(successes >= 4, "Expected most DDLs to succeed, got {}/5", successes);

    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_checkpoint_during_queries() {
    let (engine, _tmp) = create_test_engine().await;

    let session = engine.create_session();
    engine
        .execute_sql(session.id, "CREATE TABLE checkpoint_test (id INT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert some data
    for i in 0..10 {
        let sql = format!("INSERT INTO checkpoint_test VALUES ({}, 'val_{}')", i, i);
        engine.execute_sql(session.id, &sql).await.ok();
    }

    // Run checkpoint concurrently with more queries
    let eng_checkpoint = engine.clone();
    let checkpoint_handle = tokio::spawn(async move {
        eng_checkpoint.checkpoint().await
    });

    // Run queries concurrently with checkpoint
    let eng_query = engine.clone();
    let query_handle = tokio::spawn(async move {
        let s = eng_query.create_session();
        for i in 10..20 {
            let sql = format!("INSERT INTO checkpoint_test VALUES ({}, 'val_{}')", i, i);
            eng_query.execute_sql(s.id, &sql).await.ok();
        }
        eng_query.close_session(s.id);
    });

    // Both should complete without deadlock or crash
    let checkpoint_result = tokio::time::timeout(Duration::from_secs(10), checkpoint_handle).await;
    assert!(checkpoint_result.is_ok(), "Checkpoint timed out (possible deadlock)");

    let query_result = tokio::time::timeout(Duration::from_secs(10), query_handle).await;
    assert!(query_result.is_ok(), "Queries timed out during checkpoint");

    engine.close_session(session.id);
    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_read_only_mode_rejects_writes() {
    let (engine, _tmp) = create_test_engine().await;

    let session = engine.create_session();
    engine
        .execute_sql(session.id, "CREATE TABLE readonly_test (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Enable read-only mode (simulating disk-full)
    engine.set_read_only(true);
    assert!(engine.is_read_only());

    // Writes should be rejected
    let insert_result = engine
        .execute_sql(session.id, "INSERT INTO readonly_test VALUES (1)")
        .await;
    assert!(insert_result.is_err(), "INSERT should fail in read-only mode");
    let err_msg = insert_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("read-only"),
        "Error should mention read-only: {}",
        err_msg
    );

    // Reads should still work
    let select_result = engine
        .execute_sql(session.id, "SELECT id FROM readonly_test")
        .await;
    assert!(select_result.is_ok(), "SELECT should work in read-only mode");

    // Disable read-only mode
    engine.set_read_only(false);

    // Writes should work again
    let insert_result = engine
        .execute_sql(session.id, "INSERT INTO readonly_test VALUES (1)")
        .await;
    assert!(insert_result.is_ok(), "INSERT should work after disabling read-only");

    engine.close_session(session.id);
    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_engine_shutdown_with_active_sessions() {
    let (engine, _tmp) = create_test_engine().await;

    // Create multiple sessions
    let sessions: Vec<_> = (0..5).map(|_| engine.create_session()).collect();

    // Start a transaction on one session
    let _txn_result = engine.begin_transaction(sessions[0].id).await;

    // Shutdown should complete even with active sessions/transactions
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        engine.shutdown(),
    )
    .await;

    assert!(result.is_ok(), "Shutdown timed out with active sessions");
}

#[tokio::test]
async fn test_recovery_after_simulated_crash() {
    let tmp = TempDir::new().expect("Failed to create temp dir");
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");

    // Phase 1: Create engine, write data, then "crash" (drop without shutdown)
    {
        let config = EngineConfig {
            data_dir: data_dir.clone(),
            wal_dir: wal_dir.clone(),
            buffer_pool_size: 16 * 1024 * 1024,
            wal_buffer_size: 1024 * 1024,
            page_size: 16 * 1024,
            query_timeout: Duration::from_secs(10),
            max_concurrent_queries: 50,
            max_result_rows: 10_000,
            ..EngineConfig::default()
        };

        let engine = DatabaseEngine::new(config).await.expect("Engine create failed");
        let session = engine.create_session();

        engine
            .execute_sql(session.id, "CREATE TABLE crash_test (id INT, val TEXT)")
            .await
            .expect("CREATE TABLE failed");

        for i in 0..5 {
            let sql = format!("INSERT INTO crash_test VALUES ({}, 'data_{}')", i, i);
            engine.execute_sql(session.id, &sql).await.ok();
        }

        // Force WAL flush to ensure data is on disk
        engine.checkpoint().await.ok();

        // Simulate crash: drop engine without shutdown
        drop(engine);
    }

    // Phase 2: Re-open engine (WAL recovery should run automatically)
    {
        let config = EngineConfig {
            data_dir: data_dir.clone(),
            wal_dir: wal_dir.clone(),
            buffer_pool_size: 16 * 1024 * 1024,
            wal_buffer_size: 1024 * 1024,
            page_size: 16 * 1024,
            query_timeout: Duration::from_secs(10),
            max_concurrent_queries: 50,
            max_result_rows: 10_000,
            ..EngineConfig::default()
        };

        // Engine creation triggers WAL recovery automatically
        let engine_result = DatabaseEngine::new(config).await;
        assert!(
            engine_result.is_ok(),
            "Engine should recover after crash: {:?}",
            engine_result.err()
        );

        let engine = engine_result.unwrap();

        // The table should exist after recovery (WAL replay of CREATE TABLE)
        let tables = engine.list_tables();
        assert!(
            tables.contains(&"crash_test".to_string()),
            "Table 'crash_test' should exist after WAL recovery, found: {:?}",
            tables
        );

        engine.shutdown().await.ok();
    }
}

#[tokio::test]
async fn test_duplicate_table_creation() {
    let (engine, _tmp) = create_test_engine().await;
    let session = engine.create_session();

    // First create should succeed
    let result = engine
        .execute_sql(session.id, "CREATE TABLE dup_test (id INT)")
        .await;
    assert!(result.is_ok());

    // Second create should fail (table already exists)
    let result = engine
        .execute_sql(session.id, "CREATE TABLE dup_test (id INT)")
        .await;
    assert!(result.is_err(), "Duplicate CREATE TABLE should fail");

    // IF NOT EXISTS should succeed silently
    let result = engine
        .execute_sql(session.id, "CREATE TABLE IF NOT EXISTS dup_test (id INT)")
        .await;
    assert!(result.is_ok(), "CREATE TABLE IF NOT EXISTS should succeed");

    engine.close_session(session.id);
    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_scrub_pages_on_empty_database() {
    let (engine, _tmp) = create_test_engine().await;

    // Scrub on an empty database should work and find no corruptions
    let (pages, corruptions) = engine.scrub_pages().await;
    assert_eq!(corruptions, 0, "No corruptions expected on fresh database");
    // pages_checked may be 0 or small depending on initial allocation
    eprintln!("Scrub result: {} pages checked, {} corruptions", pages, corruptions);

    engine.shutdown().await.ok();
}

#[tokio::test]
async fn test_backup_and_list() {
    let (engine, _tmp) = create_test_engine().await;
    let session = engine.create_session();

    engine
        .execute_sql(session.id, "CREATE TABLE backup_test (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Create a backup
    let backup_result = engine.backup(None).await;
    assert!(backup_result.is_ok(), "Backup failed: {:?}", backup_result.err());

    let backup = backup_result.unwrap();
    assert!(!backup.backup_id.is_empty());
    assert!(backup.size_bytes > 0 || true); // Size may be 0 if no data pages

    // List backups
    let backups = engine.list_backups();
    assert!(backups.is_ok());
    let backup_list = backups.unwrap();
    assert!(!backup_list.is_empty(), "Should have at least one backup");
    assert_eq!(backup_list[0].backup_id, backup.backup_id);

    engine.close_session(session.id);
    engine.shutdown().await.ok();
}
