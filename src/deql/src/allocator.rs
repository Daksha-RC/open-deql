//! Sequence allocators for `dereg_meta_store`.
//!
//! - Global `id` allocator: single monotonic sequence across all rows [REQ-025]
//! - Per-entity `concept_key` allocator: monotonic per (org_id, stream_id) [REQ-022d]

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbErr, FromQueryResult, Statement, TransactionTrait,
};

/// Allocate the next global monotonic `id` for `dereg_meta_store`.
///
/// Uses `SELECT MAX(id) + 1` within a transaction to ensure monotonicity.
/// Falls back to 1 if the table is empty.
/// [REQ-025] Single stream across all rows (all orgs).
pub async fn allocate_next_id(db: &DatabaseConnection) -> Result<i64, DbErr> {
    let txn = db.begin().await?;
    let id = allocate_next_id_txn(&txn).await?;
    txn.commit().await?;
    Ok(id)
}

/// Allocate next id within an existing transaction.
pub async fn allocate_next_id_txn<C: ConnectionTrait>(conn: &C) -> Result<i64, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MaxId {
        max_id: Option<i64>,
    }

    let result = MaxId::find_by_statement(Statement::from_string(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT MAX(id) as max_id FROM dereg_meta_store",
    ))
    .one(conn)
    .await?;

    Ok(result.and_then(|r| r.max_id).unwrap_or(0) + 1)
}

/// Allocate the next `concept_key` for a given `(org_id, stream_id)`.
///
/// Monotonic per entity — increments on all attempts including failures.
/// [REQ-022d] [REQ-027]
pub async fn allocate_concept_key_txn<C: ConnectionTrait>(
    conn: &C,
    org_id: &str,
    stream_id: &str,
) -> Result<i64, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MaxKey {
        max_key: Option<i64>,
    }

    let result = MaxKey::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT MAX(concept_key) as max_key FROM dereg_meta_store WHERE org_id = $1 AND stream_id = $2",
        [org_id.into(), stream_id.into()],
    ))
    .one(conn)
    .await?;

    Ok(result.and_then(|r| r.max_key).unwrap_or(0) + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;
    use sea_orm_migration::MigratorTrait;

    use crate::migration::DeqlMigrator;

    async fn setup_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        DeqlMigrator::up(&db, None).await.unwrap();
        db
    }

    #[tokio::test]
    async fn allocate_next_id_starts_at_1() {
        let db = setup_db().await;
        let id = allocate_next_id(&db).await.unwrap();
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn allocate_next_id_increments() {
        let db = setup_db().await;

        // Insert a row with id=5
        db.execute(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO dereg_meta_store (id, org_id, stream_id, event_type, concept_type, concept_key, occurred_at, status, statement, meta) VALUES (5, 'org1', 's1', 'AggregateCreated', 'AGGREGATE', 1, '2026-01-01T00:00:00Z', 'ok', 'CREATE AGGREGATE X', '{}')",
        ))
        .await
        .unwrap();

        let id = allocate_next_id(&db).await.unwrap();
        assert_eq!(id, 6);
    }

    #[tokio::test]
    async fn allocate_next_id_strictly_increasing_sequential() {
        let db = setup_db().await;

        let txn = db.begin().await.unwrap();
        let id1 = allocate_next_id_txn(&txn).await.unwrap();
        assert_eq!(id1, 1);

        // Insert with that id
        txn.execute(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            format!("INSERT INTO dereg_meta_store (id, org_id, stream_id, event_type, concept_type, concept_key, occurred_at, status, statement, meta) VALUES ({}, 'org1', 's1', 'AggregateCreated', 'AGGREGATE', 1, '2026-01-01T00:00:00Z', 'ok', 'X', '{{}}')", id1),
        ))
        .await
        .unwrap();

        let id2 = allocate_next_id_txn(&txn).await.unwrap();
        assert_eq!(id2, 2);
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn concept_key_starts_at_1() {
        let db = setup_db().await;
        let txn = db.begin().await.unwrap();
        let key = allocate_concept_key_txn(&txn, "org1", "aggregate:BankAccount")
            .await
            .unwrap();
        assert_eq!(key, 1);
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn concept_key_increments_per_stream() {
        let db = setup_db().await;

        // Insert row for org1/aggregate:BankAccount with concept_key=2
        db.execute(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO dereg_meta_store (id, org_id, stream_id, event_type, concept_type, concept_key, occurred_at, status, statement, meta) VALUES (1, 'org1', 'aggregate:BankAccount', 'AggregateCreated', 'AGGREGATE', 2, '2026-01-01T00:00:00Z', 'ok', 'X', '{}')",
        ))
        .await
        .unwrap();

        let txn = db.begin().await.unwrap();
        let key = allocate_concept_key_txn(&txn, "org1", "aggregate:BankAccount")
            .await
            .unwrap();
        assert_eq!(key, 3);

        // Different stream_id starts at 1
        let key2 = allocate_concept_key_txn(&txn, "org1", "decision:HandleWithdraw")
            .await
            .unwrap();
        assert_eq!(key2, 1);
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn concept_key_isolated_across_orgs() {
        let db = setup_db().await;

        // Insert for org1
        db.execute(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO dereg_meta_store (id, org_id, stream_id, event_type, concept_type, concept_key, occurred_at, status, statement, meta) VALUES (1, 'org1', 'aggregate:X', 'AggregateCreated', 'AGGREGATE', 3, '2026-01-01T00:00:00Z', 'ok', 'X', '{}')",
        ))
        .await
        .unwrap();

        let txn = db.begin().await.unwrap();
        // org2 same stream_id starts at 1
        let key = allocate_concept_key_txn(&txn, "org2", "aggregate:X")
            .await
            .unwrap();
        assert_eq!(key, 1);
        txn.commit().await.unwrap();
    }
}
