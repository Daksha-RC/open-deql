//! DeQL startup recovery — rehydrates in-memory DeReg state and registers
//! virtual stream schemas on server startup.
//!
//! Gated behind `#[cfg(feature = "deql")]`.

use std::sync::Arc;

/// Discover orgs with DeQL data and rehydrate their registries.
///
/// Runs as a background task spawned from `job::init_deferred()`.
/// Non-blocking: the server accepts traffic immediately; virtual streams
/// become available once recovery completes for each org.
pub async fn deql_startup_recovery() {
    use infra::db::{ORM_CLIENT, connect_to_orm};
    use o2_deql::store::dereg_meta_store;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};

    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    // 1. Query distinct org_ids that have DeQL audit rows
    let org_ids: Vec<String> = match dereg_meta_store::Entity::find()
        .select_only()
        .column(dereg_meta_store::Column::OrgId)
        .distinct()
        .filter(dereg_meta_store::Column::OrgId.is_not_null())
        .into_tuple::<String>()
        .all(db)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "DeQL startup recovery: failed to query orgs from dereg_meta_store, skipping"
            );
            return;
        }
    };

    if org_ids.is_empty() {
        tracing::debug!("DeQL startup recovery: no orgs with DeQL data found");
        return;
    }

    tracing::info!(
        org_count = org_ids.len(),
        orgs = ?org_ids,
        "DeQL startup recovery: rehydrating org(s)"
    );

    // 2. Rehydrate each org sequentially (avoids thundering herd on DB)
    for org_id in &org_ids {
        recover_org(org_id).await;
    }

    tracing::info!(
        org_count = org_ids.len(),
        "DeQL startup recovery: completed for all orgs"
    );
}

/// Rehydrate a single org and register its virtual stream schemas.
async fn recover_org(org_id: &str) {
    use infra::db::{ORM_CLIENT, connect_to_orm};
    use o2_deql::{RehydrateService, RehydrateServiceImpl};
    use crate::handler::http::request::dereg::get_deql_state;

    let state = get_deql_state().await;
    let db = Arc::new(ORM_CLIENT.get_or_init(connect_to_orm).await.clone());

    let service = RehydrateServiceImpl::new(
        db,
        Arc::new(state.org_map.clone_for_service()),
        Arc::new(state.lock_map.clone_for_service()),
        state.rehydrate_state_map.clone(),
    );

    let trace_id = format!("startup-recovery-{}", org_id);

    // Rehydrate with 120-second timeout per org
    match tokio::time::timeout(
        std::time::Duration::from_secs(120),
        service.rehydrate_org(org_id, None, Some(&trace_id)),
    )
    .await
    {
        Ok(Ok(result)) => {
            tracing::info!(
                org_id = %org_id,
                rows_processed = result.rows_processed,
                elapsed_ms = result.elapsed_ms,
                "DeQL startup recovery: rehydrate succeeded"
            );

            // Register virtual stream schemas (deql_agg_*, deql_prj_*)
            let org_dereg = state.org_map.get_or_init(org_id).await;
            let dereg = org_dereg.read().await;
            let agg_count = dereg.list_aggregate_names().len();
            let proj_count = dereg.list_projection_names().len();
            tracing::info!(
                org_id = %org_id,
                aggregates = agg_count,
                projections = proj_count,
                "DeQL startup recovery: registering virtual stream schemas"
            );
            if let Err(e) = dereg.register_stream_schema(org_id).await {
                tracing::error!(
                    org_id = %org_id,
                    error = ?e,
                    "DeQL startup recovery: virtual stream schema registration failed"
                );
            } else {
                tracing::info!(
                    org_id = %org_id,
                    aggregates = agg_count,
                    projections = proj_count,
                    "DeQL startup recovery: virtual stream schemas registered successfully"
                );
                // Directly update the in-memory schema cache so virtual streams
                // appear immediately in the UI without waiting for the watcher.
                update_schema_cache_for_virtual_streams(org_id, &dereg).await;
            }
        }
        Ok(Err(o2_deql::RehydrateError::InProgress)) => {
            tracing::info!(
                org_id = %org_id,
                "DeQL startup recovery: rehydrate already in progress, skipping"
            );
        }
        Ok(Err(e)) => {
            tracing::error!(
                org_id = %org_id,
                error = %e,
                "DeQL startup recovery: rehydrate failed"
            );
        }
        Err(_) => {
            tracing::error!(
                org_id = %org_id,
                "DeQL startup recovery: rehydrate timed out (120s)"
            );
        }
    }
}

/// Directly update the in-memory schema cache (`STREAM_SCHEMAS_LATEST`) for
/// virtual streams so they appear immediately in the Logs Explore dropdown.
///
/// Calls `infra::schema::get_cache()` for each virtual stream, which reads
/// from the DB and inserts into the cache if not already present.
async fn update_schema_cache_for_virtual_streams(
    org_id: &str,
    dereg: &o2_deql::dereg::DeReg,
) {
    use config::meta::stream::StreamType;
    use infra::schema::get_cache;

    // Collect all virtual stream names for this org
    let mut stream_names: Vec<String> = Vec::new();

    // deql_events
    stream_names.push("deql_events".to_string());

    // deql_agg_* streams
    for agg_name in dereg.list_aggregate_names() {
        stream_names.push(format!("deql_agg_{}", agg_name.to_lowercase()));
    }

    // deql_prj_* streams
    for proj_name in dereg.list_projection_names() {
        stream_names.push(format!("deql_prj_{}", proj_name.to_lowercase()));
    }

    // Warm the cache by reading each schema (get_cache reads from DB and inserts into cache)
    let mut cached_count = 0;
    for stream_name in &stream_names {
        match get_cache(org_id, stream_name, StreamType::Logs).await {
            Ok(schema) if !schema.schema().fields().is_empty() => {
                cached_count += 1;
            }
            Ok(_) => {
                tracing::debug!(
                    org_id = %org_id,
                    stream = %stream_name,
                    "DeQL startup recovery: schema empty for stream"
                );
            }
            Err(e) => {
                tracing::debug!(
                    org_id = %org_id,
                    stream = %stream_name,
                    error = ?e,
                    "DeQL startup recovery: failed to warm cache for stream"
                );
            }
        }
    }

    tracing::info!(
        org_id = %org_id,
        cached_count = cached_count,
        total_streams = stream_names.len(),
        "DeQL startup recovery: schema cache warmed for virtual streams"
    );
}
