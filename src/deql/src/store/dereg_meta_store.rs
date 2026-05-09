//! SeaORM entity for the `dereg_meta_store` table.
//!
//! Canonical append-only audit log of all DeQL concept registrations.
//! [REQ-028]

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "dereg_meta_store")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i64,
    pub org_id: String,
    pub stream_id: String,
    pub event_type: String,
    pub concept_type: String,
    pub concept_key: i64,
    pub occurred_at: DateTimeWithTimeZone,
    pub status: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub statement: String,
    pub meta: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
