//! SeaORM entity for the `meta_decisions` projection table.
//! [REQ-032]

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "meta_decisions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub org_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub aggregate: String,
    pub command: String,
    pub emits_json: Json,
    pub has_guard: bool,
    #[sea_orm(column_type = "Text", nullable)]
    pub guard_sql: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub state_sql: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub full_sql: String,
    pub last_applied_id: i64,
    pub is_dropped: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
