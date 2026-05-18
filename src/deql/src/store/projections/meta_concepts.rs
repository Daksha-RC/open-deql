//! SeaORM entity for the `meta_concepts` projection table.
//! [REQ-030]

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "meta_concepts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub org_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub kind: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub json_source: Json,
    pub last_applied_id: i64,
    pub is_dropped: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
