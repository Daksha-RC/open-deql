//! SeaORM entity for the `meta_inspections` projection table.
//! [REQ-033a]

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "meta_inspections")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub org_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub decision_name: String,
    pub input_output_json: Json,
    #[sea_orm(column_type = "Text")]
    pub full_sql: String,
    pub last_applied_id: i64,
    pub is_dropped: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
