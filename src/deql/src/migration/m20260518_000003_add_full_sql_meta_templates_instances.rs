use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MetaTemplatesInstances::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(MetaTemplatesInstances::FullSql)
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MetaTemplatesInstances::Table)
                    .drop_column(MetaTemplatesInstances::FullSql)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum MetaTemplatesInstances {
    Table,
    FullSql,
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigrationName;

    use super::*;

    #[test]
    fn test_migration_name() {
        assert_eq!(
            Migration.name(),
            "m20260518_000003_add_full_sql_meta_templates_instances"
        );
    }
}
