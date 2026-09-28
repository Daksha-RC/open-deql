use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MetaAggregates::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(MetaAggregates::FullSql)
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
                    .table(MetaAggregates::Table)
                    .drop_column(MetaAggregates::FullSql)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum MetaAggregates {
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
            "m20260518_000001_add_full_sql_meta_aggregates"
        );
    }
}
