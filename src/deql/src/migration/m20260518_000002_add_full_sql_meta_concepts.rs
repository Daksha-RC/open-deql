use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MetaConcepts::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(MetaConcepts::FullSql)
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
                    .table(MetaConcepts::Table)
                    .drop_column(MetaConcepts::FullSql)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum MetaConcepts {
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
            "m20260518_000002_add_full_sql_meta_concepts"
        );
    }
}
