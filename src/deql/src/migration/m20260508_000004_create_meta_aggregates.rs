use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.create_table(create_table_stmt()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(MetaAggregates::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaAggregates::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaAggregates::OrgId).string().not_null())
        .col(ColumnDef::new(MetaAggregates::Name).string().not_null())
        .col(
            ColumnDef::new(MetaAggregates::FieldsJson)
                .json()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaAggregates::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaAggregates::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaAggregates::OrgId)
                .col(MetaAggregates::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaAggregates {
    Table,
    OrgId,
    Name,
    FieldsJson,
    LastAppliedId,
    IsDropped,
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigrationName;

    use super::*;

    #[test]
    fn test_migration_name() {
        assert_eq!(
            Migration.name(),
            "m20260508_000004_create_meta_aggregates"
        );
    }
}
