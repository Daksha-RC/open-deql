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
            .drop_table(Table::drop().table(MetaEvents::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaEvents::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaEvents::OrgId).string().not_null())
        .col(ColumnDef::new(MetaEvents::Name).string().not_null())
        .col(ColumnDef::new(MetaEvents::Aggregate).string().not_null())
        .col(
            ColumnDef::new(MetaEvents::AttributesJson)
                .json()
                .not_null(),
        )
        .col(ColumnDef::new(MetaEvents::FullSql).text().not_null())
        .col(
            ColumnDef::new(MetaEvents::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaEvents::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaEvents::OrgId)
                .col(MetaEvents::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaEvents {
    Table,
    OrgId,
    Name,
    Aggregate,
    AttributesJson,
    FullSql,
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
            "m20260508_000008_create_meta_events"
        );
    }
}
