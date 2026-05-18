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
            .drop_table(Table::drop().table(MetaTemplates::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaTemplates::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaTemplates::OrgId).string().not_null())
        .col(ColumnDef::new(MetaTemplates::Name).string().not_null())
        .col(
            ColumnDef::new(MetaTemplates::ParametersJson)
                .json()
                .not_null(),
        )
        .col(ColumnDef::new(MetaTemplates::FullSql).text().not_null())
        .col(
            ColumnDef::new(MetaTemplates::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaTemplates::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaTemplates::OrgId)
                .col(MetaTemplates::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaTemplates {
    Table,
    OrgId,
    Name,
    ParametersJson,
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
            "m20260508_000009_create_meta_templates"
        );
    }
}
