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
            .drop_table(Table::drop().table(MetaCommands::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaCommands::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaCommands::OrgId).string().not_null())
        .col(ColumnDef::new(MetaCommands::Name).string().not_null())
        .col(
            ColumnDef::new(MetaCommands::Aggregate)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaCommands::AttributesJson)
                .json()
                .not_null(),
        )
        .col(ColumnDef::new(MetaCommands::FullSql).text().not_null())
        .col(
            ColumnDef::new(MetaCommands::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaCommands::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaCommands::OrgId)
                .col(MetaCommands::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaCommands {
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
            "m20260508_000007_create_meta_commands"
        );
    }
}
