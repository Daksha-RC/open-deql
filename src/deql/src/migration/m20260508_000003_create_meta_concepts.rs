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
            .drop_table(Table::drop().table(MetaConcepts::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaConcepts::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaConcepts::OrgId).string().not_null())
        .col(ColumnDef::new(MetaConcepts::Kind).string().not_null())
        .col(ColumnDef::new(MetaConcepts::Name).string().not_null())
        .col(ColumnDef::new(MetaConcepts::JsonSource).json().not_null())
        .col(
            ColumnDef::new(MetaConcepts::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaConcepts::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaConcepts::OrgId)
                .col(MetaConcepts::Kind)
                .col(MetaConcepts::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaConcepts {
    Table,
    OrgId,
    Kind,
    Name,
    JsonSource,
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
            "m20260508_000003_create_meta_concepts"
        );
    }
}
