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
            .drop_table(Table::drop().table(MetaInspections::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaInspections::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaInspections::OrgId).string().not_null())
        .col(ColumnDef::new(MetaInspections::Name).string().not_null())
        .col(
            ColumnDef::new(MetaInspections::DecisionName)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaInspections::InputOutputJson)
                .json()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaInspections::FullSql)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaInspections::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaInspections::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaInspections::OrgId)
                .col(MetaInspections::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaInspections {
    Table,
    OrgId,
    Name,
    DecisionName,
    InputOutputJson,
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
            "m20260508_000006_create_meta_inspections"
        );
    }
}
