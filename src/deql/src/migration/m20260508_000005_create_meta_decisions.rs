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
            .drop_table(Table::drop().table(MetaDecisions::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaDecisions::Table)
        .if_not_exists()
        .col(ColumnDef::new(MetaDecisions::OrgId).string().not_null())
        .col(ColumnDef::new(MetaDecisions::Name).string().not_null())
        .col(
            ColumnDef::new(MetaDecisions::Aggregate)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaDecisions::Command)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaDecisions::EmitsJson)
                .json()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaDecisions::HasGuard)
                .boolean()
                .not_null(),
        )
        .col(ColumnDef::new(MetaDecisions::GuardSql).text().null())
        .col(ColumnDef::new(MetaDecisions::StateSql).text().null())
        .col(ColumnDef::new(MetaDecisions::FullSql).text().not_null())
        .col(
            ColumnDef::new(MetaDecisions::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaDecisions::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaDecisions::OrgId)
                .col(MetaDecisions::Name),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaDecisions {
    Table,
    OrgId,
    Name,
    Aggregate,
    Command,
    EmitsJson,
    HasGuard,
    GuardSql,
    StateSql,
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
            "m20260508_000005_create_meta_decisions"
        );
    }
}
