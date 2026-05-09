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
            .drop_table(
                Table::drop()
                    .table(MetaTemplatesInstances::Table)
                    .to_owned(),
            )
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(MetaTemplatesInstances::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(MetaTemplatesInstances::OrgId)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaTemplatesInstances::TemplateName)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaTemplatesInstances::ArgsJson)
                .json()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaTemplatesInstances::GeneratedNamesJson)
                .json()
                .not_null(),
        )
        .col(
            ColumnDef::new(MetaTemplatesInstances::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(MetaTemplatesInstances::IsDropped)
                .boolean()
                .not_null()
                .default(false),
        )
        .primary_key(
            Index::create()
                .col(MetaTemplatesInstances::OrgId)
                .col(MetaTemplatesInstances::TemplateName),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum MetaTemplatesInstances {
    Table,
    OrgId,
    TemplateName,
    ArgsJson,
    GeneratedNamesJson,
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
            "m20260508_000010_create_meta_templates_instances"
        );
    }
}
