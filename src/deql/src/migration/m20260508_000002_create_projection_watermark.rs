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
            .drop_table(Table::drop().table(ProjectionWatermark::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(ProjectionWatermark::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ProjectionWatermark::OrgId)
                .string()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ProjectionWatermark::LastAppliedId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(ProjectionWatermark::UpdatedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum ProjectionWatermark {
    Table,
    OrgId,
    LastAppliedId,
    UpdatedAt,
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigrationName;

    use super::*;

    #[test]
    fn test_migration_name() {
        assert_eq!(
            Migration.name(),
            "m20260508_000002_create_projection_watermark"
        );
    }
}
