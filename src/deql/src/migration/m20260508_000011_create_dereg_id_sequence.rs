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
            .drop_table(Table::drop().table(DeregIdSequence::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeregIdSequence::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeregIdSequence::Name)
                .string()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(DeregIdSequence::LastId)
                .big_integer()
                .not_null()
                .default(0),
        )
        .to_owned()
}

#[derive(DeriveIden)]
enum DeregIdSequence {
    Table,
    Name,
    LastId,
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigrationName;

    use super::*;

    #[test]
    fn test_migration_name() {
        assert_eq!(Migration.name(), "m20260508_000011_create_dereg_id_sequence");
    }
}