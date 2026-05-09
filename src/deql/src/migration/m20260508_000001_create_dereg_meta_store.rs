use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.create_table(create_table_stmt()).await?;
        manager.create_index(idx_org_id_id()).await?;
        manager.create_index(idx_org_stream_concept()).await?;
        manager.create_index(idx_org_status()).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(DeregMetaStore::Table).to_owned())
            .await
    }
}

fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeregMetaStore::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeregMetaStore::Id)
                .big_integer()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(DeregMetaStore::OrgId).string().not_null())
        .col(ColumnDef::new(DeregMetaStore::StreamId).string().not_null())
        .col(
            ColumnDef::new(DeregMetaStore::EventType)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(DeregMetaStore::ConceptType)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(DeregMetaStore::ConceptKey)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(DeregMetaStore::OccurredAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(ColumnDef::new(DeregMetaStore::Status).string().not_null())
        .col(ColumnDef::new(DeregMetaStore::ErrorMessage).text().null())
        .col(ColumnDef::new(DeregMetaStore::Statement).text().not_null())
        .col(ColumnDef::new(DeregMetaStore::Meta).json().not_null())
        .to_owned()
}

fn idx_org_id_id() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("idx_dereg_org_id_id")
        .table(DeregMetaStore::Table)
        .col(DeregMetaStore::OrgId)
        .col(DeregMetaStore::Id)
        .to_owned()
}

fn idx_org_stream_concept() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("idx_dereg_org_stream_concept")
        .table(DeregMetaStore::Table)
        .col(DeregMetaStore::OrgId)
        .col(DeregMetaStore::StreamId)
        .col(DeregMetaStore::ConceptKey)
        .to_owned()
}

fn idx_org_status() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("idx_dereg_org_status")
        .table(DeregMetaStore::Table)
        .col(DeregMetaStore::OrgId)
        .col(DeregMetaStore::Status)
        .to_owned()
}

#[derive(DeriveIden)]
enum DeregMetaStore {
    Table,
    Id,
    OrgId,
    StreamId,
    EventType,
    ConceptType,
    ConceptKey,
    OccurredAt,
    Status,
    ErrorMessage,
    Statement,
    Meta,
}

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigrationName;

    use super::*;

    #[test]
    fn test_migration_name() {
        assert_eq!(Migration.name(), "m20260508_000001_create_dereg_meta_store");
    }
}
