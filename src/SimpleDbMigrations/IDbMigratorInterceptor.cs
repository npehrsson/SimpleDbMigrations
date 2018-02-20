namespace SimpleDbMigrations
{
    public interface IDbMigratorInterceptor
    {
        void DetectedMigrations(Migration[] migrations, long latestSchemaVersionNumber);
        void PreMigrationStep(string databaseName, Migration migration);
        void PostMigrationStep(string databaseName, Migration migration);
        void PreMigration(string databaseName, long fromVersion, long toVersion);
        void PostMigration(string databaseName, long fromVersion, long toVersion);
    }
}