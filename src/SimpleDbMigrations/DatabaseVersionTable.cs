using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations
{
    internal class DatabaseVersionTable
    {
        private const string DatabaseVersionTableName = "DatabaseVersion";
        private const string DatabaseVersionColumnName = "Version";
        private const string DefaultSchema = "dbo";
        private const int ThereIsAlreadyAnObjectNamedXxxInTheDatabase = 2714;
        private const int SecondsToWaitOnFetchingTheDatabaseVersion = 240;

        public DatabaseVersionTable(string schemaName)
        {
            SchemaName = string.IsNullOrEmpty(schemaName) ? DefaultSchema : schemaName;
        }

        public DatabaseVersionTable()
        {
            SchemaName = DefaultSchema;
        }

        private bool HasSchema => !string.IsNullOrEmpty(SchemaName);
        public string SchemaName { get; }
        public string CurrentVersionQuery => $"SELECT {DatabaseVersionColumnName} FROM {TableName}";
        public string IfExistsCurrentVersionQuery => $@"IF OBJECT_ID('{TableName}', 'U') IS NULL    SELECT NULL ELSE    {CurrentVersionQuery};";
        private string TableName => (HasSchema ? $"{SchemaName}." : string.Empty) + DatabaseVersionTableName;

        public void CreateIfNotExisting(MigratorDatabase migratorDatabase)
        {
            if (!Exists(migratorDatabase))
            {
                Create(migratorDatabase);
            }
        }

        public long GetCurrentVersion(MigratorDatabase migratorDatabase)
        {
            return migratorDatabase.SqlQuery<long>(CurrentVersionQuery, SecondsToWaitOnFetchingTheDatabaseVersion).FirstOrDefault();
        }

        public long GetCurrentVersionWithLock(MigratorDatabase migratorDatabase)
        {
            return migratorDatabase.SqlQuery<long>(CurrentVersionQuery + " WITH (UPDLOCK, TABLOCK)", SecondsToWaitOnFetchingTheDatabaseVersion).FirstOrDefault();
        }

        public void SetVersion(MigratorDatabase migratorDatabase, long version)
        {
            if (migratorDatabase.ExecuteSqlCommand($"UPDATE {TableName} SET {DatabaseVersionColumnName} = {version}") == 0)
            {
                migratorDatabase.ExecuteSqlCommand($"INSERT INTO {TableName}(Version) VALUES({version})");
            }
        }

        public bool Exists(MigratorDatabase migratorDatabase)
        {
            return 0 != migratorDatabase.SqlQuery<int>($@"
                    SELECT Count(*) FROM sys.tables AS tables
                    JOIN sys.schemas AS schemas on tables.schema_id = schemas.schema_id
                    WHERE concat(schemas.name, '.', tables.name) = '{TableName}' AND type = 'U'").Single();
        }

        private void CreateSchemaIfNotExisting(MigratorDatabase migratorDatabase)
        {
            var schemaCount = migratorDatabase.SqlQuery<int>($@"
                    SELECT Count(schema_name) 
                    FROM information_schema.schemata 
                    WHERE schema_name = '{SchemaName}'").Single();

            if (schemaCount == 1)
                return;

            try
            {
                migratorDatabase.ExecuteSqlCommand($"CREATE SCHEMA {SchemaName}");
            }
            catch (SqlException e) when (e.Number == ThereIsAlreadyAnObjectNamedXxxInTheDatabase) { }
        }

        private void Create(MigratorDatabase migratorDatabase)
        {
            if (HasSchema)
                CreateSchemaIfNotExisting(migratorDatabase);

            try
            {
                migratorDatabase.ExecuteSqlCommand($@"
                    CREATE TABLE {TableName} (
                        Id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY CLUSTERED,
                        [Version] BIGINT
                    )");
            }
            catch (SqlException e) when (e.Number == ThereIsAlreadyAnObjectNamedXxxInTheDatabase)
            {
            }
        }
    }
}
