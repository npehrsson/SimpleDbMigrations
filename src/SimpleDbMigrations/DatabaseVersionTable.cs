using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations
{
    internal class DatabaseVersionTable
    {
        private const string DatabaseVersionTableName = "DatabaseVersion";
        private const string DatabaseVersionColumnName = "Version";
        public const string DefaultSchema = "dbo";
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

        public async Task CreateIfNotExistingAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (!await ExistsAsync(database, cancellation))
            {
                await CreateAsync(database, cancellation);
            }
        }

        public async Task<long> GetCurrentVersionAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            var cmd = await database.SqlQueryAsync<long>(CurrentVersionQuery, SecondsToWaitOnFetchingTheDatabaseVersion, cancellation);
            return await cmd.FirstOrDefaultAsync(cancellation);
        }

        public async Task<long> GetCurrentVersionWithLockAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            var cmd = await database.SqlQueryAsync<long>(CurrentVersionQuery + " WITH (UPDLOCK, TABLOCK)", SecondsToWaitOnFetchingTheDatabaseVersion, cancellation);
            return await cmd.FirstOrDefaultAsync(cancellation);
        }

        public async Task SetVersionAsync(MigratorDatabase database, long version, CancellationToken cancellation = default)
        {
            if (await database.ExecuteSqlCommandAsync($"UPDATE {TableName} SET {DatabaseVersionColumnName} = {version}", cancellation) == 0)
            {
                await database.ExecuteSqlCommandAsync($"INSERT INTO {TableName}(Version) VALUES({version})", cancellation);
            }
        }

        public async Task<bool> ExistsAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            var cmd = await database.SqlQueryAsync<int>($@"
                    SELECT Count(*) FROM sys.tables AS tables
                    JOIN sys.schemas AS schemas on tables.schema_id = schemas.schema_id
                    WHERE concat(schemas.name, '.', tables.name) = '{TableName}' AND type = 'U'", cancellation: cancellation);
            return 0 != await cmd.SingleAsync(cancellation);
        }

        private async Task CreateSchemaIfNotExistingAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            var cmd = await database.SqlQueryAsync<int>($@"
                    SELECT Count(schema_name) 
                    FROM information_schema.schemata 
                    WHERE schema_name = '{SchemaName}'", cancellation: cancellation);
            var schemaCount = await cmd.SingleAsync(cancellation);

            if (schemaCount == 1)
                return;

            try
            {
                await database.ExecuteSqlCommandAsync($"CREATE SCHEMA {SchemaName}", cancellation);
            }
            catch (SqlException e) when (e.Number == ThereIsAlreadyAnObjectNamedXxxInTheDatabase) { }
        }

        private async Task CreateAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (HasSchema)
                await CreateSchemaIfNotExistingAsync(database, cancellation);

            try
            {
                await database.ExecuteSqlCommandAsync($@"
                    CREATE TABLE {TableName} (
                        Id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY CLUSTERED,
                        [Version] BIGINT
                    )", cancellation);
            }
            catch (SqlException e) when (e.Number == ThereIsAlreadyAnObjectNamedXxxInTheDatabase)
            {
            }
        }
    }
}
