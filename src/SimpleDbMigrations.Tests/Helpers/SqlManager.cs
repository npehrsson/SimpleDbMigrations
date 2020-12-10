// <author>Niclas Pehrsson (npehrsson@gmail.com)</author>
// <author>Adam Brengesjö (ca.brengesjo@gmail.com)</author>

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using SimpleDbMigrations.DbExtensions;

namespace SimpleDbMigrations.Tests.Helpers
{
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    public static class SqlManager
    {
        private static readonly int DefaultCommandTimeout = (int)TimeSpan.FromMinutes(15).TotalSeconds;
        private const string DropDatabaseQuery = "ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{0}]";
        private const string ContainsDatabaseQueryTemplate = "SELECT COUNT(*) FROM sys.databases WHERE name = '{0}'";

        public static async Task<bool> DatabaseExistsAsync(string connectionString, string databaseName)
        {
            await using var db = await OpenAsync(connectionString, string.Empty);
            return await DatabaseExistsAsync(db, databaseName);
        }

        public static async Task<bool> DatabaseExistsAsync(SqlConnection cnn, string databaseName)
        {
            return await ExecuteScalarAsync<int>(cnn, string.Format(CultureInfo.InvariantCulture, ContainsDatabaseQueryTemplate, databaseName)) == 1;
        }

        public static async Task<bool> DatabaseExistsAsync(string connectionString)
        {
            var databaseName = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
            return await DatabaseExistsAsync(connectionString, databaseName);
        }

        private static async Task CreateDatabaseAsync(string connectionString, string databaseName)
        {
            await using var db = await OpenAsync(connectionString, string.Empty);
            await new SqlDatabaseCreateCommand(databaseName).ExecuteAsync(db);
        }

        public static async Task DropDatabaseIfExistsAsync(string connectionString, string databaseName)
        {
            await using var db = await OpenAsync(connectionString);
            await DropDatabaseIfExistsAsync(db, databaseName);
        }

        public static async Task DropDatabaseIfExistsAsync(SqlConnection cnn, string databaseName)
        {
            if (cnn == null) throw new ArgumentNullException(nameof(cnn));
            if (!await DatabaseExistsAsync(cnn, databaseName))
                return;

            await ExecuteAsync(cnn, string.Format(CultureInfo.InvariantCulture, DropDatabaseQuery, databaseName));
        }

        public static async Task DropAndCreateDatabaseAsync(string connectionString, string databaseName)
        {
            await DropDatabaseIfExistsAsync(connectionString, databaseName);
            await CreateDatabaseAsync(connectionString, databaseName);
        }

        public static Task DropAndCreateDatabaseAsync(string connectionString) => ExecuteDbActionAsync(connectionString, DropAndCreateDatabaseAsync);

        public static Task CreateDatabaseAsync(string connectionString) => ExecuteDbActionAsync(connectionString, CreateDatabaseAsync);

        public static async Task CreateDatabaseIfNotExistsAsync(string connectionString)
        {
            if (!await DatabaseExistsAsync(connectionString))
                await CreateDatabaseAsync(connectionString);
        }

        public static Task DropDatabaseIfExistsAsync(string connectionString) => ExecuteDbActionAsync(connectionString, DropDatabaseIfExistsAsync);

        public static async Task RenameDatabaseAsync(string connectionString, string name, string newName)
        {
            await using var db = await OpenAsync(connectionString);

            await ExecuteAsync(db, $"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
            await ExecuteAsync(db, $"ALTER DATABASE [{name}] MODIFY NAME = [{newName}]"); 
            await ExecuteAsync(db, $"ALTER DATABASE [{newName}] SET MULTI_USER");
        }

        private static Task ExecuteDbActionAsync(string connectionString, Func<string, string, Task> dbAction)
        {
            if (dbAction == null) throw new ArgumentNullException(nameof(dbAction));
            var info = SqlManagerInfo.FromConnectionString(connectionString);
            return dbAction(info.ConnectionStringWithoutInitialCatalog, info.DatabaseName);
        }

        private static Task ExecuteAsync(SqlConnection db, string command) => db.ExecuteAsync(command, commandTimeout: DefaultCommandTimeout);
        private static Task<T> ExecuteScalarAsync<T>(SqlConnection db, string command) => db.ExecuteScalarAsync<T>(command, commandTimeout: DefaultCommandTimeout);
        private static Task<SqlConnection> OpenAsync(string connectionString, string databaseName) => new SqlConnectionFactory(connectionString, databaseName).OpenAsync();
        private static Task<SqlConnection> OpenAsync(string connectionString) => new SqlConnectionFactory(connectionString).OpenAsync();

        private class SqlManagerInfo
        {
            private SqlManagerInfo(string connectionString, string databaseName)
            {
                ConnectionStringWithoutInitialCatalog = connectionString;
                DatabaseName = databaseName;
            }

            public string ConnectionStringWithoutInitialCatalog { get; }
            public string DatabaseName { get; }

            public static SqlManagerInfo FromConnectionString(string connectionString)
            {
                var builder = new SqlConnectionStringBuilder(connectionString);
                var databaseName = builder.InitialCatalog;
                if (string.IsNullOrWhiteSpace(databaseName))
                    throw new ArgumentException("The specified connection string does not contain a Initial Catalog block.");
                builder.InitialCatalog = string.Empty;
                return new SqlManagerInfo(builder.ConnectionString, databaseName);
            }
        }
    }
}