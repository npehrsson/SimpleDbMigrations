using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations.Tests.Helpers
{
    public class SqlConnectionFactory
    {
        public SqlConnectionFactory(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(connectionString));

            ConnectionString = connectionString;
        }

        public SqlConnectionFactory(string connectionString, string databaseName) : this(ChangeInitialCatalog(connectionString, databaseName))
        {
        }

        public string ConnectionString { get; }

        public Task<SqlConnection> OpenAsync(CancellationToken cancellationToken = default) => OpenAsync(ConnectionString, cancellationToken);
        public Task<SqlConnection> OpenDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) => OpenAsync(ConnectionString, databaseName, cancellationToken);
        public Task<SqlConnection> OpenMasterAsync(CancellationToken cancellationToken = default) => OpenDatabaseAsync("master", cancellationToken);

        public static Task<SqlConnection> OpenAsync(string connectionString, string databaseName, CancellationToken cancellationToken = default) => OpenAsync(ChangeInitialCatalog(connectionString, databaseName), cancellationToken);

        public static async Task<SqlConnection> OpenAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            SqlConnection? connection = null;

            try
            {
                connection = NewConnection(connectionString);
                await connection.OpenAsync(cancellationToken);
                return connection;
            }
            catch
            {
                connection?.Dispose();
                throw;
            }
        }

        private static SqlConnection NewConnection(string connectionString) => new SqlConnection(connectionString);
        private static string ChangeInitialCatalog(string connectionString, string databaseName) => new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName }.ConnectionString;
    }
}