using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations.Tests.Helpers
{
    public class EmptyDbBuilder
    {
        public EmptyDbBuilder(string databaseName)
        {
            var connectionString = Environment.GetEnvironmentVariable("TESTS_CONNECTIONSTRING");

            if (string.IsNullOrEmpty(connectionString))
                connectionString = @"Data Source=(local);Integrated Security=True;Connection Timeout=600;";

            connectionString = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = databaseName
            }.ConnectionString;

            ConnectionFactory = new SqlConnectionFactory(connectionString);
        }

        public EmptyDbBuilder() : this("__SimpleDbMigrationsTests")
        {
        }

        public string ConnectionString => ConnectionFactory.ConnectionString;
        public SqlConnectionFactory ConnectionFactory { get; }

        public async Task CreateDatabaseAsync()
        {
            await SqlManager.DropDatabaseIfExistsAsync(ConnectionString);
            await SqlManager.CreateDatabaseAsync(ConnectionString);
            OnDatabaseCreated();
        }

        public async Task DropDatabaseAsync()
        {
            await SqlManager.DropDatabaseIfExistsAsync(ConnectionString);
        }

        protected virtual void OnDatabaseCreated()
        {
        }
    }
}
