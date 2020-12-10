using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global

namespace SimpleDbMigrations.Tests.Helpers
{
    public class SqlDatabaseCreateCommand
    {
        public SqlDatabaseCreateCommand(string databaseName)
        {
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public string DatabaseName { get; set; }
        public TimeSpan CreateCommandTimeout { get; set; } = TimeSpan.Zero;
        public bool DropIfExists { get; set; }

        public async Task ExecuteAsync(SqlConnection cnn, CancellationToken cancellation = default)
        {
            if (cnn == null) throw new ArgumentNullException(nameof(cnn));
            if (!IsValidIdentifier(DatabaseName))
                throw new InvalidOperationException($"{nameof(DatabaseName)} is not a valid identifier.");

            if (DropIfExists)
                await SqlManager.DropDatabaseIfExistsAsync(cnn, DatabaseName);

            var commandTimeout = 0;
            if (CreateCommandTimeout > TimeSpan.Zero)
                commandTimeout = (int)CreateCommandTimeout.TotalSeconds;

            await CreateDatabaseAsync(cnn, $"CREATE DATABASE [{DatabaseName}]", commandTimeout);
            SqlConnection.ClearAllPools();
        }

        private async Task CreateDatabaseAsync(SqlConnection cnn, string sql, int? commandTimeout)
        {
            await using var command = cnn.CreateCommand();
            command.CommandText = sql;
            if (commandTimeout != null)
                command.CommandTimeout = commandTimeout.Value;
            await command.ExecuteNonQueryAsync();
        }

        private static bool IsValidIdentifier(string value) => SqlIdentifierValidation.IsValid(value);
    }
}