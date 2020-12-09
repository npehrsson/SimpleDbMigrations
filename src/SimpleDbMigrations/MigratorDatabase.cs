using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations
{
    public class MigratorDatabase : IDisposable
    {
        private readonly string _connectionString;
        private readonly SqlConnection _connection;
        private SqlTransaction? _transaction;

        public MigratorDatabase(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            _connection = new SqlConnection(connectionString);
            _connectionString = connectionString;
        }

        public string Name => _connection.Database;

        public async Task<IDisposable> BeginTransactionAsync(CancellationToken cancellation = default)
        {
            if (_transaction != null) 
                throw new InvalidOperationException("Cannot open a transaction twice");

            await OpenIfClosedAsync(cancellation);
            return await _connection.BeginTransactionAsync(cancellation);
        }

        public async Task<SqlQuery<T>> SqlQueryAsync<T>(string command, int commandTimeout = 30, CancellationToken cancellation = default) 
        {
            await OpenIfClosedAsync(cancellation);
            return new SqlQuery<T>(command, this) { CommandTimeout = commandTimeout };
        }

        public async Task<int> ExecuteSqlCommandAsync(string commandText, CancellationToken cancellation = default)
        {
            await OpenIfClosedAsync(cancellation);
            await using var command = await CreateCommandAsync(cancellation);
            command.CommandText = commandText;
            return await command.ExecuteNonQueryAsync(cancellation);
        }

        public async Task<SqlCommand> CreateCommandAsync(CancellationToken cancellation = default)
        {
            await OpenIfClosedAsync(cancellation);
            var command = _connection.CreateCommand();
            command.Transaction = _transaction;
            return command;
        }

        public async Task CommitAsync(CancellationToken cancellation = default)
        {
            var transaction = _transaction ?? throw new InvalidOperationException($"No transaction active. Call {nameof(BeginTransactionAsync)} first.");
            await transaction.CommitAsync(cancellation);
            _transaction = null;
        }

        public MigratorDatabase Clone() => new MigratorDatabase(_connectionString);
        
        private Task OpenIfClosedAsync(CancellationToken cancellation = default)
        {
            if (_connection.State != ConnectionState.Closed)
                return Task.CompletedTask;

            return _connection.OpenAsync(cancellation);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _transaction?.Dispose();
        }
    }
}
