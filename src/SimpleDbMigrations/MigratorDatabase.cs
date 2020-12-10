using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using SimpleDbMigrations.DbExtensions;

namespace SimpleDbMigrations
{
    public class MigratorDatabase : IDisposable
    {
        private readonly string _connectionString;
        private readonly SqlConnection _connection;
        private DbTransaction? _transaction;

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
            return _transaction = await _connection.BeginTransactionAsync(cancellation);
        }

        public async Task<int> ExecuteAsync(string commandText, int? commandTimeout = null, CancellationToken cancellation = default)
            => await _connection.ExecuteAsync(commandText, commandTimeout, transaction: _transaction, cancellation: cancellation);
        public async Task<T> FirstOrDefaultAsync<T>(string commandText, int? commandTimeout = null, CancellationToken cancellation = default)
            => await _connection.FirstOrDefaultAsync<T>(commandText, commandTimeout, transaction: _transaction, cancellation: cancellation);
        public async Task<T> SingleAsync<T>(string commandText, int? commandTimeout = null, CancellationToken cancellation = default)
            => await _connection.SingleAsync<T>(commandText, commandTimeout, transaction: _transaction, cancellation: cancellation);

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
