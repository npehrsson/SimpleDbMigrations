using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations
{
    public class MigratorDatabase : IDisposable
    {
        public MigratorDatabase(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            Connection = new SqlConnection(connectionString);
            ConnectionString = connectionString;
        }

        private string ConnectionString { get; set; }
        private SqlConnection Connection { get; }
        public string Name => Connection.Database;
        private SqlTransaction Transaction { get; set; }

        public async Task<IDisposable> BeginTransactionAsync(CancellationToken cancellation = default)
        {
            if (Transaction != null) 
                throw new InvalidOperationException("Cannot open a transaction twice");

            await OpenIfClosedAsync(cancellation);
            return await Connection.BeginTransactionAsync(cancellation);
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
            var command = Connection.CreateCommand();
            command.Transaction = Transaction;
            return command;
        }

        public async Task CommitASync(CancellationToken cancellation = default)
        {
            await Transaction.CommitAsync(cancellation);
            Transaction = null;
        }

        public MigratorDatabase Clone() => new MigratorDatabase(ConnectionString);
        
        private Task OpenIfClosedAsync(CancellationToken cancellation = default)
        {
            if (Connection.State != ConnectionState.Closed)
                return Task.CompletedTask;

            return Connection.OpenAsync(cancellation);
        }

        public void Dispose()
        {
            Connection?.Dispose();
            Transaction?.Dispose();
        }
    }
}
