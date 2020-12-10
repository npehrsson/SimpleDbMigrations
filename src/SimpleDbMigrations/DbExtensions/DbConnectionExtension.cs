using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations.DbExtensions
{
    internal static class DbConnectionExtension
    {
        public static async Task<int> ExecuteAsync(
            this IDbConnection connection,
            string commandText,
            int? commandTimeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction? transaction = null,
            CancellationToken cancellation = default)
        {
            await using var command = await CreateCommandAsync(connection, commandText, commandTimeout, commandType, transaction, cancellation);

            return await command.ExecuteNonQueryAsync(cancellation);
        }

        public static async Task<T> ExecuteScalarAsync<T>(
            this IDbConnection connection,
            string commandText,
            int? commandTimeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction? transaction = null,
            CancellationToken cancellation = default)
        {
            await using var command = await CreateCommandAsync(connection, commandText, commandTimeout, commandType, transaction, cancellation);

            var scalar = await command.ExecuteScalarAsync(cancellation);
            return ChangeType<T>(scalar);
        }

        public static async Task<List<T>> QueryAsync<T>(
            this IDbConnection connection,
            string commandText,
            int? commandTimeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction? transaction = null,
            CancellationToken cancellation = default)
        {
            await using var command = await CreateCommandAsync(connection, commandText, commandTimeout, commandType, transaction, cancellation);

            await using var reader = await command.ExecuteReaderAsync(cancellation);
            var result = new List<T>();
            while (await reader.ReadAsync(cancellation))
                result.Add(ChangeType<T>(reader.GetValue(0)));
            return result;
        }

        public static async Task<T> FirstOrDefaultAsync<T>(
            this IDbConnection connection,
            string commandText,
            int? commandTimeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction? transaction = null,
            CancellationToken cancellation = default)
        {
            await using var command = await CreateCommandAsync(connection, commandText, commandTimeout, commandType, transaction, cancellation);

            await using var reader = await command.ExecuteReaderAsync(cancellation);
            if (!await reader.ReadAsync(cancellation))
                return default!;

            return ChangeType<T>(reader.GetValue(0));
        }

        public static async Task<T> SingleAsync<T>(
            this IDbConnection connection,
            string commandText,
            int? commandTimeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction? transaction = null,
            CancellationToken cancellation = default)
        {
            await using var command = await CreateCommandAsync(connection, commandText, commandTimeout, commandType, transaction, cancellation);

            await using var reader = await command.ExecuteReaderAsync(cancellation);
            if (!await reader.ReadAsync(cancellation))
                throw new InvalidOperationException("Sequence contains no elements.");

            var result = ChangeType<T>(reader.GetValue(0));

            if (await reader.ReadAsync(cancellation))
                throw new InvalidOperationException("Sequence contains more than one element.");

            return result;
        }

        private static async Task<DbCommand> CreateCommandAsync(
            IDbConnection connection,
            string commandText,
            int? commandTimeout,
            CommandType commandType,
            IDbTransaction? transaction,
            CancellationToken cancellation)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(commandText))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(commandText));
            DbTransaction? dbTransaction = null;
            if (transaction is DbTransaction dbTransaction2)
                dbTransaction = dbTransaction2;
            else if (transaction != null)
                throw new InvalidOperationException($"Transaction must be a {nameof(DbTransaction)} to support async operations.");

            await OpenIfClosedAsync(connection, cancellation);

            var command = CreateAsyncCommand(connection);
            command.CommandText = commandText;
            command.CommandType = commandType;

            if (dbTransaction != null)
                command.Transaction = dbTransaction;

            if (commandTimeout != null)
                command.CommandTimeout = commandTimeout.Value;

            return command;
        }

        private static T ChangeType<T>(object scalar) => (T)Convert.ChangeType(scalar, typeof(T));

        private static DbCommand CreateAsyncCommand(IDbConnection connection)
        {
            if (connection.CreateCommand() is DbCommand command)
                return command;
            
            throw new InvalidOperationException($"Command returned by {nameof(IDbConnection)}.{nameof(connection.CreateCommand)} must return a {nameof(DbCommand)} to support async operations.");
        }

        private static Task OpenIfClosedAsync(IDbConnection connection, CancellationToken cancellation = default)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (!(connection is DbConnection dbConnection))
                throw new InvalidOperationException($"Connection must be a {nameof(DbConnection)} to support async operations.");

            if (dbConnection.State != ConnectionState.Closed)
                return Task.CompletedTask;

            return dbConnection.OpenAsync(cancellation);
        }
    }
}
