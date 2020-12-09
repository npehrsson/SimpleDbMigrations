using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public class SqlQuery<T>
    {
        private readonly string _command;
        private readonly MigratorDatabase _migratorDatabase;

        public SqlQuery(string command, MigratorDatabase migratorDatabase)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _migratorDatabase = migratorDatabase ?? throw new ArgumentNullException(nameof(migratorDatabase));
        }

        public int CommandTimeout { get; set; } = 30;

        public async Task<T> FirstOrDefaultAsync(CancellationToken cancellation = default)
        {
            await using var command = await _migratorDatabase.CreateCommandAsync(cancellation);
            command.CommandText = _command;
            command.CommandTimeout = CommandTimeout;
            await using var reader = await command.ExecuteReaderAsync(cancellation);
            if (await reader.ReadAsync(cancellation))
                return (T) reader.GetValue(0);

            return default!;
        }

        public async Task<T> SingleAsync(CancellationToken cancellation = default)
        {
            await using var command = await _migratorDatabase.CreateCommandAsync(cancellation);
            command.CommandText = _command;
            command.CommandTimeout = CommandTimeout;
            await using var reader = await command.ExecuteReaderAsync(cancellation);
            if (!await reader.ReadAsync(cancellation))
                throw new InvalidOperationException("Sequence contained zero items");

            var value = (T)reader.GetValue(0);

            if (await reader.ReadAsync(cancellation))
                throw new InvalidOperationException("Sequence contained more than one item");

            return value;
        }
    }
}
