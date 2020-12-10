using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public class TextMigration : Migration
    {
        private static readonly Regex CommandSeparator = new Regex(@"\sGO\s", RegexOptions.Compiled);

        private readonly Func<Task<string>> _readToEndAsync;

        public TextMigration(long version, bool disableTransaction, string text)
            : this(version, disableTransaction, () => Task.FromResult(text))
        {
            if (string.IsNullOrWhiteSpace(text))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(text));
        }

        public TextMigration(long version, bool disableTransaction, Func<Task<string>> readToEndAsync)
        {
            Version = version;
            DisableTransaction = disableTransaction;
            _readToEndAsync = readToEndAsync ?? throw new ArgumentNullException(nameof(readToEndAsync));
        }

        public override long Version { get; }
        public override bool DisableTransaction { get; }

        public override async Task ExecuteAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (database == null) throw new ArgumentNullException(nameof(database));

            foreach (var commandText in CommandSeparator.Split(await _readToEndAsync()))
            {
                if (string.IsNullOrWhiteSpace(commandText))
                    continue;

                await database.ExecuteAsync(commandText, 0, cancellation);
            }
        }
    }
}
