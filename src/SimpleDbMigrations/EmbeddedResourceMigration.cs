using System;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public class EmbeddedResourceMigration : Migration
    {
        public EmbeddedResourceMigration(Assembly assembly, string resourceName, long version, bool disableTransaction)
        {
            Assembly = assembly ?? throw new ArgumentNullException(nameof(assembly));
            ResourceName = resourceName;
            Version = version;
            DisableTransaction = disableTransaction;
        }

        public Assembly Assembly { get; }
        public string ResourceName { get; }
        public override bool DisableTransaction { get; }
        public override long Version { get; }

        public override async Task ExecuteAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (database == null) throw new ArgumentNullException(nameof(database));
            foreach (var commandText in await ParseCommandsAsync())
            {
                cancellation.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(commandText))
                    continue;

                await using var command = await database.CreateCommandAsync(cancellation);
                command.CommandText = commandText;
                command.CommandTimeout = 0;
                await command.ExecuteNonQueryAsync(cancellation);
            }
        }

        private async Task<string[]> ParseCommandsAsync()
        {
            using var streamReader = new StreamReader(Assembly.GetManifestResourceStream(ResourceName) ?? throw new InvalidOperationException($"No such resource found: {ResourceName}"));
            var commandText = await streamReader.ReadToEndAsync();
            return Regex.Split(commandText, @"\sGO\s");
        }
    }
}
