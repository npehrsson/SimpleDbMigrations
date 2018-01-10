using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace SimpleDbMigrations
{
    public class ResourceMigration : Migration
    {
        public ResourceMigration(Assembly assembly, string resourceName, long version)
        {
            Assembly = assembly ?? throw new ArgumentNullException(nameof(assembly));
            ResourceName = resourceName;
            Version = version;
        }

        public Assembly Assembly { get; }
        public string ResourceName { get; }
        public override long Version { get; }

        public override void Execute(MigratorDatabase migratorDatabase)
        {
            foreach (var commandText in ParseCommands())
            {
                using (var command = migratorDatabase.CreateCommand())
                {
                    command.CommandText = commandText;
                    command.CommandTimeout = 0;
                    command.ExecuteNonQuery();
                }
            }
        }

        private IEnumerable<string> ParseCommands()
        {
            using (var streamReader = new StreamReader(Assembly.GetManifestResourceStream(ResourceName)))
            {
                var commandText = streamReader.ReadToEnd();
                return Regex.Split(commandText, @"\sGO\s").Where(x => !string.IsNullOrEmpty(x));
            }
        }
    }
}
