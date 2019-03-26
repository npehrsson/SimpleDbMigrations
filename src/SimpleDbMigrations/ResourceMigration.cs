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
        private static Regex Comments = new Regex(@"--.*$", RegexOptions.Compiled|RegexOptions.Multiline);
        private static Regex MiddleGo = new Regex(@"\sGO\s", RegexOptions.Compiled|RegexOptions.IgnoreCase);
        private static Regex LeadingGo = new Regex(@"^GO\s", RegexOptions.Compiled|RegexOptions.IgnoreCase);
        private static Regex TrailingGo = new Regex(@"\sGO$", RegexOptions.Compiled|RegexOptions.IgnoreCase);
        
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
                commandText = Comments.Replace(commandText, string.Empty);
                return MiddleGo
                    .Split(commandText)
                    .Select(x => {
                        if (LeadingGo.IsMatch(x))
                            x = x.Substring(2);
                        if (TrailingGo.IsMatch(x))
                            x = x.Substring(0, x.Length - 2);
                        return x;
                    })
                    .Where(x => !string.IsNullOrEmpty(x));
            }
        }
    }
}
