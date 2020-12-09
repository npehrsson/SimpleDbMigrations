using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace SimpleDbMigrations
{
    public class EmbeddedResourceMigrationResolver : IMigrationsResolver
    {
        private readonly Regex _versionNumberRegex = new Regex(@"(?'version'\d+).*\.sql");
        private readonly Assembly _assembly;
        private readonly string _manifestPath;

        public EmbeddedResourceMigrationResolver(Assembly assembly, string manifestPath)
        {
            _assembly = assembly ?? throw new ArgumentNullException(nameof(assembly));
            _manifestPath = manifestPath ?? throw new ArgumentNullException(nameof(manifestPath));
        }

        public EmbeddedResourceMigrationResolver(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            _assembly = type.Assembly;
            _manifestPath = type.Namespace;
        }

        public IList<Migration> Resolve()
        {
            return _assembly
                .GetManifestResourceNames()
                .Where(x => x.IndexOf(_manifestPath, StringComparison.OrdinalIgnoreCase) > -1
                            && x.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
                .Select(x => new EmbeddedResourceMigration(_assembly, x, GetFileVersion(x), IsTransactionDisabled(x)) as Migration)
                .OrderBy(x => x.Version)
                .ToList();
        }

        private bool IsTransactionDisabled(string name) =>  name.EndsWith("disable-transaction.sql", StringComparison.InvariantCultureIgnoreCase);

        private long GetFileVersion(string name)
        {
            return long.Parse(_versionNumberRegex.Match(name).Groups["version"].Value);
        }
    }
}