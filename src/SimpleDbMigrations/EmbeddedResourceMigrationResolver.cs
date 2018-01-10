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
            _assembly = assembly;
            _manifestPath = manifestPath;
        }

        public IList<Migration> Resolve()
        {
            return _assembly
                .GetManifestResourceNames()
                .Where(x => x.IndexOf(_manifestPath, StringComparison.OrdinalIgnoreCase) > -1
                            && x.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
                .Select(x => new ResourceMigration(_assembly, x, GetFileVersion(x)) as Migration)
                .OrderBy(x => x.Version)
                .ToList();
        }

        private long GetFileVersion(string name)
        {
            return long.Parse(_versionNumberRegex.Match(name).Groups["version"].Value);
        }
    }
}