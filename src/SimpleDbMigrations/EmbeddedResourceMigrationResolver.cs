using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

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

        public EmbeddedResourceMigrationResolver(Type type, string? manifestSubPath = null)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            _assembly = type.Assembly;
            _manifestPath = type.Namespace ?? throw new InvalidOperationException("Type does not have namespace.");
            if (manifestSubPath != null)
                _manifestPath += "." + manifestSubPath;
        }

        public IList<Migration> Resolve()
        {
            return _assembly
                .GetManifestResourceNames()
                .Where(x => x.IndexOf(_manifestPath, StringComparison.OrdinalIgnoreCase) > -1
                            && x.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
                .Select(x => new TextMigration(GetFileVersion(x), IsTransactionDisabled(x), () => ReadToEndAsync(x)) as Migration)
                .OrderBy(x => x.Version)
                .ToList();
        }

        private async Task<string> ReadToEndAsync(string resourceName)
        {
            using var streamReader = new StreamReader(_assembly.GetManifestResourceStream(resourceName) ?? throw new InvalidOperationException($"No such resource found: {resourceName}"));
            return await streamReader.ReadToEndAsync();
        }

        private bool IsTransactionDisabled(string name) =>  name.EndsWith("disable-transaction.sql", StringComparison.InvariantCultureIgnoreCase);

        private long GetFileVersion(string name)
        {
            return long.Parse(_versionNumberRegex.Match(name).Groups["version"].Value);
        }
    }
}