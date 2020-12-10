using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public class CachedDatabaseVersionTable
    {
        private readonly DatabaseVersionTable _versionTable;
        private readonly ConcurrentDictionary<string, long> _cache;

        internal CachedDatabaseVersionTable(DatabaseVersionTable versionTable)
        {
            _versionTable = versionTable ?? throw new ArgumentNullException(nameof(versionTable));
            _cache = new ConcurrentDictionary<string, long>();
        }

        public Task CreateIfNotExistingAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (_cache.ContainsKey(database.Name))
                return Task.CompletedTask;

            return _versionTable.CreateIfNotExistingAsync(database, cancellation);
        }

        public Task<bool> ExistsAsync(MigratorDatabase database, CancellationToken cancellation = default) => _versionTable.ExistsAsync(database, cancellation);

        public async Task<long> GetCurrentVersionAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            if (_cache.TryGetValue(database.Name, out var version))
            {
                return version;
            }

            version = await _versionTable.GetCurrentVersionAsync(database, cancellation);
            _cache.AddOrUpdate(database.Name, version, (key, value) => version);

            return version;
        }

        public async Task<long> GetCurrentVersionWithLockAsync(MigratorDatabase database)
        {
            var version = await _versionTable.GetCurrentVersionWithLockAsync(database);
            _cache.AddOrUpdate(database.Name, version, (key, value) => version);
            return version;
        }

        public async Task SetVersionAsync(MigratorDatabase database, long version, CancellationToken cancellation = default)
        {
            await _versionTable.SetVersionAsync(database, version, cancellation);
            _cache.AddOrUpdate(database.Name, version, (key, value) => version);
        }

        public bool IsVersionLoaded(string database)
        {
            return _cache.ContainsKey(database);
        }
    }
}
