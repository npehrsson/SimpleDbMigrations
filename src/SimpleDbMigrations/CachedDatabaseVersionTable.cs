using System;
using System.Collections.Concurrent;

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

        public void CreateIfNotExisting(MigratorDatabase migratorDatabase)
        {
            if (_cache.ContainsKey(migratorDatabase.Name))
            {
                return;
            }

            _versionTable.CreateIfNotExisting(migratorDatabase);
        }

        public bool Exists(MigratorDatabase database)
        {
            return _versionTable.Exists(database);
        }

        public long GetCurrentVersion(MigratorDatabase migratorDatabase)
        {
            if (_cache.TryGetValue(migratorDatabase.Name, out var version))
            {
                return version;
            }

            version = _versionTable.GetCurrentVersion(migratorDatabase);
            _cache.AddOrUpdate(migratorDatabase.Name, version, (key, value) => version);

            return version;
        }

        public long GetCurrentVersionWithLock(MigratorDatabase migratorDatabase)
        {
            var version = _versionTable.GetCurrentVersionWithLock(migratorDatabase);
            _cache.AddOrUpdate(migratorDatabase.Name, version, (key, value) => version);
            return version;
        }

        public void SetVersion(MigratorDatabase migratorDatabase, long version)
        {
            _versionTable.SetVersion(migratorDatabase, version);
            _cache.AddOrUpdate(migratorDatabase.Name, version, (key, value) => version);
        }

        public bool IsVersionLoaded(string database)
        {
            return _cache.ContainsKey(database);
        }
    }
}
