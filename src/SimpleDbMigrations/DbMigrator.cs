using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public class DbMigrator
    {
        private const string DefaultSchema = DatabaseVersionTable.DefaultSchema;

        private readonly IMigrationsResolver _migrationsResolver;
        private readonly CachedDatabaseVersionTable _versionTable;
        private long _latestSchemaVersion;
        private volatile IList<Migration>? _migrations;


        public DbMigrator(IMigrationsResolver migrationsResolver, string schemaName = DefaultSchema)
        {
            _migrationsResolver = migrationsResolver ?? throw new ArgumentNullException(nameof(migrationsResolver));
            _versionTable = new CachedDatabaseVersionTable(new DatabaseVersionTable(schemaName));
        }

        public DbMigrator(Assembly assembly, string manifestPath, string schema = DefaultSchema)
            : this(new EmbeddedResourceMigrationResolver(assembly, manifestPath), schema)
        {
        }

        public DbMigrator(Type type, string schemaName = DefaultSchema)
            : this(new EmbeddedResourceMigrationResolver(type), schemaName)
        {
        }

        [Obsolete("Use overload with optional schemaName.")]
        public DbMigrator(string schemaName, IMigrationsResolver migrationsResolver)
            : this(migrationsResolver, schemaName)
        {
        }

        [Obsolete("Use overload with optional schemaName.")]
        public DbMigrator(string schemaName, Assembly assembly, string manifestPath)
            : this(schemaName, new EmbeddedResourceMigrationResolver(assembly, manifestPath))
        {
        }

        [Obsolete("Use overload with optional schemaName.")]
        public DbMigrator(string schemaName, Type type)
            : this(schemaName, new EmbeddedResourceMigrationResolver(type))
        {
        }

        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public IDbMigratorInterceptor? Interceptor { get; set; }

        public void Migrate(string connectionString) => MigrateAsync(connectionString).GetAwaiter().GetResult();

        public Task MigrateAsync(string connectionString, CancellationToken cancellation = default)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            return MigrateAsync(new MigratorDatabase(connectionString), cancellation);
        }

        private async Task MigrateAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            var migrations = LoadMigrationsIfNotLoaded();

            var dbVersion = await GetDbVersionAsync(database, cancellation);

            if (dbVersion >= _latestSchemaVersion)
                return;

            while (await MigrateNextAsync(dbVersion, database, migrations, cancellation))
            {
            }
        }

        private async Task<bool> MigrateNextAsync(long dbVersion, MigratorDatabase database, IList<Migration> migrations, CancellationToken cancellation = default)
        {
            using (database.BeginTransactionAsync(cancellation))
            {
                dbVersion = await _versionTable.GetCurrentVersionWithLockAsync(database);

                if (dbVersion >= _latestSchemaVersion)
                    return false;

                Interceptor?.PreMigration(database.Name, dbVersion, _latestSchemaVersion);

                migrations = migrations
                    .Where(x => x.Version > dbVersion)
                    .ToList();

                var firstMigration = migrations.First();

                if (firstMigration.DisableTransaction)
                {
                    using (var noTransactionDatabase = database.Clone())
                        await ExecuteMigrationAsync(migrations.First(), noTransactionDatabase, cancellation);

                    if (firstMigration.Version > dbVersion)
                        await _versionTable.SetVersionAsync(database, firstMigration.Version, cancellation);

                    await database.CommitAsync(cancellation);

                    if (migrations.Count == 1)
                    {
                        Interceptor?.PostMigration(database.Name, dbVersion, _latestSchemaVersion);
                        return false;
                    }

                    return true;
                }

                foreach (var migration in migrations)
                {
                    cancellation.ThrowIfCancellationRequested();

                    if (migration.DisableTransaction)
                    {
                        await database.CommitAsync(cancellation);
                        return true;
                    }

                    await ExecuteMigrationAsync(migration, database, cancellation);

                    if (migration.Version > dbVersion)
                        await _versionTable.SetVersionAsync(database, migration.Version, cancellation);
                }

                await database.CommitAsync(cancellation);

                Interceptor?.PostMigration(database.Name, dbVersion, _latestSchemaVersion);

                return false;
            }
        }

        private async Task ExecuteMigrationAsync(Migration migration, MigratorDatabase database, CancellationToken cancellation = default)
        {
            Interceptor?.PreMigrationStep(database.Name, migration);
            await migration.ExecuteAsync(database, cancellation);
            Interceptor?.PostMigrationStep(database.Name, migration);
        }

        private IList<Migration> LoadMigrationsIfNotLoaded()
        {
            if (_migrations != null)
                return _migrations;

            List<Migration> migrations;
            lock (_migrationsResolver)
            {
                if (_migrations != null)
                    return _migrations;

                migrations = _migrationsResolver.Resolve()
                    .OrderBy(x => x.Version)
                    .ToList();

                _latestSchemaVersion = migrations.Any() ? migrations.Max(x => x.Version) : 0;
                _migrations = migrations;
            }

            Interceptor?.DetectedMigrations(migrations.ToArray(), _latestSchemaVersion);
            return _migrations;
        }

        private async Task<long> GetDbVersionAsync(MigratorDatabase database, CancellationToken cancellation)
        {
            await _versionTable.CreateIfNotExistingAsync(database, cancellation);
            return await _versionTable.GetCurrentVersionAsync(database, cancellation);
        }
    }
}
