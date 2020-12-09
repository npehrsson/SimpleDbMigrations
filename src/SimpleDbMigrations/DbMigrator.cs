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
        private readonly IMigrationsResolver _migrationsResolver;

        public DbMigrator(string schemaName, IMigrationsResolver migrationsResolver)
        {
            _migrationsResolver = migrationsResolver ?? throw new ArgumentNullException(nameof(migrationsResolver));
            SchemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
            VersionTable = new CachedDatabaseVersionTable(new DatabaseVersionTable(SchemaName));
        }

        public DbMigrator(string schemaName, Assembly assembly, string manifestPath)
            : this(schemaName, new EmbeddedResourceMigrationResolver(assembly, manifestPath)) { }

        public DbMigrator(string schemaName, Type type)
            : this(schemaName, new EmbeddedResourceMigrationResolver(type)) { }

        private string SchemaName { get; }
        private long LatestSchemaVersion { get; set; }
        private CachedDatabaseVersionTable VersionTable { get; }
        private IList<Migration> Migrations { get; set; }
        public IDbMigratorInterceptor Interceptor { get; set; }

        public void Migrate(string connectionString) => MigrateAsync(connectionString).GetAwaiter().GetResult();

        public Task MigrateAsync(string connectionString, CancellationToken cancellation = default)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            return MigrateAsync(new MigratorDatabase(connectionString), cancellation);
        }

        private async Task MigrateAsync(MigratorDatabase database, CancellationToken cancellation = default)
        {
            LoadMigrationsIfNotLoaded();

            var dbVersion = await GetDbVersionAsync(database, cancellation);

            if (dbVersion >= LatestSchemaVersion)
                return;

            while (await MigrateNextAsync(dbVersion, database, cancellation))
            {
            }
        }

        private async Task<bool> MigrateNextAsync(long dbVersion, MigratorDatabase database, CancellationToken cancellation = default)
        {
            using (database.BeginTransactionAsync(cancellation))
            {
                dbVersion = await VersionTable.GetCurrentVersionWithLockAsync(database);

                if (dbVersion >= LatestSchemaVersion)
                    return false;

                Interceptor?.PreMigration(database.Name, dbVersion, LatestSchemaVersion);

                var migrations = Migrations
                    .Where(x => x.Version > dbVersion)
                    .ToArray();

                var firstMigration = migrations.First();

                if (firstMigration.DisableTransaction)
                {
                    using (var noTransactionDatabase = database.Clone())
                        await ExecuteMigrationAsync(migrations.First(), noTransactionDatabase, cancellation);

                    if (firstMigration.Version > dbVersion)
                        await VersionTable.SetVersionAsync(database, firstMigration.Version, cancellation);

                    await database.CommitASync(cancellation);

                    if (migrations.Length == 1)
                    {
                        Interceptor?.PostMigration(database.Name, dbVersion, LatestSchemaVersion);
                        return false;
                    }

                    return true;
                }

                foreach (var migration in migrations)
                {
                    cancellation.ThrowIfCancellationRequested();

                    if (migration.DisableTransaction)
                    {
                        await database.CommitASync(cancellation);
                        return true;
                    }

                    await ExecuteMigrationAsync(migration, database, cancellation);

                    if (migration.Version > dbVersion)
                        await VersionTable.SetVersionAsync(database, migration.Version, cancellation);
                }

                await database.CommitASync(cancellation);

                Interceptor?.PostMigration(database.Name, dbVersion, LatestSchemaVersion);

                return false;
            }
        }

        private async Task ExecuteMigrationAsync(Migration migration, MigratorDatabase database, CancellationToken cancellation = default)
        {
            Interceptor?.PreMigrationStep(database.Name, migration);
            await migration.ExecuteAsync(database, cancellation);
            Interceptor?.PostMigrationStep(database.Name, migration);
        }

        private void LoadMigrationsIfNotLoaded()
        {
            if (Migrations != null)
                return;

            lock (_migrationsResolver)
            {
                if (Migrations != null)
                    return;

                Migrations = _migrationsResolver.Resolve()
                    .OrderBy(x => x.Version)
                    .ToList();

                LatestSchemaVersion = Migrations.Any() ? Migrations.Max(x => x.Version) : 0;
            }

            Interceptor?.DetectedMigrations(Migrations.ToArray(), LatestSchemaVersion);
        }

        private async Task<long> GetDbVersionAsync(MigratorDatabase database, CancellationToken cancellation)
        {
            await VersionTable.CreateIfNotExistingAsync(database, cancellation);
            return await VersionTable.GetCurrentVersionAsync(database, cancellation);
        }
    }
}
