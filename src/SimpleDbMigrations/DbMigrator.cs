using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

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

        public void Migrate(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            Migrate(new MigratorDatabase(connectionString));
        }

        [Obsolete("Using this can result in issues if using no-transaction migrations, use Migrate(string connectionString) instead")]
        public void Migrate(IDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            Migrate(new MigratorDatabase(connection));
        }

        private void Migrate(MigratorDatabase database)
        {
            LoadMigrationsIfNotLoaded();

            var dbVersion = GetDbVersion(database);

            if (dbVersion >= LatestSchemaVersion)
                return;

            while (MigrateNext(dbVersion, database)) { };
        }

        private bool MigrateNext(long dbVersion, MigratorDatabase database)
        {
            using (database.BeginTransaction())
            {
                dbVersion = VersionTable.GetCurrentVersionWithLock(database);

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
                        ExecuteMigration(migrations.First(), noTransactionDatabase);

                    if (firstMigration.Version > dbVersion)
                        VersionTable.SetVersion(database, firstMigration.Version);

                    database.Commit();

                    if (migrations.Length == 1)
                    {
                        Interceptor?.PostMigration(database.Name, dbVersion, LatestSchemaVersion);
                        return false;
                    }

                    return true;
                }

                foreach (var migration in migrations)
                {
                    if (migration.DisableTransaction)
                    {
                        database.Commit();
                        return true;
                    }

                    ExecuteMigration(migration, database);

                    if (migration.Version > dbVersion)
                        VersionTable.SetVersion(database, migration.Version);
                }

                database.Commit();

                Interceptor?.PostMigration(database.Name, dbVersion, LatestSchemaVersion);

                return false;
            }
        }

        private void ExecuteMigration(Migration migration, MigratorDatabase database)
        {
            Interceptor?.PreMigrationStep(database.Name, migration);
            migration.Execute(database);
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

        private long GetDbVersion(MigratorDatabase database)
        {
            VersionTable.CreateIfNotExisting(database);
            return VersionTable.GetCurrentVersion(database);
        }
    }
}
