using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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
            using (var connection = new SqlConnection(connectionString))
            {
                Migrate(connection);
            }
        }

        public void Migrate(IDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            LoadMigrationsIfNotLoaded();

            var database = new MigratorDatabase(connection);
            var dbVersion = GetDbVersion(database);

            if (dbVersion >= LatestSchemaVersion)
                return;

            Migrate(dbVersion, database);
        }

        private void Migrate(long dbVersion, MigratorDatabase database)
        {
            using (var transaction = database.BeginTransaction())
            {
                dbVersion = VersionTable.GetCurrentVersionWithLock(database);

                if (dbVersion >= LatestSchemaVersion)
                    return;

                Interceptor?.PreMigration(database.Name, dbVersion, LatestSchemaVersion);

                var migrations = Migrations.Where(x => x.Version > dbVersion);

                foreach (var migration in migrations)
                {
                    Interceptor?.PreMigrationStep(database.Name, migration);
                    migration.Execute(database);
                    Interceptor?.PostMigrationStep(database.Name, migration);

                    if (migration.Version > dbVersion)
                        VersionTable.SetVersion(database, migration.Version);
                }

                transaction.Commit();

                Interceptor?.PostMigration(database.Name, dbVersion, LatestSchemaVersion);
            }
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
