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
        public DbMigrator(string schemaName, IMigrationsResolver migrationsResolver)
        {
            if (migrationsResolver == null) throw new ArgumentNullException(nameof(migrationsResolver));

            SchemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
            Migrations = migrationsResolver.Resolve()
                .OrderBy(x => x.Version)
                .ToList();

            VersionTable = new CachedDatabaseVersionTable(new DatabaseVersionTable(SchemaName));
            LatestSchemaVersion = Migrations.Any() ?
                Migrations.Max(x => x.Version) :
                0;
        }

        public DbMigrator(string schemaName, Assembly assembly, string manifestPath)
            : this(schemaName, new EmbeddedResourceMigrationResolver(assembly, manifestPath)) { }

        public DbMigrator(string schemaName, Type type)
            : this(schemaName, new EmbeddedResourceMigrationResolver(type)) { }

        private string SchemaName { get; }
        private long LatestSchemaVersion { get; }
        private CachedDatabaseVersionTable VersionTable { get; }
        private IList<Migration> Migrations { get; }

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
            {
                throw new ArgumentNullException(nameof(connection));
            }

            var database = new MigratorDatabase(connection);
            var dbVersion = GetDbVersion(database);

            if (dbVersion >= LatestSchemaVersion)
            {
                return;
            }

            Migrate(dbVersion, database);
        }

        private void Migrate(long dbVersion, MigratorDatabase database)
        {
            using (var transaction = database.BeginTransaction())
            {
                dbVersion = VersionTable.GetCurrentVersionWithLock(database);

                if (dbVersion >= LatestSchemaVersion)
                {
                    return;
                }

                var migrations = Migrations.Where(x => x.Version > dbVersion);

                foreach (var migration in migrations)
                {
                    migration.Execute(database);

                    if (migration.Version > dbVersion)
                    {
                        VersionTable.SetVersion(database, migration.Version);
                    }
                }

                transaction.Commit();
            }
        }

        private long GetDbVersion(MigratorDatabase database)
        {
            VersionTable.CreateIfNotExisting(database);
            return VersionTable.GetCurrentVersion(database);
        }
    }
}
