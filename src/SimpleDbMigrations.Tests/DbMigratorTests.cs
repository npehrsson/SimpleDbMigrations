using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SimpleDbMigrations.DbExtensions;
using SimpleDbMigrations.Tests.Helpers;

namespace SimpleDbMigrations.Tests
{
    [TestFixture]
    public class DbMigratorTests
    {
        public EmptyDbBuilder Database { get; } = new EmptyDbBuilder();

        [SetUp]
        public async Task SetupAsync()
        {
            await Database.CreateDatabaseAsync();
        }

        [TearDown]
        public async Task TearDownAsync()
        {
            await Database.DropDatabaseAsync();
        }

        [Test]
        public async Task MigrateAsync_HaveNothing_AllIsMigrated()
        {
            var migration = new DbMigrator(typeof(MigrationFixtures.R));
            await migration.MigrateAsync(Database.ConnectionString);

            await using var cnn = await Database.ConnectionFactory.OpenAsync();
            var fruits = await cnn.QueryAsync<string>("SELECT [Name] FROM [Fruit] ORDER BY [Created] ASC");
            Assert.That(fruits, Is.EqualTo(new[] { "Banana", "Apple", "Orange" }));
        }

        [Test]
        public async Task MigrateAsync_HaveHalf_AllIsMigrated()
        {
            var resolver = new EmbeddedResourceMigrationResolver(typeof(MigrationFixtures.R));

            // Part 1
            var migration = new DbMigrator(new ListMigrationResolver(resolver.Resolve().Take(2).ToList()));
            await migration.MigrateAsync(Database.ConnectionString);

            await using var cnn = await Database.ConnectionFactory.OpenAsync();
            var fruits = await cnn.QueryAsync<string>("SELECT [Name] FROM [Fruit] ORDER BY [Created] ASC");
            Assert.That(fruits, Is.EqualTo(new [] { "Banana" }));

            // Part 2
            migration = new DbMigrator(resolver);
            await migration.MigrateAsync(Database.ConnectionString);

            await using var cnn2 = await Database.ConnectionFactory.OpenAsync();
            fruits = await cnn.QueryAsync<string>("SELECT [Name] FROM [Fruit] ORDER BY [Created] ASC");
            Assert.That(fruits, Is.EqualTo(new[] { "Banana", "Apple", "Orange" }));
        }
    }
}