using NUnit.Framework;

namespace SimpleDbMigrations.Tests
{
    [TestFixture]
    public class EmbeddedResourceMigrationResolverTests
    {
        [Test]
        public void Resolve()
        {
            var sut = new EmbeddedResourceMigrationResolver(typeof(MigrationFixtures.R));
            var actual = sut.Resolve();

            Assert.That(actual, Has.Count.EqualTo(4), "actual");
            Assert.That(actual, Does.Not.Contain(null), "actual");

            Assert.Multiple(() =>
            {
                Assert.That(actual[0].Version, Is.EqualTo(1L), "actual[0].Version");
                Assert.That(actual[0].DisableTransaction, Is.False, "actual[0].DisableTransaction");

                Assert.That(actual[1].Version, Is.EqualTo(2L), "actual[1].Version");
                Assert.That(actual[1].DisableTransaction, Is.False, "actual[1].DisableTransaction");

                Assert.That(actual[2].Version, Is.EqualTo(3L), "actual[2].Version");
                Assert.That(actual[2].DisableTransaction, Is.True, "actual[2].DisableTransaction");

                Assert.That(actual[3].Version, Is.EqualTo(4L), "actual[3].Version");
                Assert.That(actual[3].DisableTransaction, Is.False, "actual[3].DisableTransaction");
            });
        }
    }
}