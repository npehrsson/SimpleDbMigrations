using System;
using System.Collections.Generic;
using System.Linq;

namespace SimpleDbMigrations
{
    public class ListMigrationResolver : IMigrationsResolver
    {
        private readonly ICollection<Migration> _migrations;
        public ListMigrationResolver(ICollection<Migration> migrations) => _migrations = migrations ?? throw new ArgumentNullException(nameof(migrations));
        public IList<Migration> Resolve() => _migrations.ToList();
    }
}