using System.Collections.Generic;

namespace SimpleDbMigrations
{
    public interface IMigrationsResolver
    {
        IList<Migration> Resolve();
    }
}