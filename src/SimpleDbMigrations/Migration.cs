using System.Threading;
using System.Threading.Tasks;

namespace SimpleDbMigrations
{
    public abstract class Migration
    {
        public abstract long Version { get; }
        public abstract Task ExecuteAsync(MigratorDatabase database, CancellationToken cancellation = default);
        public virtual bool DisableTransaction => false;
    }
}
