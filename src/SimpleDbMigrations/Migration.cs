namespace SimpleDbMigrations
{
    public abstract class Migration
    {
        public abstract long Version { get; }
        public abstract void Execute(MigratorDatabase migratorDatabase);
        public virtual bool DisableTransaction => false;
    }
}
