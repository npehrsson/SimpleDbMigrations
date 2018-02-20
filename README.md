1. Add .sql-files in your project as manifest resources
2. Add *R.cs* to the same folder as the .sql-files

    public sealed class R {
      private R() {
        /* Dummy. Placeholder for finding manifest resource prefix. */
      }
    }

3. Initialize and call migrator during start-up of your program

    var migrator = new DbMigrator("dbo", typeof(R));
    migrator.Migrate(connectionString);
