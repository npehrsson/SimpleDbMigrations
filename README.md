| --- | --- |
| **Build** | [![Build status](https://ci.appveyor.com/api/projects/status/jk7yhplpwmap5agk/branch/master?svg=true)](https://ci.appveyor.com/project/tygerbytes/resourcefitness) |
| **NuGet** | [![nuget](https://img.shields.io/nuget/v/SimpleDbMigrations.svg)](https://www.nuget.org/packages/SimpleDbMigrations/)


1. Add .sql-files in your project as manifest resources
2. Add *R.cs* to the same folder as the .sql-files
```C#
public sealed class R {
	private R() {
		/* Dummy. Placeholder for finding manifest resource prefix. */
	}
}
```
3. Initialize and call migrator during start-up of your program
```C#
var migrator = new DbMigrator("dbo", typeof(R));
migrator.Migrate(connectionString);
``` 
