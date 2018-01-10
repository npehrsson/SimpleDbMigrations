using System;

namespace SimpleDbMigrations
{
    public class SqlQuery<T>
    {
        private readonly string _command;
        private readonly MigratorDatabase _migratorDatabase;

        public SqlQuery(string command, MigratorDatabase migratorDatabase)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _migratorDatabase = migratorDatabase ?? throw new ArgumentNullException(nameof(migratorDatabase));
        }

        public int CommandTimeout { get; set; } = 30;

        public T FirstOrDefault()
        {
            using (var command = _migratorDatabase.CreateCommand())
            {
                command.CommandText = _command;
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return (T)reader.GetValue(0);
                    }

                    return default(T);
                }
            }
        }

        public T Single()
        {
            using (var command = _migratorDatabase.CreateCommand())
            {
                command.CommandText = _command;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new InvalidOperationException("Sequence contained zero items");
                    }

                    var value = (T)reader.GetValue(0);

                    if (reader.Read())
                    {
                        throw new InvalidOperationException("Sequence contained more than one item");
                    }

                    return value;
                }
            }
        }
    }
}
