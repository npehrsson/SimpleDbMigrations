using System;
using System.Data;

namespace SimpleDbMigrations {
    public class MigratorDatabase {
        public MigratorDatabase(IDbConnection connection) {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        private IDbConnection Connection { get; }
        public string Name => Connection.Database;
        public IDbTransaction Transaction { get; private set; }

        public IDbTransaction BeginTransaction() {
            if (Transaction != null) {
                throw new InvalidOperationException("Cannot open a transaction twice");
            }

            OpenIfClosed();

            Transaction = Connection.BeginTransaction();
            return Transaction;
        }

        public SqlQuery<T> SqlQuery<T>(string command, int commandTimeout = 30) {
            OpenIfClosed();
            return new SqlQuery<T>(command, this) { CommandTimeout = commandTimeout };
        }

        public int ExecuteSqlCommand(string commandText) {
            OpenIfClosed();
            using (var command = CreateCommand()) {
                command.CommandText = commandText;
                return command.ExecuteNonQuery();
            }
        }

        public IDbCommand CreateCommand() {
            OpenIfClosed();
            var command = Connection.CreateCommand();
            command.Transaction = Transaction;
            return command;
        }

        private void OpenIfClosed() {
            if (Connection.State != ConnectionState.Closed) {
                return;
            }

            Connection.Open();
        }
    }
}
