using System;
using System.Data;
using Microsoft.Data.SqlClient;

namespace SimpleDbMigrations
{
    public class MigratorDatabase : IDisposable
    {
        public MigratorDatabase(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public MigratorDatabase(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            Connection = new SqlConnection(connectionString);
            ConnectionString = connectionString;
        }

        private string ConnectionString { get; set; }
        private IDbConnection Connection { get; }
        public string Name => Connection.Database;
        private IDbTransaction Transaction { get; set; }

        public IDisposable BeginTransaction()
        {
            if (Transaction != null) 
                throw new InvalidOperationException("Cannot open a transaction twice");

            OpenIfClosed();

            Transaction = Connection.BeginTransaction();
            return Transaction;
        }

        public SqlQuery<T> SqlQuery<T>(string command, int commandTimeout = 30) 
        {
            OpenIfClosed();
            return new SqlQuery<T>(command, this) { CommandTimeout = commandTimeout };
        }

        public int ExecuteSqlCommand(string commandText)
        {
            OpenIfClosed();
            using (var command = CreateCommand())
            {
                command.CommandText = commandText;
                return command.ExecuteNonQuery();
            }
        }

        public IDbCommand CreateCommand()
        {
            OpenIfClosed();
            var command = Connection.CreateCommand();
            command.Transaction = Transaction;
            return command;
        }

        public void Commit()
        {
            Transaction.Commit();
            Transaction = null;
        }

        public MigratorDatabase Clone() => new MigratorDatabase(new SqlConnection(ConnectionString));
        
        private void OpenIfClosed()
        {
            if (Connection.State != ConnectionState.Closed)
                return;

            Connection.Open();
        }

        public void Dispose()
        {
            Connection?.Dispose();
            Transaction?.Dispose();
        }
    }
}
