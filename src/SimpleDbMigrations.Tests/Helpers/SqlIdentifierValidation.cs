using System;
using System.Text.RegularExpressions;

namespace SimpleDbMigrations.Tests.Helpers
{
    public static class SqlIdentifierValidation
    {
        private static readonly Regex DatabaseName = new Regex(@"^[A-Za-z0-9\-_]{1,127}$");

        public static bool IsValid(string value) => DatabaseName.IsMatch(value);

        public static void ThrowIfInvalid(string value)
        {
            if (!IsValid(value))
                throw new FormatException("Supplied database name is not valid: " + value);
        }
    }
}