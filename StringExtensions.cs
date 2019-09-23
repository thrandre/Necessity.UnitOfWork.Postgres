using System;
using System.Linq;

namespace Necessity.UnitOfWork.Postgres
{
    public static class StringExtensions
    {
        public static string ToSnakeCase(this string pascalOrCamelCasedString)
        {
            return string.Concat(
                pascalOrCamelCasedString
                    .Select((x, i) =>
                        i > 0 && char.IsUpper(x)
                            ? "_" + x.ToString()
                            : x.ToString()))
                .ToLower();
        }

        public static string TrimEnd(
            this string input,
            string suffixToRemove,
            StringComparison comparisonType)
        {
            return !string.IsNullOrWhiteSpace(input)
                && !string.IsNullOrWhiteSpace(suffixToRemove)
                && input.EndsWith(suffixToRemove, comparisonType)
                    ? input.Substring(0, input.Length - suffixToRemove.Length)
                    : input;
        }
    }
}