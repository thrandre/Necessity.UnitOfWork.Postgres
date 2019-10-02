using System;
using System.Collections.Generic;
using System.Linq;
using Necessity.UnitOfWork.Predicates;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres
{
    public class PostgresPredicateVisitor : IPredicateVisitor<string>
    {
        public static Dictionary<Operator, Tuple<string, string>> DefaultOperatorMap { get; } =
            new Dictionary<Operator, Tuple<string, string>>
            {
                {Operator.And, Tuple.Create("AND", string.Empty)},
                {Operator.Or, Tuple.Create("OR", string.Empty)},
                {Operator.Eq, Tuple.Create("=", "!=")},
                {Operator.Matches, Tuple.Create("LIKE", "NOT LIKE")},
                {Operator.Gt, Tuple.Create(">", string.Empty)},
                {Operator.Gte, Tuple.Create(">=", string.Empty)},
                {Operator.Lt, Tuple.Create("<", string.Empty)},
                {Operator.Lte, Tuple.Create("<=", string.Empty)},
                {Operator.In, Tuple.Create("IN", "NOT IN")}
            };
        private const string JsonAccessOperator = "#>>";
        private const string JsonContainsOperator = "@>";
        private const string CastOperator = "::";
        private const char PathSeparator = '.';
        private const string ParameterPrefix = "@";

        public PostgresPredicateVisitor(
            ISchema schema,
            Dictionary<string, object> queryParams)
        {
            Schema = schema;
            QueryParams = queryParams;
        }

        public Dictionary<string, object> QueryParams { get; }

        private ISchema Schema { get; }

        public string VisitPredicate(Predicate predicate)
        {
            if (predicate is PredicateGroup group)
            {
                var expr = string.Join(Pad(GetOperator(group.Op, false)), group.Children.Select(c => VisitPredicate(c)));
                return group.Op == Operator.Or
                    ? Pad(expr, "(", ")")
                    : expr;
            }

            var binaryPredicate = predicate as BinaryPredicate;

            var propertyName = binaryPredicate.Op1.ToString();
            var propertyValue = binaryPredicate.Op2;

            var dynamicParameter = ExtractDynamicParameter(
                propertyName,
                propertyValue,
                QueryParams);

            var rightOperand = GetRightOperand(dynamicParameter, propertyValue);
            var leftOperand = GetLeftOperand(binaryPredicate.Op, propertyName, propertyValue);

            var @operator = GetOperator(binaryPredicate.Op, binaryPredicate.Negate,
                propertyValue != null && TypeHelpers.IsJsonNetType(propertyValue.GetType()));

            if (binaryPredicate.Op == Operator.Eq)
            {
                var equals = leftOperand + Pad(@operator) + rightOperand;
                var nullChecks = $"ROW({rightOperand},{leftOperand}) IS NULL";

                return Pad(
                    string.Join(Pad(GetOperator(Operator.Or, false)), equals, nullChecks),
                    "(", ")"
                );
            }
            return leftOperand
                + Pad(@operator)
                + rightOperand;
        }

        private string ExtractDynamicParameter(string propertyName, object value, Dictionary<string, object> queryParams)
        {
            var dynamicParameterName = propertyName.Replace(".", "_");

            queryParams.Add(dynamicParameterName, value);

            return "@" + dynamicParameterName;
        }

        private string CastToMatchValue(string columnOrPropertyName, object value, string[] validCasts = null)
        {
            string cast(object v)
            {
                switch (value)
                {
                    case int _:
                    case long _:
                        return "integer";

                    case float _:
                    case double _:
                        return "decimal";

                    case bool b:
                        return "boolean";

                    case object o when TypeHelpers.IsJsonNetType(o.GetType()):
                        return "jsonb";

                    default:
                        return null;
                }
            };

            var castTo = cast(value);

            return castTo != null && (validCasts?.Contains(castTo) ?? true)
                ? columnOrPropertyName + CastOperator + castTo
                : columnOrPropertyName;
        }

        private string CastRightOperand(string dynamicParameter, object value)
        {
            return CastToMatchValue(
                dynamicParameter,
                value,
                new[] { "jsonb" });
        }

        private string GetJsonAccessOperator(string columnName, IEnumerable<string> path, object right)
        {
            return CastToMatchValue(
                columnName
                    + JsonAccessOperator
                    + Pad(string.Join(",", path),
                "'{", "}'"),
                right);
        }

        private string GetRightOperand(string dynamicParameter, object value)
        {
            return CastRightOperand(dynamicParameter, value);
        }

        private string GetLeftOperand(Operator op, string propertyName, object value)
        {
            var pathParts = propertyName.Split(PathSeparator);
            var propertyBaseName = pathParts.First();

            var mapping = Schema.Columns.Mapping[propertyBaseName];

            var isPropertyAccess = pathParts.Length > 1
                && mapping.NonStandardDbType == NonStandardDbType.JsonB;

            var columnName = mapping.QualifiedColumnName(Schema);

            return isPropertyAccess
                ? GetJsonAccessOperator(columnName, pathParts.Skip(1), value)
                : columnName;
        }

        private string GetOperator(Operator op, bool negate, bool isJsonColumn = false)
        {
            var @operator = DefaultOperatorMap[op];

            if (isJsonColumn && op == Operator.Matches)
            {
                return JsonContainsOperator;
            }

            return negate
                ? @operator.Item2
                : @operator.Item1;
        }

        private string Pad(string paddable, string padLeft = " ", string padRight = null)
        {
            return padLeft + paddable + (padRight ?? padLeft);
        }
    }
}
