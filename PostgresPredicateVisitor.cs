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

        private Dictionary<Type, string> _casts = new Dictionary<Type, string>
        {
            { typeof(string), "text" },
            { typeof(int), "number" },
            { typeof(long), "number" },
            { typeof(float), "decimal" },
            { typeof(double), "decimal" },
            { typeof(bool), "boolean" }
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
            return VisitPredicateInner(predicate);
        }

        private string VisitPredicateInner(Predicate predicate, bool isExpandedAlongPath = false)
        {
            if (!isExpandedAlongPath)
            {
                return VisitPredicateInner(ExpandPredicate(predicate), true);
            }

            if (predicate is PredicateGroup group)
            {
                return Pad(
                    string.Join(Pad(GetOperator(group.Op, false, null)), group.Children.Select(c => VisitPredicateInner(c, isExpandedAlongPath))),
                    "(",
                    ")");
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

            var @operator = GetOperator(binaryPredicate.Op, binaryPredicate.Negate, propertyValue);

            return leftOperand
                + Pad(@operator)
                + rightOperand;
        }

        private Predicate ExpandPredicate(Predicate predicate)
        {
            if (predicate is BinaryPredicate bp)
            {
                return !bp.Negate
                    ? Predicate
                        .Create(x =>
                            x.Group(
                                Operator.Or,
                                bp,
                                x.Group(
                                    Operator.And,
                                    x.Binary(
                                        Operator.Eq,
                                        bp.Op1.ToString(),
                                        null),
                                    x.Binary(
                                        Operator.Eq,
                                        GetDynamicParameterName(bp.Op1.ToString(), true),
                                        null))))
                    : Predicate
                        .Create(x =>
                            x.Group(
                                Operator.Or,
                                bp,
                                x.Group(
                                    Operator.And,
                                    new BinaryPredicate(
                                        Operator.Eq,
                                        bp.Op1.ToString(),
                                        null),
                                    new BinaryPredicate(
                                        Operator.Eq,
                                        GetDynamicParameterName(bp.Op1.ToString(), true),
                                        null,
                                        negate: true)),
                            x.Group(
                                    Operator.And,
                                    new BinaryPredicate(
                                        Operator.Eq,
                                        bp.Op1.ToString(),
                                        null,
                                        negate: true),
                                    new BinaryPredicate(
                                        Operator.Eq,
                                        GetDynamicParameterName(bp.Op1.ToString(), true),
                                        null,
                                        negate: false))));
            }

            return predicate;
        }

        private string GetDynamicParameterName(string propertyName, bool includePrefix)
        {
            return (includePrefix ? ParameterPrefix : string.Empty)
                + propertyName.Replace(".", "_");
        }

        private string ExtractDynamicParameter(string propertyName, object value, Dictionary<string, object> queryParams)
        {
            if (value == null)
            {
                return "NULL";
            }

            var dictionaryKey = GetDynamicParameterName(propertyName, false);

            if (!queryParams.ContainsKey(dictionaryKey))
            {
                queryParams.Add(dictionaryKey, value);
            }

            return GetDynamicParameterName(propertyName, true);
        }

        private string CastToMatchValue(string columnOrPropertyName, object value, bool castSimpleTypes)
        {
            string cast(Type type, bool castSimple)
            {
                if (_casts.ContainsKey(type))
                {
                    if (!castSimple)
                    {
                        return null;
                    }

                    return _casts[type];
                }

                if (TypeHelpers.IsJsonNetType(type))
                {
                    return "jsonb";
                }

                if (TypeHelpers.IsArrayType(type))
                {
                    return cast(type.GetElementType(), true) + "[]";
                }

                return null;
            }

            if (value == null)
            {
                return columnOrPropertyName;
            }

            var castTo = cast(value?.GetType(), castSimpleTypes);

            return castTo != null
                ? columnOrPropertyName + CastOperator + castTo
                : columnOrPropertyName;
        }

        private string CastRightOperand(string dynamicParameter, object value)
        {
            return CastToMatchValue(
                dynamicParameter,
                value,
                false);
        }

        private string GetJsonAccessOperator(string columnName, IEnumerable<string> path, object right)
        {
            return CastToMatchValue(
                columnName
                    + JsonAccessOperator
                    + Pad(string.Join(",", path),
                "'{", "}'"),
                right,
                true);
        }

        private string GetRightOperand(string dynamicParameter, object value)
        {
            return CastRightOperand(dynamicParameter, value);
        }

        private string GetLeftOperand(Operator op, string propertyName, object value)
        {
            if (propertyName.StartsWith(ParameterPrefix))
            {
                return propertyName;
            }

            var pathParts = propertyName.Split(PathSeparator);
            var propertyBaseName = pathParts.First();

            var mapping = Schema.Columns.Mapping[propertyBaseName];

            var isPropertyAccess = pathParts.Length > 1
                && mapping.NonStandardDbType == NonStandardDbType.JsonB;

            var columnName = mapping.QualifiedColumnNameForSelect(Schema);

            return isPropertyAccess
                ? GetJsonAccessOperator(columnName, pathParts.Skip(1), value)
                : columnName;
        }

        private string GetOperator(Operator op, bool negate, object value)
        {
            var @operator = DefaultOperatorMap[op];

            if (value != null && TypeHelpers.IsJsonNetType(value.GetType()) && op == Operator.Matches)
            {
                return JsonContainsOperator;
            }

            if (value != null && TypeHelpers.IsArrayType(value.GetType()) && op == Operator.Matches)
            {
                return JsonContainsOperator;
            }

            if (op == Operator.Eq && value == null)
            {
                return negate
                    ? "IS NOT"
                    : "IS";
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