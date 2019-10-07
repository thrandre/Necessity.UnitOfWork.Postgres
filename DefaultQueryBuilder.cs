using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Necessity.UnitOfWork.Predicates;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres
{
    using QueryParameters = Dictionary<string, object>;

    public class DefaultQueryBuilder<TEntity, TKey> : IQueryBuilder<TEntity, TKey>
    {
        public DefaultQueryBuilder(ISchema schema)
        {
            Schema = schema;
        }

        public ISchema Schema { get; }

        public virtual string Get(TKey key, QueryParameters queryParams)
        {
            return Find(
                Predicate.Create(x =>
                    x.Binary(Operator.Eq, Schema.Columns.KeyProperty, key)),
                queryParams);
        }

        public virtual string GetAll(QueryParameters queryParams)
        {
            return Find(null, queryParams);
        }

        public string Find(Predicate predicate, QueryParameters queryParams)
        {
            return FormatSqlStatement(
                 $@"
                    { GetSelectClause(Schema.Columns.Mapping, queryParams) }
                    { GetWhereClause(Schema.Columns.Mapping, predicate, queryParams) }
                    { GetOrderByClause(Schema.Columns.Mapping) }
                ");
        }

        public virtual string Create(TEntity entity, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetInsertStatement(Schema.Columns.Mapping, queryParams);
        }

        public virtual string Update(TEntity entity, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetUpdateStatement(Schema.Columns.Mapping, queryParams);
        }

        public virtual string Upsert(TEntity entity, OnConflict onConflict, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetUpsertStatement(Schema.Columns.Mapping, onConflict, queryParams);
        }

        public virtual string Delete(TKey key, QueryParameters queryParams)
        {
            var predicate = Predicate
                .Create(x =>
                    x.Binary(Operator.Eq, Schema.Columns.KeyProperty, key));

            return FormatSqlStatement(
                $@"
                    DELETE
                    FROM { Schema.TableName }
                    { GetWhereClause(Schema.Columns.Mapping, predicate, queryParams) }
                ");
        }

        protected virtual string GetTypeCastForDynamicParameter(string parameterName, NonStandardDbType? dbType)
        {
            return dbType != null
                ? $"CAST({ parameterName } as jsonb)"
                : parameterName;
        }

        protected virtual string FormatDynamicParameter(string parameterName)
        {
            return $"@{ parameterName }";
        }

        protected virtual string GetColumnList(IEnumerable<string> columns)
        {
            return string.Join(",", columns);
        }

        protected virtual string GetColumnValueList(
            Dictionary<string, string> columnParameterMap,
            bool insert = true)
        {
            if (insert)
            {
                return $@"
                    ({ string.Join(",", columnParameterMap.Keys) }) 
                    VALUES
                    ({ string.Join(",", columnParameterMap.Values) })
                ";
            }

            return string.Join(
                ",",
                columnParameterMap.Select(p => $"{p.Key}={p.Value}"));
        }

        protected virtual PropertyColumnMap ExcludeColumnsForUpdate(PropertyColumnMap mapping)
        {
            return new PropertyColumnMap(
                mapping
                    .Where(x => x.Key != Schema.Columns.KeyProperty && x.Value.OnInsert == null)
                    .ToDictionary(x => x.Key, x => x.Value));
        }

        protected virtual PropertyColumnMap ExcludeComputedColumnsForUpdateAndInserts(PropertyColumnMap mapping)
        {
            return new PropertyColumnMap(
                mapping
                    .Where(x => x.Value.OnSelect == null)
                    .ToDictionary(x => x.Key, x => x.Value));
        }

        protected virtual Dictionary<string, string> GetColumnParameterMap(PropertyColumnMap mapping, QueryParameters queryParams)
        {
            return ExcludeComputedColumnsForUpdateAndInserts(mapping)
                .ToDictionary(
                    x => x.Value.ColumnName,
                    x => x.Value.OnInsert?.Invoke(queryParams)
                        ?? GetTypeCastForDynamicParameter(
                            FormatDynamicParameter(x.Key),
                            x.Value.NonStandardDbType));
        }

        protected virtual string GetInsertStatement(PropertyColumnMap mapping, QueryParameters queryParams)
        {
            return FormatSqlStatement(
                $@"
                    INSERT INTO { Schema.TableName }
                    { GetColumnValueList(GetColumnParameterMap(mapping, queryParams), insert: true) }
                ");
        }

        protected virtual string GetUpdateStatement(PropertyColumnMap mapping, QueryParameters queryParameters)
        {
            var columnsParameterMap = GetColumnParameterMap(mapping, queryParameters);
            var updateColumnsParameterMap = GetColumnParameterMap(ExcludeColumnsForUpdate(mapping), queryParameters);

            var keyColumn = Schema.Columns.Mapping[Schema.Columns.KeyProperty].ColumnName;

            return FormatSqlStatement(
                $@"
                    UPDATE { Schema.TableName } { Schema.TableAlias }
                    SET
                    { GetColumnValueList(updateColumnsParameterMap, insert: false) }
                    WHERE { keyColumn } = { columnsParameterMap[keyColumn] }
                ");
        }

        protected virtual string GetUpsertStatement(PropertyColumnMap mapping, OnConflict onConflict, QueryParameters queryParameters)
        {
            var insertColumnExpressions = GetInsertStatement(mapping, queryParameters);

            var updateColumnsMapping = ExcludeColumnsForUpdate(mapping);
            var updateColumnExpressions = GetColumnValueList(
                GetColumnParameterMap(updateColumnsMapping, queryParameters)
                    .ToDictionary(x => x.Key, x => $"EXCLUDED.{x.Key}"), insert: false);

            var keyColumn = Schema.Columns.Mapping[Schema.Columns.KeyProperty].ColumnName;

            var sql = insertColumnExpressions;
            var onConflictExpression = $"ON CONFLICT({ keyColumn })";

            switch (onConflict)
            {
                case OnConflict.Update:
                    sql += $@"
                    { onConflictExpression }
                    DO UPDATE
                    SET
                    { updateColumnExpressions }
                ";
                    break;

                case OnConflict.DoNothing:
                    sql += $@"
                    { onConflictExpression }
                    DO NOTHING";
                    break;

                default:
                    break;
            }

            return FormatSqlStatement(sql);
        }

        protected virtual string GetSelectClause(PropertyColumnMap mapping, QueryParameters queryParams)
        {
            return
                $@"
                    SELECT
                    { GetColumnList(mapping.Values.Select(v => v.OnSelect?.Invoke(queryParams) ?? v.QualifiedColumnNameForSelect(Schema))) }
                    FROM { Schema.TableName } { Schema.TableAlias }
                    { string.Join(Environment.NewLine, Schema.Joins.Select(j => j.JoinExpression)) }
                ";
        }

        protected virtual string GetWhereClause(PropertyColumnMap mapping, Predicate predicate, QueryParameters queryParams)
        {
            var sqlWhere = predicate?.Accept(
                    new PostgresPredicateVisitor(
                        Schema,
                        queryParams));

            return sqlWhere != null
                ? $"WHERE { sqlWhere }"
                : string.Empty;
        }

        protected virtual string GetOrderByClause(PropertyColumnMap mapping)
        {
            if (string.IsNullOrWhiteSpace(Schema.DefaultOrderBy.propertyName))
            {
                return string.Empty;
            }

            return $"ORDER BY { mapping[Schema.DefaultOrderBy.propertyName].QualifiedColumnNameForSelect(Schema) } { (Schema.DefaultOrderBy.direction == OrderDirection.Ascending ? "ASC" : "DESC") }";
        }

        protected virtual string FormatSqlStatement(string rawStatement)
        {
            return Regex
                .Replace(
                    Regex.Replace(rawStatement, @"\t|\n|\r", ""),
                    @"\s+",
                    " ")
                .Trim();
        }

        protected virtual void ExtractValues<TObject>(TObject @object, Dictionary<string, object> queryParams)
        {
            typeof(TObject)
                .GetProperties()
                .ToList()
                .ForEach(p => queryParams.Add(p.Name, p.GetValue(@object)));
        }
    }
}