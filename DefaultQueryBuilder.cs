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
                    { GetSelectClause(Schema.Columns.Mapping) }
                    { GetWhereClause(Schema.Columns.Mapping, predicate, queryParams) }
                    { GetOrderByClause(Schema.Columns.Mapping) }
                ");
        }

        public virtual string Create(TEntity entity, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetInsertStatement(Schema.Columns.Mapping);
        }

        public virtual string Update(TEntity entity, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetUpdateStatement(Schema.Columns.Mapping);
        }

        public virtual string Upsert(TEntity entity, OnConflict onConflict, QueryParameters queryParams)
        {
            ExtractValues(entity, queryParams);
            return GetUpsertStatement(Schema.Columns.Mapping, onConflict);
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
            return $@"
                ({ string.Join(",", columnParameterMap.Keys) }) 
                {(insert ? "VALUES" : "=")}
                ({ string.Join(",", columnParameterMap.Values) })
            ";
        }

        protected virtual PropertyColumnMap ExcludeColumnsForUpdate(PropertyColumnMap mapping)
        {
            return new PropertyColumnMap(
                mapping
                    .Where(x => x.Key != Schema.Columns.KeyProperty)
                    .ToDictionary(x => x.Key, x => x.Value));
        }

        protected virtual PropertyColumnMap ExcludeComputedColumnsForUpdateAndInserts(PropertyColumnMap mapping)
        {
            return new PropertyColumnMap(
                mapping
                    .Where(x => x.Value.CustomSqlExpression == null)
                    .ToDictionary(x => x.Key, x => x.Value));
        }

        protected virtual Dictionary<string, string> GetColumnParameterMap(PropertyColumnMap mapping)
        {
            return ExcludeComputedColumnsForUpdateAndInserts(mapping)
                .ToDictionary(
                    x => x.Value.ColumnName,
                    x => GetTypeCastForDynamicParameter(
                        FormatDynamicParameter(x.Key),
                        x.Value.NonStandardDbType));
        }

        protected virtual string GetInsertStatement(PropertyColumnMap mapping)
        {
            return FormatSqlStatement(
                $@"
                    INSERT INTO { Schema.TableName }
                    { GetColumnValueList(GetColumnParameterMap(mapping), insert: true) }
                ");
        }

        protected virtual string GetUpdateStatement(PropertyColumnMap mapping)
        {
            var updateColumnsMapping = ExcludeColumnsForUpdate(mapping);
            var columnsParameterMap = GetColumnParameterMap(updateColumnsMapping);

            var keyColumn = Schema.Columns.Mapping[Schema.Columns.KeyProperty].ColumnName;

            return FormatSqlStatement(
                $@"
                    UPDATE { Schema.TableName } { Schema.TableAlias }
                    SET
                    { GetColumnValueList(columnsParameterMap, insert: false) }
                    WHERE { keyColumn } = { columnsParameterMap[keyColumn] }
                ");
        }

        protected virtual string GetUpsertStatement(PropertyColumnMap mapping, OnConflict onConflict)
        {
            var insertColumnExpressions = GetInsertStatement(mapping);

            var updateColumnsMapping = ExcludeColumnsForUpdate(mapping);
            var updateColumnExpressions = GetColumnValueList(
                GetColumnParameterMap(updateColumnsMapping)
                    .ToDictionary(x => x.Key, x => $"EXCLUDED.{x.Key}"));

            var keyColumn = Schema.Columns.Mapping[Schema.Columns.KeyProperty].ColumnName;

            var sql = insertColumnExpressions;
            var onConflictExpression = $"ON CONFLICT({ keyColumn })";

            switch (onConflict)
            {
                case OnConflict.Update:
                    sql += $@"
                    { onConflictExpression }
                    DO UPDATE
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

        protected virtual string GetSelectClause(PropertyColumnMap mapping)
        {
            return
                $@"
                    SELECT
                    { GetColumnList(mapping.Values.Select(v => v.CustomSqlExpression ?? v.ColumnName)) }
                    FROM { Schema.TableName } { Schema.TableAlias }
                    { string.Join(Environment.NewLine, Schema.Joins) }
                ";
        }

        protected virtual string GetWhereClause(PropertyColumnMap mapping, Predicate predicate, QueryParameters queryParams)
        {
            var sqlWhere = predicate?.Accept(
                    new PostgresPredicateVisitor(
                        p => mapping[p],
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

            return $"ORDER BY { mapping[Schema.DefaultOrderBy.propertyName].ColumnName } { (Schema.DefaultOrderBy.direction == OrderDirection.Ascending ? "ASC" : "DESC") }";
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