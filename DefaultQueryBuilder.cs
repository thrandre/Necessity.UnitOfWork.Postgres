using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Data.Predicates;
using Necessity.UnitOfWork.Postgres.Schema;

namespace Necessity.UnitOfWork.Postgres
{
    public class DefaultQueryBuilder<TEntity, TKey> : IQueryBuilder<TEntity, TKey>
    {
        public DefaultQueryBuilder(ISchema schema)
        {
            Schema = schema;
        }

        public ISchema Schema { get; }

        public virtual string Get(TKey key, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            queryParams.Add(Schema.Columns.KeyName, key);

            return FormatSqlStatement(
                $@"
                    SELECT { GetColumnList(mapping) }
                    FROM { Schema.TableName }
                    WHERE { GetDefaultFilterExpression(mapping) }
                ");
        }

        public string Find(Predicate predicate, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            var sqlWhere = predicate?
                .Accept(new PostgresPredicateVisitor(p => Schema.Columns.Mapping[p], queryParams));

            return FormatSqlStatement(
                $@"
                    SELECT { GetColumnList(mapping) }
                    FROM { Schema.TableName }
                    {(predicate != null
                        ? $"WHERE { sqlWhere }"
                        : "")}
                ");
        }

        public virtual string GetAll(Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            return FormatSqlStatement(
                $@"
                    SELECT { GetColumnList(mapping) }
                    FROM { Schema.TableName }
                ");
        }

        public virtual string Create(TEntity entity, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            ExtractValues(entity, queryParams);

            return GetInsertStatement(mapping);
        }

        public virtual string Update(TEntity entity, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            ExtractValues(entity, queryParams);

            return GetUpdateStatement(mapping);
        }

        public virtual string Upsert(TEntity entity, OnConflict onConflict, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            ExtractValues(entity, queryParams);

            return GetUpsertStatement(mapping, onConflict);
        }

        public virtual string Delete(TKey key, Dictionary<string, object> queryParams)
        {
            var mapping = ReverseMap(Schema.Columns.Mapping);

            queryParams.Add(mapping[Schema.Columns.KeyName], key);

            return GetDeleteStatement(mapping);
        }

        protected virtual string GetTypeCastForDynamicParameter(string parameterName, string dbType) =>
            !string.IsNullOrWhiteSpace(dbType)
                ? $"CAST({ parameterName } as { dbType })"
                : parameterName;

        protected virtual string FormatDynamicParameter(string parameterName) =>
            $"@{ parameterName }";

        protected virtual Dictionary<string, string> ReverseMap(Dictionary<string, (string columnName, string dbType)> propertyColumnDataMapping)
        {
            return propertyColumnDataMapping
                .ToDictionary(
                    x => x.Value.columnName,
                    x => GetTypeCastForDynamicParameter(
                        FormatDynamicParameter(x.Key),
                        x.Value.dbType));
        }

        protected virtual string GetColumnList(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return string.Join(",", columnPropertyExpressionMapping.Keys);
        }

        protected virtual string GetColumnValueList(
            Dictionary<string, string> columnPropertyExpressionMapping,
            bool insert = true)
        {
            return $@"
                ({ string.Join(",", columnPropertyExpressionMapping.Keys) }) 
                {(insert ? "VALUES" : "=")}
                ({ string.Join(",", columnPropertyExpressionMapping.Values) })
            ";
        }

        protected virtual Dictionary<string, string> GetExcludedColumns(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return columnPropertyExpressionMapping
                .Keys
                .ToDictionary(x => x, x => $"EXCLUDED.{x}");
        }

        protected virtual Dictionary<string, string> ExcludeColumnsForUpdate(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return columnPropertyExpressionMapping
                .Where(x => x.Key != Schema.Columns.KeyName)
                .ToDictionary(x => x.Key, x => x.Value);
        }

        protected virtual string GetInsertStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return FormatSqlStatement(
                $@"
                    INSERT INTO { Schema.TableName }
                    { GetColumnValueList(columnPropertyExpressionMapping, insert: true) }
                ");
        }

        protected virtual string GetUpdateStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            var updateColumns = ExcludeColumnsForUpdate(columnPropertyExpressionMapping);

            return FormatSqlStatement(
                $@"
                    UPDATE { Schema.TableName }
                    SET
                    { GetColumnValueList(updateColumns, insert: false) }
                    WHERE { Schema.Columns.KeyName } = { columnPropertyExpressionMapping[Schema.Columns.KeyName] }
                ");
        }

        protected virtual string GetUpsertStatement(Dictionary<string, string> columnPropertyExpressionMapping, OnConflict onConflict)
        {
            var insertColumnExpressions = GetInsertStatement(columnPropertyExpressionMapping);
            var updateColumnExpressions = GetColumnValueList(
                GetExcludedColumns(
                    ExcludeColumnsForUpdate(columnPropertyExpressionMapping)),
                    insert: false);

            var sql = insertColumnExpressions;
            var onConflictExpression = $"ON CONFLICT({ Schema.Columns.KeyName })";

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

        protected virtual string GetDeleteStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return FormatSqlStatement(
                $@"
                    DELETE
                    FROM { Schema.TableName }
                    WHERE { GetDefaultFilterExpression(columnPropertyExpressionMapping) }
                ");
        }

        protected virtual string GetDefaultFilterExpression(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return $"{ Schema.Columns.KeyName } = { columnPropertyExpressionMapping[Schema.Columns.KeyName] }";
        }

        protected virtual string FormatSqlStatement(string rawStatement)
        {
            return Regex.Replace(
                Regex.Replace(rawStatement, @"\t|\n|\r", ""),
                @"\s+",
                " ");
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