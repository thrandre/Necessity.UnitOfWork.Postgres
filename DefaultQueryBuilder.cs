using System.Collections.Generic;
using System.Linq;

namespace Necessity.UnitOfWork.Postgres
{
    public class DefaultQueryBuilder<TEntity, TKey> : DefaultReadOnlyQueryBuilder<TEntity, TKey>, IQueryBuilder<TEntity, TKey>
    {
        public DefaultQueryBuilder(ISchema schema) : base(schema) { }

        public virtual string Create(TEntity entity, Dictionary<string, object> queryParams)
        {
            var mapping = ExtractDynamicParameters(GetMapping(entity), queryParams);
            return GetInsertStatement(mapping);
        }

        public virtual string Update(TEntity entity, Dictionary<string, object> queryParams)
        {
            var mapping = ExtractDynamicParameters(GetMapping(entity), queryParams);
            return GetUpdateStatement(mapping);
        }

        public virtual string Upsert(TEntity entity, Dictionary<string, object> queryParams)
        {
            var mapping = ExtractDynamicParameters(GetMapping(entity), queryParams);
            return GetUpsertStatement(mapping);
        }

        public virtual string Delete(TKey key, Dictionary<string, object> queryParams)
        {
            queryParams.Add(Schema.Columns.KeyName, key);
            return GetDeleteStatement();
        }

        protected virtual string GetTypeCastForDynamicParameter(string parameterName, string dbType) =>
            !string.IsNullOrWhiteSpace(dbType)
                ? $"CAST({ parameterName } as { dbType })"
                : parameterName;

        protected virtual string FormatDynamicParameter(string parameterName) =>
            $"@{ parameterName }";

        protected virtual Dictionary<string, string> ExtractDynamicParameters(
            Dictionary<string, (string columnName, string dbType, object data)> propertyColumnDataMapping,
            Dictionary<string, object> queryParams)
        {
            var parameters = new Dictionary<string, string>();

            foreach (var propertyValue in propertyColumnDataMapping)
            {
                parameters.Add(
                    propertyValue.Value.columnName,
                    GetTypeCastForDynamicParameter(
                        FormatDynamicParameter(propertyValue.Key),
                        propertyValue.Value.dbType));

                if (!queryParams.ContainsKey(propertyValue.Key))
                {
                    queryParams.Add(propertyValue.Key, propertyValue.Value.data);
                }
            }

            return parameters;
        }

        protected virtual string GetColumnValueList(
            Dictionary<string, string> columnPropertyExpressionMapping,
            bool insert = true)
        {
            return $@"
                (
                    { string.Join(",", columnPropertyExpressionMapping.Keys) }
                ) {(insert ? "VALUES" : "=")} (
                    { string.Join(",", columnPropertyExpressionMapping.Values) }
                )
            ";
        }

        protected virtual Dictionary<string, string> GetExcludedColumns(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return columnPropertyExpressionMapping
                .Keys
                .ToDictionary(x => x, x => $"EXCLUDED.{x}");
        }

        protected virtual string GetInsertStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return $@"
                INSERT INTO { Schema.TableName }
                { GetColumnValueList(columnPropertyExpressionMapping, insert: true) }
            ";
        }

        protected virtual string GetUpdateStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            return $@"
                UPDATE { Schema.TableName }
                SET
                { GetColumnValueList(columnPropertyExpressionMapping, insert: false) }
            ";
        }

        protected virtual string GetUpsertStatement(Dictionary<string, string> columnPropertyExpressionMapping)
        {
            var insertColumnExpressions = GetInsertStatement(columnPropertyExpressionMapping);
            var updateColumnExpressions = GetColumnValueList(
                GetExcludedColumns(
                    columnPropertyExpressionMapping
                        .Where(x => x.Key != Schema.Columns.KeyName)
                        .ToDictionary(x => x.Key, x => x.Value)),
                    insert: false);

            return $@"
                { insertColumnExpressions }
                ON CONFLICT({ Schema.Columns.Mapping[Schema.Columns.KeyName].Item1 })
                DO UPDATE
                { updateColumnExpressions }
            ";
        }

        protected virtual string GetDeleteStatement()
        {
            return $@"
                DELETE
                FROM { Schema.TableName }
                WHERE { Schema.Columns.Mapping[Schema.Columns.KeyName].Item1 } = @{ Schema.Columns.KeyName }
            ";
        }

        protected virtual Dictionary<string, (string columnName, string dbType, object data)> GetMapping(TEntity entity)
        {
            return entity
                .GetType()
                .GetProperties()
                .ToDictionary(
                    p => p.Name,
                    p => (
                        Schema.Columns.Mapping[p.Name].Item1,
                        Schema.Columns.Mapping[p.Name].Item2,
                        p.GetValue(entity)));
        }
    }
}