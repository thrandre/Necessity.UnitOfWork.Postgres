using System;
using System.Collections.Generic;
using System.Linq;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres.Schema
{
    public class Convention
    {
        public static ISchema GetPostgresSchema<TEntity>(Action<ConventionOptions> configure = null, Action<ByConventionSchema> postConfigure = null)
        {
            var options = new ConventionOptions();

            configure?.Invoke(options);

            var entityType = typeof(TEntity);
            var tableName = GetTableName(entityType, options.PluralizeTableNames);
            var properties = GetPropertyColumnMapping(entityType);
            var primaryKey = GetPrimaryKey(properties.Values.Select(v => v.columnName));

            var schema = new ByConventionSchema(
                tableName,
                new ByConventionSchemaColumns(primaryKey, properties));

            postConfigure?.Invoke(schema);

            return schema;
        }

        private static string[] PrimaryKeyCandidates => new[] { "id", "key" };

        private static string GetTableName(Type entityType, bool pluralizeTableNames)
        {
            var tableName = entityType
                .Name
                .TrimEnd("entity", StringComparison.OrdinalIgnoreCase)
                .ToSnakeCase();

            return pluralizeTableNames
                ? tableName + "s"
                : tableName;
        }

        private static string GuessColumnDbType(Type propertyType)
        {
            return TypeHelpers
                .IsJsonNetType(propertyType)
                    ? "jsonb"
                    : null;
        }

        private static Dictionary<string, (string columnName, string dbType)> GetPropertyColumnMapping(Type entityType)
        {
            return entityType
                .GetProperties()
                .ToDictionary(
                    x => x.Name,
                    x => (x.Name.ToSnakeCase(), GuessColumnDbType(x.PropertyType)),
                    StringComparer.OrdinalIgnoreCase);
        }

        private static string GetPrimaryKey(IEnumerable<string> columnNames)
        {
            return columnNames.First(c => PrimaryKeyCandidates.Contains(c));
        }
    }

    public class ByConventionSchema : ISchema
    {
        public ByConventionSchema(string tableName, ByConventionSchemaColumns columns)
        {
            Columns = columns;
            TableName = tableName;
        }

        public string TableName { get; set; }
        public ByConventionSchemaColumns Columns { get; }

        ISchemaColumns ISchema.Columns => Columns;
    }

    public class ByConventionSchemaColumns : ISchemaColumns
    {
        public ByConventionSchemaColumns(string keyName, Dictionary<string, (string, string)> mapping)
        {
            KeyName = keyName;
            Mapping = mapping;
        }

        public string KeyName { get; set; }
        public Dictionary<string, (string columnName, string dbType)> Mapping { get; }
    }
}