using System;
using System.Collections.Generic;
using System.Linq;

namespace Necessity.UnitOfWork.Postgres.Schema
{
    public class ByConvention
    {
        public static ISchema CreateSchema<TEntity>(Action<ByConventionSchema> configure = null)
        {
            var entityType = typeof(TEntity);
            var tableName = GetTableName(entityType);
            var properties = GetPropertyColumnMapping(entityType);
            var primaryKey = GetPrimaryKey(properties.Values.Select(v => v.columnName));

            var schema = new ByConventionSchema(
                tableName,
                new ByConventionSchemaColumns(primaryKey, properties));

            configure?.Invoke(schema);

            return schema;
        }

        private static string[] PrimaryKeyCandidates => new[] { "id", "key" };

        private static string GetTableName(Type entityType)
        {
            return entityType
                .Name
                .TrimEnd("entity", StringComparison.InvariantCultureIgnoreCase)
                .ToSnakeCase();
        }

        private static Dictionary<string, (string columnName, string dbType)> GetPropertyColumnMapping(Type entityType)
        {
            return entityType
                .GetProperties()
                .ToDictionary(x => x.Name, x => (x.Name.ToSnakeCase(), (string)null));
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