using System;
using System.Collections.Generic;
using System.Linq;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres.Schema
{
    public class Convention
    {
        public static ISchema CreateSchema<TEntity>(Action<ConventionOptions> configure = null, Action<ByConventionSchema> postConfigure = null)
        {
            var options = new ConventionOptions();

            configure?.Invoke(options);

            var entityType = typeof(TEntity);
            var tableName = GetTableName(entityType, options.PluralizeTableNames);
            var properties = GetPropertyColumnMapping(entityType);
            var primaryKey = GetPrimaryKey(properties.Keys);

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

        private static NonStandardDbType? GuessColumnDbType(Type propertyType)
        {
            return TypeHelpers
                .IsJsonNetType(propertyType)
                    ? NonStandardDbType.JsonB
                    : (NonStandardDbType?)null;
        }

        private static PropertyColumnMap GetPropertyColumnMapping(Type entityType)
        {
            return new PropertyColumnMap(
                entityType
                    .GetProperties()
                    .ToDictionary(
                        p => p.Name,
                        p => new Mapping(
                            p.Name,
                            p.Name.ToSnakeCase(),
                            GuessColumnDbType(p.PropertyType)
                        )));
        }

        private static string GetPrimaryKey(IEnumerable<string> propertyNames)
        {
            return propertyNames.First(c =>
                PrimaryKeyCandidates.Any(pkc =>
                    pkc.Equals(c, StringComparison.OrdinalIgnoreCase)));
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

        public string TableAlias => throw new NotImplementedException();

        public string TableFullName => throw new NotImplementedException();

        public List<Join> Joins => throw new NotImplementedException();

        public (string propertyName, OrderDirection direction) DefaultOrderBy => throw new NotImplementedException();
    }

    public class ByConventionSchemaColumns : ISchemaColumns
    {
        public ByConventionSchemaColumns(string keyProperty, PropertyColumnMap mapping)
        {
            KeyProperty = keyProperty;
            Mapping = mapping;
        }

        public string KeyProperty { get; set; }
        public PropertyColumnMap Mapping { get; }
    }
}