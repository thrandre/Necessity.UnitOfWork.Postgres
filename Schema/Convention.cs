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
            var tableAlias = GetTableAlias(tableName);
            var properties = GetPropertyColumnMapping(entityType, tableAlias);
            var primaryKey = GetPrimaryKey(properties.Keys);

            var schema = new ByConventionSchema(
                tableName,
                tableAlias,
                new ByConventionSchemaColumns(primaryKey, properties));

            postConfigure?.Invoke(schema);

            if(string.IsNullOrWhiteSpace(schema.Columns.KeyProperty))
            {
                throw new ArgumentNullException(nameof(schema.Columns.KeyProperty));
            }

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

        private static PropertyColumnMap GetPropertyColumnMapping(Type entityType, string tableAlias)
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
            return propertyNames
                .Select((p, i) =>
                    new
                    {
                        Name = p,
                        Index = i
                    })
                .FirstOrDefault(c =>
                    PrimaryKeyCandidates.Any(pkc =>
                        c.Name.Equals(pkc, StringComparison.OrdinalIgnoreCase) ||
                            (c.Index == 0 && c.Name.EndsWith(pkc, StringComparison.OrdinalIgnoreCase))))
                ?.Name;
        }

        private static string GetTableAlias(string tableName)
        {
            return string.Concat(
                tableName
                    .Split('_')
                    .Select(x => x.Substring(0, 1)));
        }
    }
}