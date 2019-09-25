using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres.Schema
{
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