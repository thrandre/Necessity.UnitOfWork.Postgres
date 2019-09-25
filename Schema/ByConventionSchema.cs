using System;
using System.Collections.Generic;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres.Schema
{
    public class ByConventionSchema : ISchema
    {
        public ByConventionSchema(string tableName, string tableAlias, ByConventionSchemaColumns columns)
        {
            TableName = tableName;
            TableAlias = tableAlias;
            Columns = columns;
            Joins = new List<Join>();
        }

        public string TableName { get; set; }
        public string TableAlias { get; set; }
        public ByConventionSchemaColumns Columns { get; }
        public string TableFullName => throw new NotImplementedException();
        public List<Join> Joins { get; }
        public (string propertyName, OrderDirection direction) DefaultOrderBy { get; set; }

        ISchemaColumns ISchema.Columns => Columns;
    }
}