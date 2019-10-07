using System;
using System.Collections.Generic;
using System.Diagnostics;
using Necessity.UnitOfWork.Postgres.Schema;
using Necessity.UnitOfWork.Schema;

namespace Necessity.UnitOfWork.Postgres
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var schema = Convention.CreateSchema<Foo>(s => s.PluralizeTableNames = true, s =>
            {
                s.Columns.Mapping.Add(
                   "AttributeCount",
                   Mapper
                       .Map("AttributeCount")
                       .ToColumnName("attribute_count")
                       .OnSelect(_ => $@"(SELECT count(*) FROM jsonb_object_keys({ s.Columns.Mapping[nameof(Foo.Attributes)].ColumnName })) as attribute_count")
                       .CreateMapping());

                s.Columns.Mapping[nameof(Foo.SequenceNumber)]
                    .OnInsert = e => $@"nextval(seq_{((Dictionary<string, object>)e)[nameof(Foo.TemplateKey)]})";

                s.DefaultOrderBy = (propertyName: "AttributeCount", direction: OrderDirection.Descending);
            });

            var builder = new DefaultQueryBuilder<Foo, Guid>(schema);
            var qp = new Dictionary<string, object>();

            Debug.WriteLine(builder.Upsert(
                new Foo
                {
                    Id = Guid.NewGuid(),
                    TemplateKey = "nuts"
                }, OnConflict.Update, qp));

            Debug.WriteLine(builder.Get(Guid.NewGuid(), qp));
        }
    }

    public class Foo
    {
        public Guid Id { get; set; }
        public string TemplateKey { get; set; }

        public DateTime Created { get; set; }
        public object Attributes { get; set; }
        public object Metadata { get; set; }
        public long SequenceNumber { get; set; }
    }
}