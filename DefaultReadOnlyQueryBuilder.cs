using System.Collections.Generic;

namespace Necessity.UnitOfWork.Postgres
{
    public class DefaultReadOnlyQueryBuilder<TEntity, TKey> : IReadOnlyQueryBuilder<TEntity, TKey>
    {
        public DefaultReadOnlyQueryBuilder(ISchema schema)
        {
            Schema = schema;
        }

        public ISchema Schema { get; }

        public virtual string Find(TKey key, Dictionary<string, object> queryParams)
        {
            queryParams.Add(Schema.Columns.KeyName, key);

            return $@"
                SELECT *
                FROM { Schema.TableName }
                WHERE { Schema.Columns.Mapping[Schema.Columns.KeyName].Item1 } = @{ Schema.Columns.KeyName }
            ";
        }

        public virtual string GetAll(Dictionary<string, object> queryParams)
        {
            return $@"
                SELECT *
                FROM { Schema.TableName }
            ";
        }
    }
}