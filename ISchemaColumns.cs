using System.Collections.Generic;

namespace Necessity.UnitOfWork.Postgres
{
    public interface ISchemaColumns
    {
        string KeyName { get; }
        Dictionary<string, (string, string)> Mapping { get; }
    }
}