namespace Necessity.UnitOfWork.Postgres.Schema
{
    public interface ISchema
    {
        string TableName { get; }
        ISchemaColumns Columns { get; }
    }
}