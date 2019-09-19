namespace Necessity.UnitOfWork.Postgres
{
    public interface ISchema
    {
        string TableName { get; }
        ISchemaColumns Columns { get; }
    }
}