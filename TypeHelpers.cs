using System;

namespace Necessity.UnitOfWork.Postgres
{
    public class TypeHelpers
    {
        public static bool IsJsonNetType(Type type) =>
            type.FullName == "Newtonsoft.Json.Linq.JToken"
                || type.FullName == "Newtonsoft.Json.Linq.JObject"
                || type.FullName == "Newtonsoft.Json.Linq.JArray";
    }
}