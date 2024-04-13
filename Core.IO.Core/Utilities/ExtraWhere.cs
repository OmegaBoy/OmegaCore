using System.Data;

namespace Omegacorp.Core.Model.Utilities
{
    public class ExtraWhere
    {
        public string Statement { get; set; }
        public string ParameterName { get; set; }
        public object ParameterValue { get; set; }
        public DbType? ParameterType { get; set; }
    }
}
