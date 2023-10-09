using System;
using System.Data.SqlClient;
using System.Data;
using System.Reflection;

namespace Omegacorp.Core.Model.Utilities
{
    public static class TypeHelpers
    {
        public static object CastPropertyValue(PropertyInfo property, string value)
        {
            if (property == null || string.IsNullOrEmpty(value))
                return null;
            if (property.PropertyType.IsEnum)
            {
                Type enumType = property.PropertyType;
                if (Enum.IsDefined(enumType, value))
                    return Enum.Parse(enumType, value);
            }
            if (property.PropertyType == typeof(bool))
                return value == "1" || value == "true" || value == "on" || value == "checked";
            else if (property.PropertyType == typeof(Uri))
                return new Uri(Convert.ToString(value));
            else
                return Convert.ChangeType(value, property.PropertyType);
        }

        public static DbType GetDbType(Type runtimeType)
        {
            var nonNullableType = Nullable.GetUnderlyingType(runtimeType);
            if (nonNullableType != null)
            {
                runtimeType = nonNullableType;
            }

            var templateValue = (Object)null;
            if (runtimeType.IsClass == false)
            {
                templateValue = Activator.CreateInstance(runtimeType);
            }

            var sqlParamter = new SqlParameter(parameterName: String.Empty, value: templateValue);

            return sqlParamter.DbType;
        }

    }
}
