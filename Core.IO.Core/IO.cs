using Dapper;
using Microsoft.Data.Sqlite;
using Npgsql;
using Omegacorp.Core.Model.Utilities;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;

namespace Omegacorp.Core.IO
{
    public class IO<T>
    {
        public readonly IDbConnection _connection;
        public readonly IDbTransaction _transaction;

        public IO(IDbConnection sqlConnection, IDbTransaction sqlTransaction = null)
        {
            _connection = sqlConnection;
            _transaction = sqlTransaction;
        }

        public Pagination<T> Search(IEnumerable<int> IDs = null, string description = null, PaginationFilter paginationFilter = null, IEnumerable<ExtraWhere> extraWheres = null)
        {
            var where = new List<string>();
            var dbArgs = new DynamicParameters();
            if (extraWheres != null && extraWheres.Any())
            {
                foreach (var extraWhere in extraWheres)
                {
                    where.Add(extraWhere.Statement);
                    dbArgs.Add(extraWhere.ParameterName, value: extraWhere.ParameterValue, dbType: extraWhere.ParameterType);
                }
            }
            if (IDs != null && IDs.Any())
            {
                var tableIDName = typeof(T).GetProperties().SingleOrDefault(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity)));
                if (tableIDName != null)
                {
                    if (_connection.GetType() == typeof(SqlConnection))
                    {
                        where.Add($"{tableIDName.Name} IN @IDs");
                    }
                    else if (_connection.GetType() == typeof(NpgsqlConnection))
                    {
                        where.Add($"{tableIDName.Name} = ANY(@IDs)");
                    }
                    else if (_connection.GetType() == typeof(SqliteConnection))
                    {

                    }
                    dbArgs.Add("IDs", value: IDs);
                }
            }
            if (!string.IsNullOrWhiteSpace(description))
            {
                var descriptorsFields = typeof(T).GetProperties().Where(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(Descriptor)));
                if (descriptorsFields.Any())
                {
                    var descriptorWhere = new List<string>();
                    foreach (var descriptorField in descriptorsFields)
                    {
                        if (_connection.GetType() == typeof(SqlConnection))
                        {
                            descriptorWhere.Add($"{descriptorField.Name} LIKE '%{description}%'");
                        }
                        else if (_connection.GetType() == typeof(NpgsqlConnection))
                        {
                            descriptorWhere.Add($"LOWER({descriptorField.Name}) LIKE '%{description.ToLower()}%'");
                        }
                    }
                    where.Add(string.Join(" OR ", descriptorWhere));
                }
            }
            var commandWhere = "";
            if (where.Any())
            {
                commandWhere = " WHERE " + string.Join(" AND ", where);
            }
            string countover = (paginationFilter?.Page != null && paginationFilter?.RowsPerPage != null && paginationFilter?.SortBy != null) ? ",\r\n\tCount(*) Over () MaxRows" : "";
            string command = $"SELECT *{countover} FROM {typeof(T).Name} {commandWhere}";
            IEnumerable<T> ret;
            long cant = 0;
            if (paginationFilter?.SortBy != null)
            {
                var orderBy = paginationFilter.SortBy;
                var order = paginationFilter.Descending ? "desc" : "asc";
                command = $@"
    {command}
ORDER BY {orderBy} {order}";
            }
            if (paginationFilter?.Page != null && paginationFilter?.RowsPerPage != null && paginationFilter?.SortBy != null)
            {
                command = $@"
    {command}
    OFFSET ((@PageNum-1)*@PageSize) ROWS
    FETCH NEXT @PageSize ROWS ONLY";
                dbArgs.Add("PageNum", (int)paginationFilter.Page);
                dbArgs.Add("PageSize", (int)paginationFilter.RowsPerPage);
                ret = _connection.Query<T, long, T>(command, map: (data, count) => { cant = count; return data; }, param: dbArgs, transaction: _transaction, splitOn: "MaxRows");
            }
            else
            {
                ret = _connection.Query<T>(command, param: dbArgs, transaction: _transaction);
            }

            int? pages = (int)cant / (paginationFilter?.RowsPerPage ?? 1) + ((cant % (paginationFilter?.RowsPerPage ?? 1)) != 0 ? 1 : 0);
            return new Pagination<T> { Data = ret, Rows = paginationFilter != null ? pages : null };
        }

        public T Get(string ID)
        {
            var tableID = typeof(T).GetProperties().Single(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity)));
            string command = $"SELECT * FROM {typeof(T).Name} WHERE {tableID.Name + " = @" + tableID.Name}";

            var ret = _connection.Query<T>(command, param: new { ID = TypeHelpers.CastPropertyValue(tableID, ID) }, transaction: _transaction).SingleOrDefault();
            return ret;
        }

        public T Get(T item)
        {
            var tableIDNames = typeof(T).GetProperties().Where(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(SharedIdentity)));
            if (tableIDNames.Any())
            {
                string command = $"SELECT * FROM {typeof(T).Name} WHERE {string.Join(" AND ", tableIDNames.Select(x => x.Name + " = @" + x.Name))}";

                var ret = _connection.Query<T>(command, param: item, transaction: _transaction).SingleOrDefault();
                return ret;
            }
            else
            {
                return item;
            }
        }

        public T Insert(T item)
        {
            var tableNames = typeof(T).GetProperties().Where(x => !x.CustomAttributes.Any(y => y.AttributeType == typeof(OutsideModel) || y.AttributeType == typeof(IncrementalIdentity)));
            var needsOutput = typeof(T).GetProperties().Any(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity)));

            string output = "";
            string command = "";
            if (_connection.GetType() == typeof(SqlConnection))
            {
                output = needsOutput ? "output inserted.ID" : "";
                command = $"INSERT INTO {typeof(T).Name}({string.Join(",", tableNames.Select(x => x.Name))}) {output} VALUES (@{string.Join(", @", tableNames.Select(x => x.Name))})";
            }
            else if (_connection.GetType() == typeof(NpgsqlConnection) || _connection.GetType() == typeof(SqliteConnection))
            {
                output = needsOutput ? "RETURNING ID" : "";
                command = $"INSERT INTO {typeof(T).Name}({string.Join(",", tableNames.Select(x => x.Name))}) VALUES (@{string.Join(", @", tableNames.Select(x => x.Name))}) {output};";
            }

            if (needsOutput)
            {
                var ID = _connection.ExecuteScalar<int>(command, param: item, transaction: _transaction);
                return Get(ID.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                _connection.Execute(command, item, transaction: _transaction);
                return Get(item);
            }
        }

        public IEnumerable<T> Insert(IEnumerable<T> items)
        {
            var tableProps = typeof(T).GetProperties().Where(x => !x.CustomAttributes.Any(y => y.AttributeType == typeof(OutsideModel) || y.AttributeType == typeof(IncrementalIdentity)));
            var needsOutput = typeof(T).GetProperties().Any(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity)));
            string output = "";
            string command = "";
            if (_connection.GetType() == typeof(SqlConnection))
            {
                output = needsOutput ? "output inserted.ID" : "";
                command = $"INSERT INTO {typeof(T).Name}({string.Join(",", tableProps.Select(x => x.Name))}) {output} VALUES ";
            }
            else if (_connection.GetType() == typeof(NpgsqlConnection))
            {
                output = needsOutput ? "RETURNING ID" : "";
                command = $"INSERT INTO {typeof(T).Name}({string.Join(",", tableProps.Select(x => x.Name))}) VALUES";
            }
            var itemsValues = new List<string>();
            var dbArgs = new DynamicParameters();
            for (int i = 0; i < items.Count(); i++)
            {
                var item = items.ElementAt(i);
                itemsValues.Add($"(@{string.Join(", @", tableProps.Select(x => x.Name + i))})");
                for (int j = 0; j < tableProps.Count(); j++)
                {
                    var prop = tableProps.ElementAt(j);
                    dbArgs.Add(prop.Name + i, value: prop.GetValue(item));
                }
            }
            command += string.Join(", ", itemsValues);
            if (_connection.GetType() == typeof(NpgsqlConnection))
            {
                command += $" {output}";
            }

            if (needsOutput)
            {
                var IDs = _connection.Query<int>(command, param: dbArgs, transaction: _transaction);
                return Search(IDs: IDs).Data;
            }
            else
            {
                _connection.Execute(command, dbArgs, transaction: _transaction);
                var ret = new List<T>();
                return items;
            }
        }

        public T Update(T item)
        {
            string command = null;
            var classFields = typeof(T).GetProperties();
            var incrementalID = classFields.SingleOrDefault(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity)));
            if (incrementalID != null)
            {
                var tableFields = classFields.Where(x => !x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity) || y.AttributeType == typeof(OutsideModel))).Select(x => x.Name + " = @" + x.Name);
                command = $"UPDATE {typeof(T).Name} SET {string.Join(", ", tableFields)} WHERE {incrementalID.Name + " = @" + incrementalID.Name}";
            }
            var sharedIDs = classFields.Where(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(SharedIdentity)));
            if (sharedIDs.Any())
            {
                var tableFields = classFields.Where(x => !x.CustomAttributes.Any(y => y.AttributeType == typeof(SharedIdentity) || y.AttributeType == typeof(OutsideModel))).Select(x => x.Name + " = @" + x.Name);
                command = $"UPDATE {typeof(T).Name} SET {string.Join(", ", tableFields)} WHERE {string.Join(" AND ", sharedIDs.Select(x => x.Name + " = @" + x.Name))}";
            }

            _connection.Execute(command, param: item, transaction: _transaction);
            return item;
        }

        public void Delete(string ID)
        {
            var tableID = typeof(T).GetProperties().SingleOrDefault(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity) || y.AttributeType == typeof(NonIncrementalIdentity)));
            string command = $"DELETE FROM {typeof(T).Name} WHERE {tableID.Name + " = @" + tableID.Name}";

            _connection.Execute(command, param: new { ID = TypeHelpers.CastPropertyValue(tableID, ID) }, transaction: _transaction);
            return;
        }

        public void Delete(IEnumerable<string> IDs)
        {
            var uniqueID = typeof(T).GetProperties().SingleOrDefault(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(IncrementalIdentity) || y.AttributeType == typeof(NonIncrementalIdentity)));
            string command = $"DELETE FROM {typeof(T).Name} WHERE {uniqueID.Name} IN ('{string.Join("','", IDs)}')";
            _connection.Execute(command, transaction: _transaction);
            return;
        }

        public void Delete(T item)
        {
            var sharedIDs = typeof(T).GetProperties().Where(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(SharedIdentity)));
            string command = $"DELETE {typeof(T).Name} WHERE {string.Join(" AND ", sharedIDs.Select(x => x.Name + " = @" + x.Name))}";

            _connection.Execute(command, param: item, transaction: _transaction);
            return;
        }
    }
}
