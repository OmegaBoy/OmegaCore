using Microsoft.Data.Sqlite;
using Npgsql;
using Omegacorp.Core.IO.Models;
using System;
using System.Data;
using System.Data.SqlClient;

namespace Omegacorp.Core.IO
{
    public class Service<T>
    {
        /// <summary>
        /// Conexion a BBDD
        /// </summary>
        public IDbConnection _sqlConnection;
        public readonly IO<T> _IO;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="dBEngine"></param>
        public Service(string connectionString, DBEngines dBEngine)
        {
            switch (dBEngine)
            {
                case DBEngines.MSSQL:
                    _sqlConnection = new SqlConnection(connectionString);
                    break;
                case DBEngines.Postgresql:
                    _sqlConnection = new NpgsqlConnection(connectionString);
                    break;
                case DBEngines.SQLite:
                    _sqlConnection = new SqliteConnection(connectionString);
                    break;
                default:
                    throw new Exception("Se debe especificar el motor de base de datos");
            }

            _IO = new IO<T>(_sqlConnection);
        }
    }
}
