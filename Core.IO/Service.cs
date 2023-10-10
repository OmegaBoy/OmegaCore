using Microsoft.Data.Sqlite;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Omegacorp.Core.IO.Models;

namespace Omegacorp.Core.IO
{
    public class Service<T>
    {
        /// <summary>
        /// Conexion a BBDD
        /// </summary>
        public IDbConnection _sqlConnection;
        private readonly IO<T> io;

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

            io = new IO<T>(_sqlConnection);
        }
    }
}
