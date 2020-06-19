using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Xml;

namespace ImportTablesLibrary
{
   public class CopyTables
    {
        public void CopyDataTableServerToServer(string ServerSource, string NameTableSource, string ServerDestination)
        {
           var queryTableCreate = GetScriptForCreateTable(ServerSource, NameTableSource);
           CreateTable(ServerDestination, queryTableCreate);
           CopyDataToServer(ServerDestination, NameTableSource,CopyDataOfTable(ServerSource,NameTableSource));
        }

      
        public void CopyDataToServer(string serverDestination, string NameTable, DataTable dataTable)
        {
            List<string> headers = GetHeaderTable(serverDestination, NameTable);
            string query = "";
            using (SqlConnection connection = new SqlConnection(serverDestination))
            {
                connection.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    
                    try
                    {
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.DestinationTableName = NameTable;                 
                        bulkCopy.WriteToServer(dataTable);
                        Console.WriteLine("La tabla " + NameTable + " fue copiada exitosamente. XD");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        
                    }
                }
                //using (SqlCommand command = new SqlCommand(query, connection))
                // {
                //    try
                //    {
                //         connection.Open();
                //        foreach (var row in dataTable.Rows)
                //        {
                           
                //        }
                //        SqlDataReader reader = command.ExecuteReader();
                //    }
                //    catch (SqlException e)
                //    {
                //        Console.WriteLine("" + e);
                //    }
                //}
                
            }
        }

        private List<string> GetHeaderTable(string server, string tableName)
        {
            DataTable dataTable = CopyDataOfTable(server, tableName);
            List<string> headers = new List<string>();
            foreach (DataColumn column in dataTable.Columns)
            {
                headers.Add(column.ColumnName);
            }
            return headers;
        }

        public DataTable CopyDataOfTable(string Server, string TableName)
        {
            DataTable dataTable = new DataTable();
            string connectionString = Server;

            using (SqlConnection sourceConnection =new SqlConnection(connectionString))
            {
                
                try
                {
                    sourceConnection.Open();
                    SqlCommand commandRowCount = new SqlCommand("SELECT COUNT(*) FROM " + TableName + ";", sourceConnection);
                    long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());

                    Console.WriteLine("numero de filas de la tabla " + TableName + " " + countStart);
                    SqlCommand commandSourceData = new SqlCommand("SELECT * FROM " + TableName + " ;", sourceConnection);
                    SqlDataReader reader = commandSourceData.ExecuteReader();
                    dataTable.Load(reader);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("" + ex);
                }
                finally
                {
                    
                }
               
            }
            return dataTable;
        }

        private void CreateTable(string server, string queryTableCreate)
        {
            using (SqlConnection connection = new SqlConnection(server))
            {
                connection.Open();
                try { 
                using (SqlCommand command = new SqlCommand(queryTableCreate, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                catch(SqlException e)
                {
                    Console.WriteLine("" + e);
                }
            }
        }

        public string GetScriptForCreateTable(string ServerName, string TableName)
        {
            string connectionString = ServerName;
            var queryCreateTable = "";
            using (SqlConnection sourceConnection = new SqlConnection(connectionString))
            {
                string query = "DECLARE  "
                + "      @object_name SYSNAME  "
                + "    , @object_id INT  "
                + "    , @SQL VARCHAR(MAX)  "
                + "  "
                + "SELECT  "
                + "      @object_name = '[' + OBJECT_SCHEMA_NAME(o.[object_id]) + '].[' + OBJECT_NAME([object_id]) + ']'  "
                + "    , @object_id = [object_id]  "
                + "FROM (SELECT [object_id] = OBJECT_ID('"+ TableName + "', 'U')) o  "
                + "  "
                + "SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((  "
                + "    SELECT CHAR(13) + '    , [' + c.name + '] ' +   "
                + "        CASE WHEN c.is_computed = 1  "
                + "            THEN 'AS ' + OBJECT_DEFINITION(c.[object_id], c.column_id)  "
                + "            ELSE   "
                + "                CASE WHEN c.system_type_id != c.user_type_id   "
                + "                    THEN '[' + SCHEMA_NAME(tp.[schema_id]) + '].[' + tp.name + ']'   "
                + "                    ELSE '[' + UPPER(tp.name) + ']'   "
                + "                END  +   "
                + "                CASE   "
                + "                    WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')  "
                + "                        THEN '(' + CASE WHEN c.max_length = -1   "
                + "                                        THEN 'MAX'   "
                + "                                        ELSE CAST(c.max_length AS VARCHAR(5))   "
                + "                                    END + ')' "
                + "                    WHEN tp.name IN ('nvarchar')  "
                + "                        THEN '(' + CASE WHEN c.max_length = -1   "
                + "                                        THEN 'MAX'   "
                + "                                        ELSE CAST(c.max_length/2 AS VARCHAR(5))   "
                + "                                    END + ')'                  "
                + "                    WHEN tp.name IN ('nchar')  "
                + "                        THEN '(' + CASE WHEN c.max_length = -1   "
                + "                                        THEN 'MAX'   "
                + "                                        ELSE CAST(c.max_length / 2 AS VARCHAR(5))   "
                + "                                    END + ')'  "
                + "                    WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')   "
                + "                        THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'  "
                + "                    WHEN tp.name = 'decimal'  "
                + "                        THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'  "
                + "                    ELSE ''  "
                + "                END +  "
                + "                CASE WHEN c.collation_name IS NOT NULL AND c.system_type_id = c.user_type_id   "
                + "                    THEN ' COLLATE ' + c.collation_name  "
                + "                    ELSE ''  "
                + "                END +  "
                + "                CASE WHEN c.is_nullable = 1   "
                + "                    THEN ' NULL'  "
                + "                    ELSE ' NOT NULL'  "
                + "                END +  "
                + "                CASE WHEN c.default_object_id != 0   "
                + "                    THEN ' CONSTRAINT [' + OBJECT_NAME(c.default_object_id) + ']' +   "
                + "                         ' DEFAULT ' + OBJECT_DEFINITION(c.default_object_id)  "
                + "                    ELSE ''  "
                + "                END +   "
                + "                CASE WHEN cc.[object_id] IS NOT NULL   "
                + "                    THEN ' CONSTRAINT [' + cc.name + '] CHECK ' + cc.[definition]  "
                + "                    ELSE ''  "
                + "                END +  "
                + "                CASE WHEN c.is_identity = 1   "
                + "                    THEN ' IDENTITY(' + CAST(IDENTITYPROPERTY(c.[object_id], 'SeedValue') AS VARCHAR(5)) + ',' +   "
                + "                                    CAST(IDENTITYPROPERTY(c.[object_id], 'IncrementValue') AS VARCHAR(5)) + ')'   "
                + "                    ELSE ''   "
                + "                END   "
                + "        END  "
                + "    FROM sys.columns c WITH(NOLOCK)  "
                + "    JOIN sys.types tp WITH(NOLOCK) ON c.user_type_id = tp.user_type_id  "
                + "    LEFT JOIN sys.check_constraints cc WITH(NOLOCK)   "
                + "         ON c.[object_id] = cc.parent_object_id   "
                + "        AND cc.parent_column_id = c.column_id  "
                + "    WHERE c.[object_id] = @object_id  "
                + "    ORDER BY c.column_id  "
                + "    FOR XML PATH(''), TYPE).value('.', 'VARCHAR(MAX)'), 1, 7, '      ') +   "
                + "    ISNULL((SELECT '  "
                + "    , CONSTRAINT [' + i.name + '] PRIMARY KEY ' +   "
                + "    CASE WHEN i.index_id = 1   "
                + "        THEN 'CLUSTERED'   "
                + "        ELSE 'NONCLUSTERED'   "
                + "    END +' (' + (  "
                + "    SELECT STUFF(CAST((  "
                + "        SELECT ', [' + COL_NAME(ic.[object_id], ic.column_id) + ']' +  "
                + "                CASE WHEN ic.is_descending_key = 1  "
                + "                    THEN ' DESC'  "
                + "                    ELSE ''  "
                + "                END  "
                + "        FROM sys.index_columns ic WITH(NOLOCK)  "
                + "        WHERE i.[object_id] = ic.[object_id]  "
                + "            AND i.index_id = ic.index_id  "
                + "        FOR XML PATH(N''), TYPE) AS VARCHAR(MAX)), 1, 2, '')) + ')'  "
                + "    FROM sys.indexes i WITH(NOLOCK)  "
                + "    WHERE i.[object_id] = @object_id  "
                + "        AND i.is_primary_key = 1), '') + CHAR(13) + ');'  "
                + "  "
                + "PRINT @SQL  ";

                using (SqlCommand command = new SqlCommand(query, sourceConnection))
                {
                    sourceConnection.Open();
                    sourceConnection.InfoMessage += delegate (object sender, SqlInfoMessageEventArgs e)
                    {
                        queryCreateTable = e.Message;
                    };
                    SqlDataReader reader = command.ExecuteReader();
                }
            }
            return queryCreateTable;
        }
    }
}
