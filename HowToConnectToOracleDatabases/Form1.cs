using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.OleDb;
using Oracle.DataAccess.Client;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;

namespace HowToConnectToOracleDatabases
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        #region Oracle And OraDB Database Interaction Functions

        #region Get Database Table's Column Names
        private List<string> GetDatabaseTableColumnNames_OracleDA(string tableName)
        {
            var conStr = GetConnectionString_OracleDA();
            List<string> listOfColumnNames = new List<string>();

            try
            {
                using (var con = new OracleConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OracleCommand("select * from " + tableName, con))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        var table = reader.GetSchemaTable();
                        var nameCol = table.Columns["ColumnName"];
                        foreach (DataRow row in table.Rows)
                        {
                            listOfColumnNames.Add(row[nameCol].ToString());
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                MessageBox.Show("Failure in  function GetDatabaseTableColumnNames_OracleDA. Exception - " + ex.Message);
            }

            return listOfColumnNames;
        }

        /// <summary>
        /// This method will not work with some column types, use the oracleDA one, which will work for most(all) column types
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private List<string> GetDatabaseTableColumnNames_OleDBDA(string tableName)
        {
            var conStr = GetConnectionString_OleDBDA();
            List<string> listOfColumnNames = new List<string>();

            try
            {
                using (var con = new OleDbConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OleDbCommand("select * from " + tableName, con))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        var table = reader.GetSchemaTable();
                        var nameCol = table.Columns["ColumnName"];
                        foreach (DataRow row in table.Rows)
                        {
                            listOfColumnNames.Add(row[nameCol].ToString());
                        }
                    }
                }
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Failure in  function GetDatabaseTableColumnNames_OleDBDA. Exception - " + ex.Message);
            }

            return listOfColumnNames;
        }
        #endregion Get Database Table's Column Names


        #region Get Database Tables
        private DataTable GetDatabaseTables_OracleDA()
        {
            throw new NotImplementedException();
        }

        private DataTable GetDatabaseTables_OleDBDA()
        {
            OleDbConnection conn;
            string connetionString = GetConnectionString_OleDBDA();
            conn = new OleDbConnection(connetionString);

            try
            {
                conn.Open();
                DataTable schemaTable = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                return schemaTable;
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Exception in GetDatabaseTables_OleDBDA - " + ex.Message);
            }
            finally
            {
                conn.Close();
            }
            return null;
        }
        #endregion Get Database Tables


        #region Find Primary Key(s) for a given table name
        private List<string> GetPrimaryKeysForTable_OracleDA(string tableName)
        {
            List<string> primaryKeyList = new List<string>();

            try
            {
                var conStr = GetConnectionString_OracleDA();
                using (var con = new OracleConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OracleCommand(
                        "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner " +
                        "FROM all_constraints cons, all_cons_columns cols " +
                        "WHERE cols.table_name = upper('" + tableName.ToUpper() + "') " +
                        "AND cons.constraint_type = 'P' " +
                        "AND cons.constraint_name = cols.constraint_name " +
                        "AND cons.owner = cols.owner " +
                        "ORDER BY cols.table_name, cols.position", con))
                    {
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                            {
                                primaryKeyList.Add(reader["COLUMN_NAME"].ToString().ToUpper());
                            }
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                MessageBox.Show("Failure in  function GetPrimaryKeysForTable_OracleDA. Exception - " + ex.Message);
                LogMessage("Failure in  function GetPrimaryKeysForTable_OracleDA. Exception - " + ex.Message, MessageSeverity.Error);
            }

            if (primaryKeyList.Count <= 0)
            {
                MessageBox.Show("Error in GetPrimaryKeysForTable_OracleDA - No Primary Key(s) retrieved for table '" + tableName.ToUpper() + "'");
                LogMessage("Error in GetPrimaryKeysForTable_OracleDA - No Primary Key(s) retrieved for table '" + tableName.ToUpper() + "'", MessageSeverity.Error);
            }

            return primaryKeyList;
        }

        private List<string> GetPrimaryKeysForTable_OleDBDA(string tableName)
        {
            List<string> primaryKeyList = new List<string>();

            try
            {
                var conStr = GetConnectionString_OleDBDA();
                using (var con = new OleDbConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OleDbCommand(
                        "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner " +
                        "FROM all_constraints cons, all_cons_columns cols " +
                        "WHERE cols.table_name = upper('" + tableName.ToUpper() + "') " +
                        "AND cons.constraint_type = 'P' " +
                        "AND cons.constraint_name = cols.constraint_name " +
                        "AND cons.owner = cols.owner " +
                        "ORDER BY cols.table_name, cols.position", con))
                    {
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                            {
                                primaryKeyList.Add(reader["COLUMN_NAME"].ToString().ToUpper());
                            }
                        }
                    }
                }
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Failure in  function GetPrimaryKeysForTable_OleDA. Exception - " + ex.Message);
            }

            if (primaryKeyList.Count <= 0)
            {
                MessageBox.Show("Error in GetPrimaryKeysForTable_OleDA - No Primary Key(s) retrieved for table '" + tableName.ToUpper() + "'");
            }

            return primaryKeyList;
        }
        #endregion Find Primary Key(s) for a given table name


        #region Does Table Have Audit Record - SAMPLE QUERY CODE

        /// <summary>
        /// SAMPLE QUERY CODE FUNCTION
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private bool DoesTableHaveAuditRecordSetToYes_OracleDA(string tableName)
        {
            try
            {
                var conStr = GetConnectionString_OracleDA();
                using (var con = new OracleConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OracleCommand("select * from audit_table where audit_table_name = '" + tableName.ToUpper() + "'", con))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        while (reader.Read())
                        {
                            if (reader["AUDIT_INDICATOR"].ToString().ToUpper() == "Y")
                                return true;
                            //How To Read the field values:
                            //string myString = "";
                            //for (int i = 0; i < reader.FieldCount; i++)
                            //{
                            //    myString += reader[i] + "\t";
                            //}
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                MessageBox.Show("Failure in  function DoesTableHaveAuditRecord_OracleDA. Exception - " + ex.Message);
            }

            return false;
        }


        /// <summary>
        /// SAMPLE QUERY CODE FUNCTION
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private bool DoesTableHaveAuditRecord_OleDBDA(string tableName)
        {
            try
            {
                var conStr = GetConnectionString_OleDBDA();
                using (var con = new OleDbConnection(conStr))
                {
                    con.Open();
                    using (var cmd = new OleDbCommand("select * from audit_table where audit_table_name = '" + tableName.ToUpper() + "'", con))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        while (reader.Read())
                        {
                            return true;
                            //How To Read the field values:
                            //string myString = "";
                            //for(int i = 0; i < reader.FieldCount; i++)
                            //{
                            //    myString += reader[i] + "\t";
                            //}
                        }
                    }
                }
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Failure in  function GetConnectionString_OleDBDA. Exception - " + ex.Message);
            }

            return false;
        }
        #endregion Check If Table Has Audit Record


        #region Get Database Schema
        private DataTable GetSchema_OleDBDA()
        {
            OleDbConnection conn;
            string connetionString = GetConnectionString_OleDBDA();
            conn = new OleDbConnection(connetionString);

            try
            {
                conn.Open();
                DataTable schemaTable = conn.GetSchema();
                return schemaTable;
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Exception in GetSchema_OleDBDA - " + ex.Message);
            }
            finally
            {
                conn.Close();
            }
            return null;
        }

        private DataTable GetSchema_OracleDA()
        {
            OracleConnection conn;
            string connetionString = GetConnectionString_OracleDA();
            conn = new OracleConnection(connetionString);

            try
            {
                conn.Open();
                DataTable schemaTable = conn.GetSchema();
                return schemaTable;
            }
            catch (OracleException ex)
            {
                MessageBox.Show("Exception in GetSchema_OracleDA - " + ex.Message);
            }
            finally
            {
                conn.Close();
            }
            return null;
        }
        #endregion Get Database Schema


        #region Get Connection String
        private string GetConnectionString_OleDBDA()
        {
            return "Provider=MSDAORA;Data Source=" + databaseNameTextBox.Text + ";user id=" + databaseUsernameTextBox.Text + ";password=" + databasePasswordTextBox.Text + ";";
        }

        private string GetConnectionString_OracleDA()
        {
            return "Data Source=" + databaseNameTextBox.Text + ";user id=" + databaseUsernameTextBox.Text + ";password=" + databasePasswordTextBox.Text + ";";
        }
        #endregion Get Connection String


        #region Test Database Connections
        private void TestDatabaseConnection_OraceDA()
        {
            OracleConnection conn;
            string connetionString = GetConnectionString_OracleDA();
            conn = new OracleConnection(connetionString);

            try
            {
                conn.Open();
                MessageBox.Show("Oracle Data Access Connection Open!");
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Connection Unable To Open - Exception:" + ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        private void TestDatabaseConnection_OleDBDA()
        {
            OleDbConnection conn;
            string connetionString = GetConnectionString_OleDBDA();
            conn = new OleDbConnection(connetionString);

            try
            {
                conn.Open();
                MessageBox.Show("Ole DB Data Access Connection Open!");
            }
            catch (OleDbException ex)
            {
                MessageBox.Show("Connection Unable To Open - Exception:" + ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }
        #endregion Test Database Connections


        #endregion Oracle And OraDB Database Interaction Functions



    }
}

