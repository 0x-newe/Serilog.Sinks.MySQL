// Copyright 2019 Zethian Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;
using Serilog.Sinks.MySQL.Sinks.Batch;

namespace Serilog.Sinks.MySQL
{
    public class MySqlSink : BatchProvider, ILogEventSink
    {
        private readonly string _connectionString;
        private readonly bool _storeTimestampInUtc;
        private readonly string _tableName;
        private readonly PeriodicCleanup _cleaner;

        public MySqlSink(
            string connectionString,
             TimeSpan? recordsExpiration, TimeSpan? cleanupFrequency, string columnNameWithTime, int deleteLimit, string tableName, uint batchSize, bool storeTimestampInUtc) : base((int)batchSize)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;

            var sqlConnection = GetSqlConnection();
            CreateTable(sqlConnection);

            if (recordsExpiration.HasValue && recordsExpiration > TimeSpan.Zero && cleanupFrequency.HasValue && cleanupFrequency > TimeSpan.Zero)
            {
                _cleaner = new PeriodicCleanup(connectionString,
                    tableName,
                    columnNameWithTime,
                    recordsExpiration,
                    cleanupFrequency,
                    storeTimestampInUtc,
                    deleteLimit);
                _cleaner.Start();
            }
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        private MySqlConnection GetSqlConnection()
        {
            try
            {
                var conn = new MySqlConnection(_connectionString);
                conn.Open();

                return conn;
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                return null;
            }
        }

        private MySqlCommand GetInsertCommand(MySqlConnection sqlConnection)
        {
            var tableCommandBuilder = new StringBuilder();
            tableCommandBuilder.Append($"INSERT INTO  {_tableName} (");
            tableCommandBuilder.Append("Timestamp, Level, Message, LongDate,Logger,TraceIdentifier ,Exception, Properties) ");
            tableCommandBuilder.Append("VALUES (@ts, @level, @msg,@LongDate,@Logger,@TraceIdentifier, @ex, @prop)");

            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = tableCommandBuilder.ToString();

            cmd.Parameters.Add(new MySqlParameter("@ts", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@level", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@msg", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@LongDate", MySqlDbType.DateTime));
            cmd.Parameters.Add(new MySqlParameter("@Logger", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@TraceIdentifier", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@ex", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@prop", MySqlDbType.VarChar));

            return cmd;
        }

        private void CreateTable(MySqlConnection sqlConnection)
        {
            try
            {
                var tableCommandBuilder = new StringBuilder();
                tableCommandBuilder.Append($"CREATE TABLE IF NOT EXISTS {_tableName} (");
                tableCommandBuilder.Append("id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
                tableCommandBuilder.Append("Timestamp VARCHAR(100),");
                tableCommandBuilder.Append("Level VARCHAR(15),");
                tableCommandBuilder.Append("Message text,");
                tableCommandBuilder.Append("LongDate datetime DEFAULT NULL,");
                tableCommandBuilder.Append("Logger varchar(1024) DEFAULT NULL,");
                tableCommandBuilder.Append("TraceIdentifier varchar(128) DEFAULT NULL,");
                tableCommandBuilder.Append("Exception text,");
                tableCommandBuilder.Append("Properties text,");
                tableCommandBuilder.Append("_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");

                tableCommandBuilder.Append($"ALTER TABLE {_tableName} ");
                tableCommandBuilder.Append("ADD UNIQUE INDEX `id`(`id`) USING BTREE COMMENT '自增',");
                tableCommandBuilder.Append("ADD INDEX `LongDate`(`LongDate`) USING BTREE COMMENT '时间';");

                var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = tableCommandBuilder.ToString();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }
        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            try
            {
                using (var sqlCon = GetSqlConnection())
                {
                    using (var tr = await sqlCon.BeginTransactionAsync().ConfigureAwait(false))
                    {
                        var insertCommand = GetInsertCommand(sqlCon);
                        insertCommand.Transaction = tr;
                        foreach (var logEvent in logEventsBatch)
                        {
                            var logMessageString = new StringWriter(new StringBuilder());
                            logEvent.RenderMessage(logMessageString);

                            insertCommand.Parameters["@ts"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz");

                            insertCommand.Parameters["@level"].Value = LogEventConvert(logEvent.Level);
                            insertCommand.Parameters["@msg"].Value = logMessageString;
                            insertCommand.Parameters["@LongDate"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")
                                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss");
                            logEvent.Properties.TryGetValue("SourceContext", out LogEventPropertyValue sourceContext);

                            logEvent.Properties.TryGetValue("\"Message\"", out LogEventPropertyValue tagMessage);
                            if (tagMessage != null)
                            {
                                var tagObject = tagMessage as ScalarValue;
                                if (tagObject?.Value.ToString().Contains("logTag") == true)
                                {
                                    var obj = tagObject.Value.ToString().Replace("=", "").Replace("{", "")
                                        .Replace("}", "").Replace("logTag", "").Trim();
                                    insertCommand.Parameters["@Logger"].Value =
                                        obj ?? string.Empty;
                                }
                            }
                            else
                            {
                                insertCommand.Parameters["@Logger"].Value = (sourceContext as ScalarValue)?.Value ?? string.Empty;
                            }
                            logEvent.Properties.TryGetValue("RequestId", out LogEventPropertyValue requestId);
                            insertCommand.Parameters["@TraceIdentifier"].Value = (requestId as ScalarValue)?.Value ?? string.Empty;
                            insertCommand.Parameters["@ex"].Value = logEvent.Exception?.ToString() ?? string.Empty;
                            insertCommand.Parameters["@prop"].Value = logEvent.Properties.Count > 0
                                ? logEvent.Properties.Json()
                                : string.Empty;

                            await insertCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }

                        tr.Commit();

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                return false;
            }
        }

        private string LogEventConvert(LogEventLevel logEventLevel)
        {
            try
            {
                switch (logEventLevel)
                {
                    case LogEventLevel.Verbose:
                        return "TRACE";
                    case LogEventLevel.Debug:
                        return "DEBUG";
                    case LogEventLevel.Information:
                        return "INFO";
                    case LogEventLevel.Warning:
                        return "WARN";
                    case LogEventLevel.Error:
                        return "ERROR";
                    case LogEventLevel.Fatal:
                        return "FATAL";
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
                return "";
            }
            return "INFO";
        }
    }
}
