using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Serilog.Sinks.MySQL.Sinks.Batch
{
    internal class PeriodicCleanup
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly string _columnNameWithTime;
        private readonly TimeSpan _recordsExpiration;
        private readonly TimeSpan _cleanupFrequency;
        private readonly bool _timeInUtc;
        private Timer _cleanupTimer;
        private readonly int _deleteLimit;

        /// <summary>
        /// 清理日志
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">删除的表名称</param>
        /// <param name="columnNameWithTime">表中时间字段名称</param>
        /// <param name="recordsExpiration">过期记录TimeSpan</param>
        /// <param name="cleanupFrequency">记录清理频率</param>
        /// <param name="timeInUtc">时间戳InUtc</param>
        /// <param name="deleteLimit">一次删除多少个</param>
        public PeriodicCleanup(string connectionString, string tableName, string columnNameWithTime, TimeSpan? recordsExpiration, TimeSpan? cleanupFrequency, bool timeInUtc, int deleteLimit)
        {
            if (string.IsNullOrEmpty(columnNameWithTime))
                throw new ArgumentNullException(nameof(columnNameWithTime));
            if (_deleteLimit < 0)
                throw new ArgumentOutOfRangeException(nameof(deleteLimit));

            _columnNameWithTime = columnNameWithTime;
            _connectionString = connectionString;
            _tableName = tableName;
            _recordsExpiration = recordsExpiration.Value;
            _cleanupFrequency = cleanupFrequency.Value;
            _timeInUtc = timeInUtc;
            _deleteLimit = deleteLimit;
        }

        public void Start()
        {
            //We are delaying first cleanup for 2 seconds just to avoid semi-expensive query at startup
            _cleanupTimer = new Timer(EnsureCleanup, null, 2000, (int)_cleanupFrequency.TotalMilliseconds);
        }

        private void EnsureCleanup(object state)
        {
            try
            {
                using (var conn = new MySqlConnection(_connectionString))
                {
                    conn.Open();
                    int affectedRows = 0;
                    do
                    {
                        var sql = $"DELETE FROM `{_tableName}` WHERE `{_columnNameWithTime}` < @expiration LIMIT {_deleteLimit}";
                        using (var cmd = new MySqlCommand(sql, conn))
                        {
                            var deleteFromTime = _timeInUtc
                                ? DateTimeOffset.UtcNow - _recordsExpiration
                                : DateTimeOffset.Now - _recordsExpiration;

                            cmd.Parameters.AddWithValue("expiration", deleteFromTime);
                            affectedRows = cmd.ExecuteNonQuery();
                        }
                    } while (affectedRows >= _deleteLimit && affectedRows > 1);
                }
            }
            catch (Exception ex)
            {
                Serilog.Debugging.SelfLog.WriteLine("Periodic database cleanup failed: " + ex.ToString());
            }
        }

    }
}
