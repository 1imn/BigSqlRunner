using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public class BigSqlRunner
    {
        public BigSqlRunnerConfig Config { get; set; }
        protected static string CacheDirName { get; set; } = ".bsr";
        protected static string DefaultConfigFileName { get; set; } = "default.config";

        protected bool Stop { get; set; } = false;
        protected Action StopCallBack { get; set; } = null;

        public delegate void ProgressReporter(int executedUnits, int affectedRows);
        public delegate void ErrorReporter(string message);

        public BigSqlRunner(
            string connectionString, string bigSqlFilePath,
            bool enableLogging = false, string logFilePath = null,
            int batchSize = 1, string sqlUnitEndingLine = "GO",
            bool continueFromLastSessionWhenStarted = true, SessionSaveTypeEnum sessionSaveType = SessionSaveTypeEnum.SqlUnitIndex,
            int retryIntervalWhenError_Seconds = 3, int retryNumberWhenError = 9
        ) : this(
            new BigSqlRunnerConfig(
                connectionString, bigSqlFilePath,
                enableLogging, logFilePath,
                batchSize, sqlUnitEndingLine,
                continueFromLastSessionWhenStarted, sessionSaveType,
                retryIntervalWhenError_Seconds, retryNumberWhenError
            )
        )
        {

        }
        public BigSqlRunner(BigSqlRunnerConfig config)
        {
            if (null == config) throw new ArgumentNullException(nameof(config));

            Config = config;
        }

        public static BigSqlRunner FromDefaultConfig()
        {
            var configFilePath = GetDefaultConfigFilePath();
            if (false == File.Exists(configFilePath)) return null;

            var config = BigSqlRunnerConfig.FromFile(configFilePath);
            var bigSqlRunner = new BigSqlRunner(config);
            return bigSqlRunner;
        }

        protected string GetCacheDirPath()
        {
            var parentFolder = Path.GetDirectoryName(Config.BigSqlFilePath);
            var cacheDirPath = Path.Combine(parentFolder, CacheDirName);
            return cacheDirPath;
        }

        protected string GetConfigFileName(string bigSqlFilePath = null)
        {
            if (string.IsNullOrWhiteSpace(bigSqlFilePath)) throw new ArgumentException($"{nameof(bigSqlFilePath)} cannot be null or whitespace", nameof(bigSqlFilePath));
            else
            {
                var sqlFileName = Path.GetFileName(bigSqlFilePath);
                var configFileName = $"{sqlFileName}.config";
                return configFileName;
            }
        }

        protected static string GetDefaultConfigFilePath()
        {
            var assemblyDirPath = AppDomain.CurrentDomain.BaseDirectory;
            var configFileName = DefaultConfigFileName;
            var configFilePath = Path.Combine(assemblyDirPath, CacheDirName, configFileName);
            return configFilePath;
        }

        public void SaveConfig(string configFilePath = null)
        {
            if (null == configFilePath) configFilePath = GetDefaultConfigFilePath();
            else if (string.IsNullOrWhiteSpace(configFilePath)) throw new ArgumentException($"{nameof(configFilePath)} cannot be blank", nameof(configFilePath));

            Config.SaveToFile(configFilePath);
        }

        public bool LoadConfig(string configFilePath = null)
        {
            if (null == configFilePath) configFilePath = GetDefaultConfigFilePath();
            else if (string.IsNullOrWhiteSpace(configFilePath)) throw new ArgumentException($"{nameof(configFilePath)} cannot be blank", nameof(configFilePath));
            if (false == File.Exists(configFilePath)) return false;

            Config = BigSqlRunnerConfig.FromFile(configFilePath);
            return true;
        }

        protected string GetSessionLevelDbDirName(string bigSqlFilePath)
        {
            var fileNameWoExt = Path.GetFileNameWithoutExtension(bigSqlFilePath);
            var sessionLevelDbDirName = $".{fileNameWoExt}.bsrjob";
            return sessionLevelDbDirName;
        }

        protected string GetSessionLevelDbPath(string bigSqlFilePath)
        {
            var sessionLevelDbDirName = GetSessionLevelDbDirName(bigSqlFilePath);

            var cacheDirPath = GetCacheDirPath();
            var sessionLevelDbPath = Path.Combine(cacheDirPath, sessionLevelDbDirName);
            return sessionLevelDbPath;
        }

        protected string GetSessionBackupPath(string sessionLevelDbPathToBackup)
        {
            var backupPath = $"{sessionLevelDbPathToBackup}.{DateTime.Now:yyyyMMddHHmmss}.backup";
            return backupPath;
        }

        public string ReadSqlUnit(TextReader textReader, string sqlUnitEndingLine, bool includeSqlUnitEndingLine = false)
        {
            var stringBuilder = new StringBuilder();

            string line = null;
            while (null != (line = textReader.ReadLine()))
            {
                if (line.Trim().ToLower() == sqlUnitEndingLine.Trim().ToLower())
                {
                    if (includeSqlUnitEndingLine) stringBuilder.AppendLine(line);
                    break;
                }

                stringBuilder.AppendLine(line);
            }

            var sql = stringBuilder.ToString();
            if (null == line && "" == sql) return null;

            return sql;
        }

        public IEnumerable<SqlUnit> ReadBatchSqlUnits(TextReader textReader, int batchSize, string sqlUnitEndingLine, SessionLevelDb sessionDb, ref int sqlUnitIndex)
        {
            var unitList = new List<SqlUnit>();
            for (int i = 0; i < batchSize;)
            {
                var thisUnit = ReadSqlUnit(textReader, sqlUnitEndingLine);
                if (null == thisUnit) break;

                var unit = new SqlUnit(sqlUnitIndex++, thisUnit);
                if (sessionDb.IsSqlUnitAlreadyExecuted(unit.Index, unit.Sql)) continue;

                unitList.Add(unit);
                i++;
            }

            return unitList;
        }

        protected SqlConnection GetSqlConnection(string dbConnectionString)
        {
            if (string.IsNullOrWhiteSpace(dbConnectionString)) throw new ArgumentException($"{nameof(dbConnectionString)} cannot be null or whitespace", nameof(dbConnectionString));

            var sqlConnectionString = new SqlConnection(dbConnectionString);
            return sqlConnectionString;
        }

        protected int RunSql(string connectionString, string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return 0;

            using (var sqlConnection = GetSqlConnection(connectionString))
            {
                var affectedRows = sqlConnection.Execute(sql);
                return affectedRows;
            }
        }

        public bool IsStopped() => Stop;

        public void StopRunning(Action callBack = null)
        {
            StopCallBack = callBack;
            Stop = true;
        }

        public void Run(ProgressReporter progressReporter = null, ErrorReporter errorReporter = null)
        {
            if (false == File.Exists(Config.BigSqlFilePath)) throw new ArgumentException($"specified big sql file not exist: {Config.BigSqlFilePath}", nameof(Config.BigSqlFilePath));

            // reset the stop flag
            if (Stop) return;
            Stop = false;

            var sessionLevelDbPath = GetSessionLevelDbPath(Config.BigSqlFilePath);
            if (false == Config.ContinueFromLastSessionWhenStarted && Directory.Exists(sessionLevelDbPath))
            {
                var sessionBackupPath = GetSessionBackupPath(sessionLevelDbPath);
                Directory.Move(sessionLevelDbPath, sessionBackupPath);
            }

            using (var streamReader = new StreamReader(Config.BigSqlFilePath))
            using (var sessionDb = new SessionLevelDb(sessionLevelDbPath, Config.SessionSaveType))
            {
                int sqlUnitIndex = 0;
                int affectedRows = 0;
                while (true)
                {
                    // stop when requested
                    if (Stop)
                    {
                        StopCallBack?.Invoke();
                        return;
                    }

                    // read from file
                    var sqlUnitList = ReadBatchSqlUnits(streamReader, Config.BatchSize, Config.SqlUnitEndingLine, sessionDb, ref sqlUnitIndex);
                    if (false == sqlUnitList.Any()) break;

                    // prepare sql
                    var batchSql = SqlUnit.CombineSqlUnitList(sqlUnitList);
                    if (false == string.IsNullOrWhiteSpace(batchSql))
                    {
                        // batch execute sql
                        HelperFns.TryThenException(
                            () =>
                            {
                                var thisAffectedRows = RunSql(Config.ConnectionString, batchSql);
                                if (thisAffectedRows > 0) affectedRows += thisAffectedRows;
                            },
                            Config.RetryIntervalWhenError, true, Config.RetryNumberWhenError + 1,
                            (e, i) => errorReporter?.Invoke($"{e.Message}; retry in {Config.RetryIntervalWhenError.TotalSeconds} seconds...")
                        );

                        // set executing status
                        foreach (var sqlUnit in sqlUnitList)
                        {
                            sessionDb.SetSqlUnitExecuteStatus(sqlUnit.Index, sqlUnit.Sql, true);
                        }

                        // report progress
                        progressReporter?.Invoke(sqlUnitIndex + 1, affectedRows);
                    }
                }
            }
        }
    }
}
