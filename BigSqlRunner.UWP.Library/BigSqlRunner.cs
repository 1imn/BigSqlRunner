using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Windows.Storage;

namespace BigSqlRunner.UWP.Library
{
    public class BigSqlRunner
    {
        public BigSqlRunnerConfig Config { get; set; }

        protected static string ConfigDirName { get; set; } = ".configs";
        protected static string LogDirName { get; set; } = ".logs";
        protected static string CacheDirName { get; set; } = ".cache";

        protected static string ConfigFileExtName { get; set; } = ".bsr";
        protected static string LogFileExtName { get; set; } = ".log";
        protected static string CacheItemExtName { get; set; } = ".cache";

        protected bool Stop { get; set; } = false;
        protected Action StopCallBack { get; set; } = null;

        public delegate void ProgressReporter(int executedUnits, int affectedRows);
        public delegate void ErrorReporter(string message);

        protected LogPool LogPool = new LogPool();
        public async Task AddLog(LogItem logItem, bool writeLog = true) => await LogPool.AddLog(logItem, writeLog);
        public async Task<string> GetLog() => await LogPool.GetLog();
        public event Func<string, Task> WriteLog
        {
            add { LogPool.WriteLog += value; }
            remove { LogPool.WriteLog -= value; }
        }

        public BigSqlRunner(
            string connectionString, string bigSqlFilePath,
            bool enableLogging = false, string logFilePath = null, bool compactLog = true, int maxLogItemCount = 10000,
            int batchSize = 1, string sqlUnitEndingLine = "GO",
            bool continueFromLastSessionWhenStarted = true, SessionSaveTypeEnum sessionSaveType = SessionSaveTypeEnum.SqlUnitIndex,
            int retryIntervalWhenError_Seconds = 3, int retryNumberWhenError = 9
        ) : this(
            new BigSqlRunnerConfig(
                connectionString, bigSqlFilePath,
                enableLogging, logFilePath, compactLog, maxLogItemCount,
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
            LogPool = new LogPool(config.MaxLogItemCount, config.CompactLog);
        }

        public async Task LoadLogFile()
        {
            if (NetPatch.FileExists(Config.LogFilePath)) await LogPool.LoadLogFile(Config.LogFilePath);
        }

        #region static
        protected static async Task<string> GetConfigDirPath()
            => (await ApplicationData.Current.LocalFolder.CreateFolderAsync(ConfigDirName, CreationCollisionOption.OpenIfExists)).Path;
        protected static async Task<string> GetLogDirPath()
            => (await ApplicationData.Current.LocalCacheFolder.CreateFolderAsync(LogDirName, CreationCollisionOption.OpenIfExists)).Path;
        protected static async Task<string> GetCacheDirPath()
            => (await ApplicationData.Current.LocalCacheFolder.CreateFolderAsync(CacheDirName, CreationCollisionOption.OpenIfExists)).Path;

        public static async Task<string> GetConfigFilePath(string configFileName)
        {
            if (string.IsNullOrWhiteSpace(configFileName)) throw new ArgumentException($"{nameof(configFileName)} cannot be null or whitespace", nameof(configFileName));

            var configDir = await GetConfigDirPath();
            var configFilePath = Path.Combine(configDir, configFileName);
            return configFilePath;
        }
        public static async Task<string> GetLogFilePath(string logFileName)
        {
            if (string.IsNullOrWhiteSpace(logFileName)) throw new ArgumentException($"{nameof(logFileName)} cannot be null or whitespace", nameof(logFileName));

            var logDir = await GetLogDirPath();
            var logFilePath = Path.Combine(logDir, logFileName);
            return logFilePath;
        }
        public static async Task<string> GetCacheItemPath(string cacheItemName)
        {
            if (string.IsNullOrWhiteSpace(cacheItemName)) throw new ArgumentException($"{nameof(cacheItemName)} cannot be null or whitespace", nameof(cacheItemName));

            var cacheDir = await GetCacheDirPath();
            var cacheItemPath = Path.Combine(cacheDir, cacheItemName);
            return cacheItemPath;
        }

        protected static string GetIdStr_FromConnStrAndSqlFile(string connectionString, string sqlFileName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException($"{nameof(connectionString)} cannot be null or whitespace", nameof(connectionString));
            if (string.IsNullOrWhiteSpace(sqlFileName)) throw new ArgumentException($"{nameof(sqlFileName)} cannot be null or whitespace", nameof(sqlFileName));

            sqlFileName = Path.GetFileName(sqlFileName);

            var idStr = $"{connectionString.Trim().ToLower()} - {sqlFileName.Trim().ToLower()}";
            var md5 = HelperFns.ComputeHash(idStr, MD5.Create());
            var md5Guid = Guid.Parse(md5);

            return md5Guid.ToString();
        }

        public static string GetConfigFileName_ByConnStrAndSqlFile(string connectionString, string sqlFileName)
            => $"{GetIdStr_FromConnStrAndSqlFile(connectionString, sqlFileName)}{ConfigFileExtName}";
        public static async Task<string> GetConfigFilePath_ByConnStrAndSqlFile(string connectionString, string sqlFileName)
            => await GetConfigFilePath(GetConfigFileName_ByConnStrAndSqlFile(connectionString, sqlFileName));

        public static string GetLogFileName_ByConnStrAndSqlFile(string connectionString, string sqlFileName, DateTime time)
            => $"{GetIdStr_FromConnStrAndSqlFile(connectionString, sqlFileName)}.{time:yyyyMMdd-HHmmss}{LogFileExtName}";
        public static async Task<string> GetLogFilePath_ByConnStrAndSqlFile(string connectionString, string sqlFileName, DateTime time)
            => await GetLogFilePath(GetLogFileName_ByConnStrAndSqlFile(connectionString, sqlFileName, time));

        public static string GetCacheItemName_ByConnStrAndSqlFile(string connectionString, string sqlFileName, DateTime time)
            => $"{GetIdStr_FromConnStrAndSqlFile(connectionString, sqlFileName)}.{time:yyyyMMdd-HHmmss}{CacheItemExtName}";
        public static async Task<string> GetCacheItemPath_ByConnStrAndSqlFile(string connectionString, string sqlFileName, DateTime time)
            => await GetCacheItemPath(GetCacheItemName_ByConnStrAndSqlFile(connectionString, sqlFileName, time));

        public static async Task<IEnumerable<FileInfo>> GetConfigFileInfoList()
        {
            var configDir = await GetConfigDirPath();
            var fileList = new DirectoryInfo(configDir).GetFiles($"*{ConfigFileExtName}", SearchOption.TopDirectoryOnly);
            return fileList;
        }

        public static async Task<string> GetLastConfigFilePath()
            => (await GetConfigFileInfoList()).OrderByDescending(fi => fi.LastWriteTime).FirstOrDefault()?.FullName;

        public static BigSqlRunner FromConfig(string configFilePath)
        {
            if (string.IsNullOrWhiteSpace(configFilePath)) throw new ArgumentException($"{nameof(configFilePath)} cannot be null or whitespace", nameof(configFilePath));
            else if (false == NetPatch.FileExists(configFilePath)) return null;

            var config = BigSqlRunnerConfig.FromFile(configFilePath);
            var bigSqlRunner = new BigSqlRunner(config);
            return bigSqlRunner;
        }
        #endregion

        public void SaveConfig(string configFilePath)
        {
            if (string.IsNullOrWhiteSpace(configFilePath)) throw new ArgumentException($"{nameof(configFilePath)} cannot be null or whitespace", nameof(configFilePath));

            Config.SaveToFile(configFilePath);
        }

        public bool LoadConfig(string configFilePath)
        {
            if (string.IsNullOrWhiteSpace(configFilePath)) throw new ArgumentException($"{nameof(configFilePath)} cannot be null or whitespace", nameof(configFilePath));
            else if (false == NetPatch.FileExists(configFilePath)) return false;

            Config = BigSqlRunnerConfig.FromFile(configFilePath);
            return true;
        }

        protected string GetSessionLevelDbDirName(string bigSqlFilePath)
        {
            var fileNameWoExt = Path.GetFileNameWithoutExtension(bigSqlFilePath);
            var sessionLevelDbDirName = $".{fileNameWoExt}.bsrjob";
            return sessionLevelDbDirName;
        }

        protected async Task<string> GetSessionLevelDbPath(string bigSqlFilePath)
        {
            var sessionLevelDbDirName = GetSessionLevelDbDirName(bigSqlFilePath);
            var sessionLevelDbPath = await GetCacheItemPath(sessionLevelDbDirName);
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

        protected async Task<int> RunSql(string connectionString, string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return 0;

            using (var sqlConnection = GetSqlConnection(connectionString))
            using (var sqlCommand = new SqlCommand(sql, sqlConnection))
            {
                await sqlConnection.OpenAsync();
                var affectedRows = await sqlCommand.ExecuteNonQueryAsync();
                return affectedRows;
            }
        }

        protected SemaphoreSlim LogFileWriteLock = new SemaphoreSlim(1, 1);
        protected async Task WriteLogToFile(string log)
        {
            if (Config.EnableLogging)
            {
                try
                {
                    await LogFileWriteLock.WaitAsync();

                    await File.WriteAllTextAsync(Config.LogFilePath, log);
                }
                finally
                {
                    LogFileWriteLock.Release();
                }
            }
        }

        public bool IsStopped() => Stop;

        public void StopRunning(Action callBack = null)
        {
            StopCallBack = callBack;
            Stop = true;
        }

        public async Task Run(ProgressReporter progressReporter = null, ErrorReporter errorReporter = null)
        {
            try
            {
                WriteLog += WriteLogToFile;
                await LogPool.AddLog(LogItem.MakeLog(new LogMessage(">> Started running...")));

                var bigSqlStorageFile = await StorageFile.GetFileFromPathAsync(Config.BigSqlFilePath);
                if (null == bigSqlStorageFile) throw new ArgumentException($"specified big sql file not exist: {Config.BigSqlFilePath}", nameof(Config.BigSqlFilePath));

                // reset the stop flag
                if (Stop) return;
                Stop = false;

                var sessionLevelDbPath = await GetSessionLevelDbPath(Config.BigSqlFilePath);
                if (false == Config.ContinueFromLastSessionWhenStarted && Directory.Exists(sessionLevelDbPath))
                {
                    var sessionBackupPath = GetSessionBackupPath(sessionLevelDbPath);
                    Directory.Move(sessionLevelDbPath, sessionBackupPath);
                }

                using (var readStream = await bigSqlStorageFile.OpenStreamForReadAsync())
                using (var streamReader = new StreamReader(readStream))
                using (var sessionDb = new SessionLevelDb(sessionLevelDbPath, Config.SessionSaveType))
                {
                    int sqlUnitIndex = 0;
                    int affectedRows = 0;
                    while (true)
                    {
                        // stop when requested
                        if (Stop)
                        {
                            await LogPool.AddLog(LogItem.MakeLog(new LogMessage(">> Canceled by user.")));
                            StopCallBack?.Invoke();
                            return;
                        }

                        // read from file
                        var startUnitIndex = sqlUnitIndex;
                        var sqlUnitList = ReadBatchSqlUnits(streamReader, Config.BatchSize, Config.SqlUnitEndingLine, sessionDb, ref sqlUnitIndex);

                        // check skip count
                        var unitIndexDiff = sqlUnitIndex - startUnitIndex;
                        var skipCount = unitIndexDiff - sqlUnitList.Count();
                        if (skipCount > 0)
                        {
                            await LogPool.AddLog(LogItem.MakeLog(new LogMessage($"Skipped {skipCount} already executed units.")));
                        }

                        // break when nothing to do
                        if (false == sqlUnitList.Any()) break;

                        // prepare sql
                        var batchSql = SqlUnit.CombineSqlUnitList(sqlUnitList);
                        if (false == string.IsNullOrWhiteSpace(batchSql))
                        {
                            // batch execute sql
                            await HelperFns.TryThenException(
                                async () =>
                                {
                                    var thisAffectedRows = await RunSql(Config.ConnectionString, batchSql);
                                    if (thisAffectedRows > 0) affectedRows += thisAffectedRows;
                                },
                                Config.RetryIntervalWhenError, true, Config.RetryNumberWhenError + 1,
                                async (e, i) =>
                                {
                                    var message = $"{e.Message}; retry in {Config.RetryIntervalWhenError.TotalSeconds} seconds...";
                                    await LogPool.AddLog(LogItem.MakeLog(new LogMessage(message)));
                                    errorReporter?.Invoke(message);
                                }
                            );

                            // set executing status
                            foreach (var sqlUnit in sqlUnitList)
                            {
                                sessionDb.SetSqlUnitExecuteStatus(sqlUnit.Index, sqlUnit.Sql, true);
                            }

                            // report progress
                            await LogPool.AddLog(LogItem.MakeLog(new LogMessage(sqlUnitIndex, affectedRows)));
                            progressReporter?.Invoke(sqlUnitIndex, affectedRows);
                        }
                    }
                }

                await LogPool.AddLog(LogItem.MakeLog(new LogMessage(">> Completed.")));
            }
            finally
            {
                WriteLog -= WriteLogToFile;
            }
        }
    }
}
