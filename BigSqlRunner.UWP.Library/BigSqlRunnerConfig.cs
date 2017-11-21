using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public class BigSqlRunnerConfig
    {
        public string ConnectionString { get; set; }
        public string BigSqlFilePath { get; set; }

        public bool EnableLogging { get; set; } = false;
        public string LogFilePath { get; set; } = null;
        public bool CompactLog { get; set; } = true;
        public int MaxLogItemCount { get; set; } = 10000;

        public int BatchSize { get; set; } = 1;
        public string SqlUnitEndingLine { get; set; } = "GO";

        public bool ContinueFromLastSessionWhenStarted { get; set; } = true;
        public SessionSaveTypeEnum SessionSaveType { get; set; } = SessionSaveTypeEnum.SqlUnitIndex;

        public TimeSpan RetryIntervalWhenError { get; set; } = TimeSpan.FromSeconds(3);
        public int RetryNumberWhenError { get; set; } = 9;

        public BigSqlRunnerConfig(
            string connectionString, string bigSqlFilePath,
            bool enableLogging = false, string logFilePath = null, bool compactLog = true, int maxLogItemCount = 10000,
            int batchSize = 1, string sqlUnitEndingLine = "GO",
            bool continueFromLastSessionWhenStarted = true, SessionSaveTypeEnum sessionSaveType = SessionSaveTypeEnum.SqlUnitIndex,
            int retryIntervalWhenError_Seconds = 3, int retryNumberWhenError = 9
        )
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException($"{nameof(connectionString)} cannot be null or whitespace", nameof(connectionString));
            if (string.IsNullOrWhiteSpace(bigSqlFilePath)) throw new ArgumentException($"{nameof(bigSqlFilePath)} cannot be null or whitespace", nameof(bigSqlFilePath));
            if (enableLogging && string.IsNullOrWhiteSpace(logFilePath)) throw new ArgumentException($"{nameof(logFilePath)} cannot be null or whitespace", nameof(logFilePath));
            if (batchSize <= 0) throw new ArgumentException($"{nameof(batchSize)} must be >= 1", nameof(batchSize));
            if (string.IsNullOrWhiteSpace(sqlUnitEndingLine)) throw new ArgumentException($"{nameof(sqlUnitEndingLine)} cannot be null or whitespace", nameof(sqlUnitEndingLine));
            if (retryIntervalWhenError_Seconds < 0) throw new ArgumentException($"{nameof(retryIntervalWhenError_Seconds)} must be >= 0", nameof(retryIntervalWhenError_Seconds));
            if (retryNumberWhenError < 0) throw new ArgumentException($"{nameof(retryNumberWhenError)} must be >= 0", nameof(retryNumberWhenError));

            ConnectionString = connectionString;
            BigSqlFilePath = bigSqlFilePath;

            EnableLogging = enableLogging;
            LogFilePath = logFilePath;
            CompactLog = compactLog;
            MaxLogItemCount = maxLogItemCount;

            BatchSize = batchSize;
            SqlUnitEndingLine = sqlUnitEndingLine;

            ContinueFromLastSessionWhenStarted = continueFromLastSessionWhenStarted;
            SessionSaveType = sessionSaveType;

            RetryIntervalWhenError = TimeSpan.FromSeconds(retryIntervalWhenError_Seconds);
            RetryNumberWhenError = retryNumberWhenError;
        }

        public string SaveToJson() => ToJson(this);
        public void SaveToFile(string configFilePath) => ToFile(this, configFilePath);

        public static string ToJson(BigSqlRunnerConfig config) => JsonConvert.SerializeObject(config, Formatting.Indented);
        public static void ToFile(BigSqlRunnerConfig config, string configFilePath)
        {
            var configJson = ToJson(config);

            var parentDir = Path.GetDirectoryName(configFilePath);
            if (false == Directory.Exists(parentDir)) Directory.CreateDirectory(parentDir);

            File.WriteAllText(configFilePath, configJson);
        }

        public static BigSqlRunnerConfig FromJson(string configJson) => JsonConvert.DeserializeObject<BigSqlRunnerConfig>(configJson);
        public static BigSqlRunnerConfig FromFile(string configFilePath)
        {
            var configJson = File.ReadAllText(configFilePath);
            var config = FromJson(configJson);
            return config;
        }
    }
}
