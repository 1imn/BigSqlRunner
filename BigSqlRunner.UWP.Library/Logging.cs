using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public enum ProgressDataTypeEnum
    {
        Progress,
        Notification,
    }
    public class ProgressData
    {
        public DateTime TimeUtc { get; set; }

        public ProgressDataTypeEnum Type { get; set; }

        #region Progress type
        public int? ExecutedSqlUnitCount { get; set; }
        public int? AffectedRowCount { get; set; }
        #endregion

        #region Notification type
        public string Message { get; set; }
        #endregion

        public ProgressData(int? executedSqlUnitCount, int? affectedRowCount) : this(ProgressDataTypeEnum.Progress, executedSqlUnitCount, affectedRowCount, null) { }
        public ProgressData(string message) : this(ProgressDataTypeEnum.Notification, null, null, message) { }
        public ProgressData(ProgressDataTypeEnum type, int? executedSqlUnitCount, int? affectedRowCount, string message)
        {
            TimeUtc = DateTime.UtcNow;

            Type = type;

            #region Progress type
            ExecutedSqlUnitCount = executedSqlUnitCount;
            AffectedRowCount = affectedRowCount;
            #endregion

            #region Notification type
            Message = message;
            #endregion
        }
    }

    public class LogItem
    {
        public ProgressDataTypeEnum LogType { get; set; }
        public DateTime TimeUtc { get; set; }
        public string Message { get; set; }

        public LogItem(DateTime timeUtc, ProgressDataTypeEnum logType, string message)
        {
            TimeUtc = timeUtc;
            LogType = logType;
            Message = message;
        }

        public static LogItem MakeLog(ProgressData progressData)
        {
            string message;
            var nowStr = $"{progressData.TimeUtc.ToLocalTime():yyyy-MM-dd HH:mm:ss}";
            var prefix = $"{nowStr}>  ";
            switch (progressData.Type)
            {
                case ProgressDataTypeEnum.Progress:
                    message = $"{prefix}(p) {progressData.ExecutedSqlUnitCount} units executed, {progressData.AffectedRowCount} rows affected.";
                    break;

                case ProgressDataTypeEnum.Notification:
                    message = $"{prefix}(n) {progressData.Message}";
                    break;

                default: throw new ArgumentException($"unknown value of enum {nameof(ProgressDataTypeEnum)}: {progressData.Type}", nameof(progressData));
            }

            var logItem = new LogItem(progressData.TimeUtc, progressData.Type, message);
            return logItem;
        }
    }

    public class LogPool
    {
        protected int LogQueueSize { get; set; } = 10000;
        protected IList<LogItem> LogQueue { get; set; } = new List<LogItem>();
        protected SemaphoreSlim LogQueueLock = new SemaphoreSlim(1, 1);

        protected bool CompactLog { get; set; } = true;

        public event Func<string, Task> WriteLog;

        public LogPool(int logQueueSize = 10000, bool compactLog = true)
        {
            LogQueueSize = logQueueSize;
            CompactLog = compactLog;
        }

        protected DateTime? LastWriteTimeUtc { get; set; }
        protected string Log { get; set; }
        public async Task<string> GetLog()
        {
            try
            {
                await LogQueueLock.WaitAsync();

                if (false == LogQueue.Any()) return "";

                var lastWriteTimeUtc = LogQueue.Last().TimeUtc;
                if (lastWriteTimeUtc != LastWriteTimeUtc)
                {
                    Log = string.Join(Environment.NewLine, LogQueue.Select(li => li.Message).Reverse().ToList());
                    LastWriteTimeUtc = lastWriteTimeUtc;
                }

                return Log;
            }
            finally
            {
                LogQueueLock.Release();
            }
        }

        public async Task AddLog(LogItem logItem, bool writeLog = true)
        {
            try
            {
                await LogQueueLock.WaitAsync();

                if (CompactLog)
                {
                    var lastLogItem = LogQueue.LastOrDefault();
                    if (
                        ProgressDataTypeEnum.Progress == logItem.LogType
                        && ProgressDataTypeEnum.Progress == lastLogItem?.LogType
                    )
                    {
                        LogQueue.Remove(lastLogItem);
                    }
                }

                LogQueue.Add(logItem);
                if (LogQueue.Count() > LogQueueSize) LogQueue.RemoveAt(0);
            }
            finally
            {
                LogQueueLock.Release();
            }

            if (writeLog && null != WriteLog)
            {
                var totalLog = await GetLog();
                try
                {
                    if (null != WriteLog) await WriteLog(totalLog);
                }
                catch (Exception e)
                {
                    try
                    {
                        Console.WriteLine($"failed to {nameof(WriteLog)}, exception message: {e.Message}");
                    }
                    catch { }
                }
            }
        }
    }
}
