using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public enum LogMessageTypeEnum
    {
        Progress,
        Notification,
    }
    public class LogMessage
    {
        public DateTime TimeUtc { get; set; }

        public LogMessageTypeEnum Type { get; set; }

        #region Progress type
        public int? ExecutedSqlUnitCount { get; set; }
        public int? AffectedRowCount { get; set; }
        #endregion

        #region Notification type
        public string Message { get; set; }
        #endregion

        public LogMessage(int? executedSqlUnitCount, int? affectedRowCount) : this(LogMessageTypeEnum.Progress, executedSqlUnitCount, affectedRowCount, null) { }
        public LogMessage(string message) : this(LogMessageTypeEnum.Notification, null, null, message) { }
        public LogMessage(LogMessageTypeEnum type, int? executedSqlUnitCount, int? affectedRowCount, string message)
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
        public DateTime TimeUtc { get; set; }
        public LogMessageTypeEnum Type { get; set; }
        public string Message { get; set; }

        public LogItem(DateTime timeUtc, LogMessageTypeEnum logType, string message)
        {
            TimeUtc = timeUtc;
            Type = logType;
            Message = message;
        }

        public static LogItem MakeLog(LogMessage logMessage)
        {
            string message;
            var nowStr = $"{logMessage.TimeUtc.ToLocalTime():yyyy-MM-dd HH:mm:ss zzz}";
            var prefix = $"{nowStr}>  ";
            switch (logMessage.Type)
            {
                case LogMessageTypeEnum.Progress:
                    message = $"{prefix}(p) {logMessage.ExecutedSqlUnitCount} units executed, {logMessage.AffectedRowCount} rows affected.";
                    break;

                case LogMessageTypeEnum.Notification:
                    message = $"{prefix}(n) {logMessage.Message}";
                    break;

                default: throw new ArgumentException($"unknown value of enum {nameof(LogMessageTypeEnum)}: {logMessage.Type}", nameof(logMessage));
            }

            var logItem = new LogItem(logMessage.TimeUtc, logMessage.Type, message);
            return logItem;
        }

        public static LogItem ParseLog(string logLine)
        {
            if (string.IsNullOrWhiteSpace(logLine)) throw new ArgumentException($"{nameof(logLine)} cannot be null or whitespace", nameof(logLine));

            var splitByTime = logLine.Split(">", 2);
            if (2 != splitByTime.Count()) throw new ArgumentException($"{nameof(logLine)} is of unknown format: [{logLine}]", nameof(logLine));

            DateTime logTime;
            var timeStr = splitByTime[0];
            if (false == DateTime.TryParse(timeStr, out logTime)) throw new ArgumentException($"{nameof(logLine)} does not starts with time in a valid format: [{logLine}]", nameof(logLine));

            var trimedRest = splitByTime[1].Trim();
            if (false == trimedRest.StartsWith('(')) throw new ArgumentException($"{nameof(logLine)} does not have a log message type mark in expected position: [{logLine}]", nameof(logLine));

            var msgTypeMarkEndIndex = trimedRest.IndexOf(')');
            if (2 != msgTypeMarkEndIndex) throw new ArgumentException($"{nameof(logLine)} does not have a log message type mark in valid format: [{logLine}]", nameof(logLine));

            LogMessageTypeEnum msgType;
            var msgTypeMark = trimedRest.Substring(1, msgTypeMarkEndIndex - 1);
            switch (msgTypeMark)
            {
                case "n":
                    msgType = LogMessageTypeEnum.Notification;
                    break;

                case "p":
                    msgType = LogMessageTypeEnum.Progress;
                    break;

                default: throw new ArgumentException($"the log message type mark in {nameof(logLine)} is unknown: {msgTypeMark}", nameof(logLine));
            }

            var logItem = new LogItem(logTime.ToUniversalTime(), msgType, logLine);
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
                        LogMessageTypeEnum.Progress == logItem.Type
                        && LogMessageTypeEnum.Progress == lastLogItem?.Type
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

        public async Task LoadLogFile(string logFile)
        {
            var logItemList = new List<LogItem>();
            using (var streamReader = new StreamReader(logFile))
            {
                string logLine = null;
                while (null != (logLine = await streamReader.ReadLineAsync()))
                {
                    var logItem = LogItem.ParseLog(logLine);
                    logItemList.Add(logItem);

                    if (logItemList.Count() == LogQueueSize) break;
                }
            }

            logItemList.Reverse();
            LogQueue = logItemList;
        }
    }
}
