using BigSqlRunner.Library;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace BigSqlRunner.WPF
{
    public enum ProgressDataTypeEnum
    {
        Progress,
        Notification,
    }
    public class ProgressData
    {
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
        public string Message { get; set; }

        public LogItem(ProgressDataTypeEnum logType, string message)
        {
            LogType = logType;
            Message = message;
        }
    }

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        protected Library.BigSqlRunner BigSqlRunner { get; set; }
        protected BackgroundWorker BgWorker { get; set; } = new BackgroundWorker();

        protected int LogQueueSize { get; set; } = 10000;
        protected IList<LogItem> LogQueue { get; set; } = new List<LogItem>();

        public MainWindow()
        {
            InitializeComponent();
        }

        protected Library.BigSqlRunner CreateBigSqlRunner()
        {
            var connectionString = Tb_ConnectionStr.Text;
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException($"db connection string cannot be empty", nameof(Tb_ConnectionStr));

            var bigSqlFilePath = Tb_SqlFilePath.Text;
            if (string.IsNullOrWhiteSpace(bigSqlFilePath)) throw new ArgumentException($"big sql file path cannot be empty", nameof(Tb_SqlFilePath));

            var enableLog = Cb_EnableLog.IsChecked.Value;
            var logFilePath = Tb_LogFilePath.Text;
            if (enableLog && string.IsNullOrWhiteSpace(logFilePath)) throw new ArgumentException($"log file path cannot be empty", nameof(Tb_LogFilePath));

            var sqlUnitEndingLine = Tb_SqlUnitEndingLine.Text;
            if (string.IsNullOrWhiteSpace(sqlUnitEndingLine)) throw new ArgumentException($"sql unit ending line cannot be empty", nameof(Tb_SqlUnitEndingLine));

            int batchSize;
            if (false == int.TryParse(Tb_BatchSize.Text, out batchSize)) throw new ArgumentException($"the value specified for batch size is not a number", nameof(Tb_BatchSize));
            else if (batchSize <= 0) throw new ArgumentException($"batch size must be >= 1", nameof(Tb_BatchSize));

            var sessionSaveType = (SessionSaveTypeEnum)Cmb_SessionSaveType.SelectedValue;
            var continueFromLastSession = Cb_ContinueFromLastSession.IsChecked.Value;

            int retryIntervalSeconds;
            if (false == int.TryParse(Tb_RetryIntervalSeconds.Text, out retryIntervalSeconds)) throw new ArgumentException($"the value specified for retry interval is not a number", nameof(Tb_RetryIntervalSeconds));
            else if (retryIntervalSeconds < 0) throw new ArgumentException($"retry interval must be >= 0", nameof(Tb_RetryIntervalSeconds));

            int retryNumber;
            if (false == int.TryParse(Tb_RetryNumber.Text, out retryNumber)) throw new ArgumentException($"the value specified for retry number is not a number", nameof(Tb_RetryNumber));
            else if (retryNumber < 0) throw new ArgumentException($"retry number must be >= 0", nameof(Tb_RetryNumber));

            var bigSqlRunner = new Library.BigSqlRunner(connectionString, bigSqlFilePath, enableLog, logFilePath, batchSize, sqlUnitEndingLine, continueFromLastSession, sessionSaveType, retryIntervalSeconds, retryNumber);
            return bigSqlRunner;
        }

        protected void DoWork(object sender, DoWorkEventArgs e)
        {
            BigSqlRunner.Run(
                (executedUnits, affectedRows) => BgWorker.ReportProgress(-1, new ProgressData(executedUnits, affectedRows)),
                message => BgWorker.ReportProgress(-1, new ProgressData(message))
            );
        }

        protected void WriteLog(bool alsoWriteToFile)
        {
            var log = string.Join(Environment.NewLine, LogQueue.Select(li => li.Message).Reverse().ToList());
            Tb_Log.Text = log;

            if (BigSqlRunner.Config.EnableLogging)
            {
                File.WriteAllText(BigSqlRunner.Config.LogFilePath, log);
            }
        }

        protected void WriteLogMessage(LogItem logItem)
        {
            lock (LogQueue)
            {
                if (Cb_CompactLog.IsChecked.Value)
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

                WriteLog(ProgressDataTypeEnum.Notification == logItem.LogType);
            }
        }

        protected LogItem MakeLog(ProgressData progressData)
        {
            string message;
            var nowStr = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}";
            var prefix = $"{nowStr}>  ";
            switch (progressData.Type)
            {
                case ProgressDataTypeEnum.Progress:
                    message = $"{prefix}{progressData.ExecutedSqlUnitCount} unit executed, {progressData.AffectedRowCount} rows affected.";
                    break;

                case ProgressDataTypeEnum.Notification:
                    message = $"{prefix}{progressData.Message}";
                    break;

                default: throw new ArgumentException($"unknown value of enum {nameof(ProgressDataTypeEnum)}: {progressData.Type}", nameof(progressData));
            }

            var logItem = new LogItem(progressData.Type, message);
            return logItem;
        }

        protected void ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            var progressData = e.UserState as ProgressData;
            if (null == progressData) return;

            WriteLogMessage(MakeLog(progressData));
        }

        protected void RemoveBgWorkerEh()
        {
            BgWorker.DoWork -= DoWork;
            BgWorker.ProgressChanged -= ProgressChanged;
            BgWorker.RunWorkerCompleted -= WorkerCompleted;
        }

        protected void WorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            RemoveBgWorkerEh();
            EnableInputs();
            ToggleRunCancelButtonEnabledState(false);

            if (BigSqlRunner.IsStopped()) WriteLogMessage(MakeLog(new ProgressData("Canceled by user.")));
            else if (null != e.Error) WriteLogMessage(MakeLog(new ProgressData(e.Error.Message)));
            else WriteLogMessage(MakeLog(new ProgressData("Completed.")));
        }

        protected void ToggleInputEnabledState(bool enable)
        {
            Tb_ConnectionStr.IsEnabled = enable;
            Tb_SqlFilePath.IsEnabled = enable;

            Cb_EnableLog.IsEnabled = enable;
            Tb_LogFilePath.IsEnabled = enable;

            Tb_SqlUnitEndingLine.IsEnabled = enable;
            Tb_BatchSize.IsEnabled = enable;

            Cmb_SessionSaveType.IsEnabled = enable;
            Cb_ContinueFromLastSession.IsEnabled = enable;

            Tb_RetryIntervalSeconds.IsEnabled = enable;
            Tb_RetryNumber.IsEnabled = enable;

            Cb_CompactLog.IsEnabled = enable;
        }
        protected void EnableInputs()
        {
            ToggleInputEnabledState(true);
        }
        protected void DisableInputs()
        {
            ToggleInputEnabledState(false);
        }

        protected void ToggleRunCancelButtonEnabledState(bool running)
        {
            if (running)
            {
                Btn_Run.IsEnabled = false;
                Btn_Cancel.IsEnabled = true;
            }
            else
            {
                Btn_Run.IsEnabled = true;
                Btn_Cancel.IsEnabled = false;
            }
        }

        protected void LoadInputValueFromConfig(BigSqlRunnerConfig config)
        {
            Tb_ConnectionStr.Text = config.ConnectionString;
            Tb_SqlFilePath.Text = config.BigSqlFilePath;

            Cb_EnableLog.IsChecked = config.EnableLogging;
            Tb_LogFilePath.Text = config.LogFilePath;

            Tb_SqlUnitEndingLine.Text = config.SqlUnitEndingLine;
            Tb_BatchSize.Text = config.BatchSize.ToString();

            Cmb_SessionSaveType.SelectedValue = config.SessionSaveType;
            Cb_ContinueFromLastSession.IsChecked = config.ContinueFromLastSessionWhenStarted;

            Tb_RetryIntervalSeconds.Text = ((int)config.RetryIntervalWhenError.TotalSeconds).ToString();
            Tb_RetryNumber.Text = config.RetryNumberWhenError.ToString();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                var bigSqlRunner = Library.BigSqlRunner.FromDefaultConfig();
                LoadInputValueFromConfig(bigSqlRunner.Config);
            }
            catch { }
        }

        private void Btn_Run_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                ToggleRunCancelButtonEnabledState(true);
                DisableInputs();
                BigSqlRunner = CreateBigSqlRunner();
                BigSqlRunner.SaveConfig(null);

                BgWorker.WorkerReportsProgress = true;
                BgWorker.DoWork += DoWork;
                BgWorker.ProgressChanged += ProgressChanged;
                BgWorker.RunWorkerCompleted += WorkerCompleted;

                WriteLogMessage(MakeLog(new ProgressData("Started running...")));
                BgWorker.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                RemoveBgWorkerEh();

                MessageBox.Show(ex.Message);

                EnableInputs();
                ToggleRunCancelButtonEnabledState(false);
            }
        }

        private void Btn_Cancel_Click(object sender, RoutedEventArgs e)
        {
            Btn_Cancel.IsEnabled = false;

            BigSqlRunner.StopRunning();
        }
    }
}
