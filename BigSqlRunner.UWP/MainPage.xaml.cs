using BigSqlRunner.UWP.Library;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading.Tasks;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.Storage;
using Windows.Storage.AccessCache;
using Windows.Storage.Pickers;
using Windows.UI.Popups;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=402352&clcid=0x409

namespace BigSqlRunner.UWP
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public partial class MainPage : Page
    {
        protected DateTime AppStartTime { get; set; } = DateTime.Now;
        protected DateTime RunnerStartTime { get; set; } = DateTime.Now;

        protected Library.BigSqlRunner BigSqlRunner { get; set; }

        public MainPage()
        {
            this.InitializeComponent();
        }

        protected async Task<Library.BigSqlRunner> CreateBigSqlRunner()
        {
            var connectionString = Tb_ConnectionStr.Text;
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException($"db connection string cannot be empty", nameof(Tb_ConnectionStr));

            var bigSqlFilePath = Tb_SqlFilePath.Text;
            if (string.IsNullOrWhiteSpace(bigSqlFilePath)) throw new ArgumentException($"big sql file path cannot be empty", nameof(Tb_SqlFilePath));

            var logToFile = Cb_LogToFile.IsChecked.Value;
            string logFilePath = null;
            if (logToFile)
            {
                logFilePath = await Library.BigSqlRunner.GetLogFilePath_ByConnStrAndSqlFile(connectionString, bigSqlFilePath, RunnerStartTime);
                if (false == NetPatch.FileExists(logFilePath))
                {
                    RunnerStartTime = DateTime.Now;
                    logFilePath = await Library.BigSqlRunner.GetLogFilePath_ByConnStrAndSqlFile(connectionString, bigSqlFilePath, RunnerStartTime);
                }
            }
            var compactLog = Cb_CompactLog.IsChecked.Value;

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

            var bigSqlRunner = new Library.BigSqlRunner(
                connectionString: connectionString, bigSqlFilePath: bigSqlFilePath,
                enableLogging: logToFile, logFilePath: logFilePath, compactLog: compactLog,
                batchSize: batchSize, sqlUnitEndingLine: sqlUnitEndingLine,
                continueFromLastSessionWhenStarted: continueFromLastSession, sessionSaveType: sessionSaveType,
                retryIntervalWhenError_Seconds: retryIntervalSeconds, retryNumberWhenError: retryNumber);
            await bigSqlRunner.LoadLogFile();
            return bigSqlRunner;
        }

        protected async Task ShowLog(string log)
        {
            Tb_Log.Text = log;
            await Task.CompletedTask;
        }

        protected async Task Run()
        {
            try
            {
                BigSqlRunner.WriteLog += ShowLog;

                await BigSqlRunner.Run((executedUnits, affectedRows) => { }, message => { });
            }
            finally
            {
                BigSqlRunner.WriteLog -= ShowLog;
            }

            EnableInputs();
            ToggleRunCancelButtonEnabledState(false);
        }


        protected void ToggleInputEnabledState(bool enable)
        {
            Tb_ConnectionStr.IsEnabled = enable;
            Tb_SqlFilePath.IsEnabled = enable;
            Btn_SelectSqlFile.IsEnabled = enable;

            Cb_LogToFile.IsEnabled = enable;

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

            Cb_LogToFile.IsChecked = config.EnableLogging;

            Tb_SqlUnitEndingLine.Text = config.SqlUnitEndingLine;
            Tb_BatchSize.Text = config.BatchSize.ToString();

            Cmb_SessionSaveType.SelectedItem = config.SessionSaveType;
            Cb_ContinueFromLastSession.IsChecked = config.ContinueFromLastSessionWhenStarted;

            Tb_RetryIntervalSeconds.Text = ((int)config.RetryIntervalWhenError.TotalSeconds).ToString();
            Tb_RetryNumber.Text = config.RetryNumberWhenError.ToString();
        }

        private async void Page_Loaded(object sender, RoutedEventArgs e)
        {
            // default settings
            {
                Cmb_SessionSaveType.ItemsSource = Enum.GetValues(typeof(SessionSaveTypeEnum));
                Cmb_SessionSaveType.SelectedItem = SessionSaveTypeEnum.SqlUnitIndex;
            }

            // load saved settings
            try
            {
                var lastConfigFile = await Library.BigSqlRunner.GetLastConfigFilePath();
                var bigSqlRunner = Library.BigSqlRunner.FromConfig(lastConfigFile);
                LoadInputValueFromConfig(bigSqlRunner.Config);
            }
            catch { }
        }

        private async void Btn_Run_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                ToggleRunCancelButtonEnabledState(true);
                DisableInputs();
                BigSqlRunner = await CreateBigSqlRunner();

                var configFilePath = await Library.BigSqlRunner.GetConfigFilePath_ByConnStrAndSqlFile(
                    BigSqlRunner.Config.ConnectionString, BigSqlRunner.Config.BigSqlFilePath);
                BigSqlRunner.SaveConfig(configFilePath);

                await Run();
            }
            catch (Exception ex)
            {
                await new MessageDialog(ex.Message).ShowAsync();

                EnableInputs();
                ToggleRunCancelButtonEnabledState(false);
            }
        }

        private void Btn_Cancel_Click(object sender, RoutedEventArgs e)
        {
            Btn_Cancel.IsEnabled = false;
            BigSqlRunner.StopRunning();
        }

        private async void Btn_SelectSqlFile_Click(object sender, RoutedEventArgs e)
        {
            var fileOpenPicker = new FileOpenPicker
            {
                ViewMode = PickerViewMode.List,
                SuggestedStartLocation = PickerLocationId.ComputerFolder,
            };
            fileOpenPicker.FileTypeFilter.Add(".sql");

            var storageFile = await fileOpenPicker.PickSingleFileAsync();
            if (null == storageFile) return;

            Tb_SqlFilePath.Text = storageFile.Path;

            StorageApplicationPermissions.FutureAccessList.AddOrUpdate(storageFile);
            StorageApplicationPermissions.MostRecentlyUsedList.AddOrUpdate(storageFile);
        }
    }
}
