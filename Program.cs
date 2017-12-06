﻿using System;
using System.Collections.Generic;
using System.Threading;
using PRISM;

namespace MyEMSL_MTS_File_Cache_Manager
{
    /// <summary>
    /// This program processes the MyEMSL Download Queue in MTS to download requested files
    /// </summary>
    /// <remarks>
    /// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
    ///
    /// E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
    /// Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/
    /// </remarks>
    internal static class Program
    {

        public const string PROGRAM_DATE = "December 6, 2017";

        private static clsLogTools.LogLevels mLogLevel;

        private static string mMTSServer;
        private static string mLogDBConnectionString;
        private static int mMinimumCacheFreeSpaceGB;
        private static bool mLocalServerMode;
        private static bool mPreviewMode;
        private static bool mTraceMode;

        private static double mPercentComplete;
        private static DateTime mLastProgressUpdateTime;

        public static int Main(string[] args)
        {
            var objParseCommandLine = new clsParseCommandLine();

            mLogLevel = clsLogTools.LogLevels.INFO;

            mMTSServer = string.Empty;
            mLogDBConnectionString = clsMyEMSLMTSFileCacher.LOG_DB_CONNECTION_STRING;
            mMinimumCacheFreeSpaceGB = clsMyEMSLMTSFileCacher.DEFAULT_MINIMUM_CACHE_FREE_SPACE_GB;
            mLocalServerMode = false;
            mPreviewMode = false;
            mTraceMode = false;

            try
            {
                var success = false;

                if (objParseCommandLine.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
                        success = true;
                }

                if (!success ||
                    objParseCommandLine.NeedToShowHelp ||
                    objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0 ||
                    mMTSServer.Length == 0 && !mLocalServerMode)
                {
                    ShowProgramHelp();
                    return -1;

                }

                if (mLocalServerMode)
                {
                    mMTSServer = string.Empty;
                }
                else
                {
                    var updatesArePending = clsWindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage);

                    if (updatesArePending)
                    {
                        Console.WriteLine(pendingWindowsUpdateMessage);
                        Console.WriteLine("Will not contact the MTS server to process cache requests");
                        return 0;
                    }
                }

                var downloader = new clsMyEMSLMTSFileCacher(mMTSServer, mLogLevel, mLogDBConnectionString)
                {
                    MinimumCacheFreeSpaceGB = mMinimumCacheFreeSpaceGB,
                    TraceMode = mTraceMode
                };

                // Attach the events
                downloader.DebugEvent += Downloader_DebugEvent;
                downloader.ErrorEvent += Downloader_ErrorEvent;
                downloader.StatusEvent += Downloader_StatusEvent;
                downloader.WarningEvent += Downloader_WarningEvent;
                downloader.ProgressUpdate += Downloader_ProgressUpdate;

                mPercentComplete = 0;
                mLastProgressUpdateTime = DateTime.UtcNow;

                // Initiate processing, which will contact the MTS Server to see if any files need to be cached
                success = downloader.Start(mPreviewMode);

                if (!success)
                {
                    ShowErrorMessage("Error processing cache requests for MTS server " + mMTSServer + ": " + downloader.ErrorMessage);
                    return -3;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(ex.StackTrace);
                Thread.Sleep(1500);
                return -1;
            }

            return 0;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false
            var lstValidParameters = new List<string> { "Local", "Preview", "Trace", "LogDB", "FS" };

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in objParseCommandLine.InvalidParameters(lstValidParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid commmand line parameters", badArguments);

                    return false;
                }

                // Query objParseCommandLine to see if various parameters are present
                if (objParseCommandLine.NonSwitchParameterCount > 0)
                {
                    mMTSServer = objParseCommandLine.RetrieveNonSwitchParameter(0);
                }

                if (objParseCommandLine.IsParameterPresent("Local"))
                {
                    mLocalServerMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }

                if (objParseCommandLine.RetrieveValueForParameter("LogDB", out var strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/LogDB does not have a value; not overriding the logging connection string");
                    else
                        mLogDBConnectionString = strValue;
                }

                if (objParseCommandLine.RetrieveValueForParameter("FS", out strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/FS does not have a value; not overriding the minimum free space");
                    else
                    {
                        if (!int.TryParse(strValue, out mMinimumCacheFreeSpaceGB))
                            ShowErrorMessage("Error converting " + strValue + " to an integer for parameter /FS");
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }

            return false;
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string message, IReadOnlyCollection<string> additionalInfo)
        {
            ConsoleMsgUtils.ShowErrors(message, additionalInfo);
        }

        private static void ShowProgramHelp()
        {
            var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("This program processes the MyEMSL Download Queue in MTS to download requested files");
                Console.WriteLine();
                Console.WriteLine("Program syntax #1:" + Environment.NewLine + exeName);
                Console.WriteLine(" MTS_Server [/Preview] [/Trace] [/LogDB] [/FS:MinimumFreeSpaceGB]");
                Console.WriteLine();
                Console.WriteLine("Program syntax #2:" + Environment.NewLine + exeName);
                Console.WriteLine(" /Local [/Preview] [/Trace] [/LogDB] [/FS:MinimumFreeSpaceGB]");

                Console.WriteLine();
                Console.WriteLine("MTS_Server specifies the MTS server to contact (using database MT_Main)");
                Console.WriteLine("Alternatively, use /Local to contact MT_Main on the local server running this program");
                Console.WriteLine();
                Console.WriteLine("Use /Preview to preview any files that would be uploaded");
                Console.WriteLine("Use /Trace to show additional log messages");
                Console.WriteLine();
                Console.WriteLine("Use /LogDB to override the default connection string for logging messages to a database");
                Console.WriteLine("The default is /LogDB:" + clsMyEMSLMTSFileCacher.LOG_DB_CONNECTION_STRING);
                Console.WriteLine();
                Console.WriteLine("Use /FS to customize the minimum free disk space that must be present on the cache drive");
                Console.WriteLine("The default is /FS:" + clsMyEMSLMTSFileCacher.DEFAULT_MINIMUM_CACHE_FREE_SPACE_GB);
                Console.WriteLine("Use /FS:0 to disable examining the free disk space");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/");
                Console.WriteLine();

                // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(1500);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error displaying the program syntax: " + ex.Message);
            }

        }
        
        #region "Event Handlers"

        private static void Downloader_DebugEvent(string strMessage)
        {
            ConsoleMsgUtils.ShowDebug(strMessage);
        }

        private static void Downloader_ErrorEvent(string errorMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError(errorMessage, ex);
        }

        private static void Downloader_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Downloader_WarningEvent(string strMessage)
        {
            ConsoleMsgUtils.ShowWarning(strMessage);
        }

        private static void Downloader_ProgressUpdate(string progressMessage, float percentComplete)
        {
            if (!(percentComplete > mPercentComplete) && !(DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30))
                return;

            if (!(DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1))
                return;

            Console.WriteLine("Percent complete: " + percentComplete.ToString("0.0") + "%");
            mPercentComplete = percentComplete;
            mLastProgressUpdateTime = DateTime.UtcNow;
        }

        #endregion

    }
}
