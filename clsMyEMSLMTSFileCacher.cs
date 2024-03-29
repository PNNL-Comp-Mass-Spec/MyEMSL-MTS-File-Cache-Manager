using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.IO;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMWin;

namespace MyEMSL_MTS_File_Cache_Manager
{
    internal class clsMyEMSLMTSFileCacher : EventNotifier
    {
        // Ignore Spelling: downloader, Seqs

        #region "Constants"

        public const string LOG_DB_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";
        public const int DEFAULT_MINIMUM_CACHE_FREE_SPACE_GB = 75;

        private const string SP_NAME_REQUEST_TASK = "RequestMyEMSLCacheTask";
        private const string SP_NAME_SET_TASK_COMPLETE = "SetMyEMSLCacheTaskComplete";

        #endregion

        #region "Enums"

        /// <summary>
        /// Client/server perspective
        /// </summary>
        /// <remarks>
        /// Server means that this program is running on the MTS server and thus it should use local drive paths (e.g. H:\MyEMSL_Cache\)
        /// Additionally, when using Server mode, the value for MTSServer will be auto-determined based on the computer name
        /// Client means that this program is running on a separate computer, and thus it should use UNC paths (e.g. \\ProteinSeqs\MyEMSL_Cache\)
        /// </remarks>
        public enum PerspectiveTypes
        {
            Server = 0,
            Client = 1
        }

        #endregion

        #region "Structures"

        private struct udtFileInfo
        {
            public int EntryID;
            public int DatasetID;
            public int Job;
            public string ClientPath;
            public string ServerPath;
            public string ParentPath;
            public string DatasetFolder;
            public string ResultsFolderName;
            public string Filename;
            public DateTime Queued;
            public bool Optional;
        }

        #endregion

        #region "Class variables"

        private readonly string mLogDBConnectionString;

        private IDBTools mDbTools;

        #endregion

        #region "Properties"

        public string ErrorMessage { get; private set; }

        public string MTSConnectionString => "Data Source=" + MTSServer + ";Initial Catalog=MT_Main;Integrated Security=SSPI;";

        public int MinimumCacheFreeSpaceGB { get; set; }

        public string MTSServer { get; }

        public PerspectiveTypes Perspective { get; }

        public string ProcessorName => "MyEMSLFileCacher_" + Environment.MachineName;

        /// <summary>
        /// Logging level; range is 1-5, where 5 is the most verbose
        /// </summary>
        /// <remarks>Levels are:
        /// DEBUG = 5,
        /// INFO = 4,
        /// WARN = 3,
        /// ERROR = 2,
        /// FATAL = 1
        /// </remarks>
        public BaseLogger.LogLevels LogLevel { get; set; }

        public bool TraceMode { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="logLevel"></param>
        /// <param name="logDbConnectionString"></param>
        /// <remarks>If "serverName" is blank, will auto-set Perspective to PerspectiveTypes.Server</remarks>
        public clsMyEMSLMTSFileCacher(string serverName, BaseLogger.LogLevels logLevel, string logDbConnectionString)
        {
            if (string.IsNullOrWhiteSpace(serverName))
            {
                Perspective = PerspectiveTypes.Server;
                MTSServer = Environment.MachineName;
            }
            else
            {
                Perspective = PerspectiveTypes.Client;
                MTSServer = serverName;
            }

            LogLevel = logLevel;
            mLogDBConnectionString = logDbConnectionString;

            Initialize();
        }

        private double BytesToGB(long bytes)
        {
            return bytes / 1024.0 / 1024.0 / 1024.0;
        }

        private void DeleteFolderIfEmpty(string cacheFolderPath, string folderPath)
        {
            try
            {
                var diFolderToDelete = new DirectoryInfo(folderPath);
                var diCacheFolder = new DirectoryInfo(cacheFolderPath);

                if (diFolderToDelete.Exists)
                {
                    if (string.Equals(diFolderToDelete.FullName, diCacheFolder.FullName, StringComparison.InvariantCultureIgnoreCase))
                    {
                        // Do not delete the cache folder
                        return;
                    }

                    if (diFolderToDelete.GetFileSystemInfos().Length == 0)
                    {
                        // Folder is safe to delete
                        var parentFolder = string.Empty;
                        if (diFolderToDelete.Parent != null)
                        {
                            parentFolder = diFolderToDelete.Parent.FullName;
                        }

                        diFolderToDelete.Delete();

                        if (!string.IsNullOrWhiteSpace(parentFolder))
                        {
                            // Recursively call this function
                            DeleteFolderIfEmpty(cacheFolderPath, parentFolder);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in DeleteFolderIfEmpty for {0}: {1}", folderPath, ex.Message), false, ex);
            }
        }

        /// <summary>
        /// Finds the files to cache for the specified cache task
        /// </summary>
        /// <param name="taskID"></param>
        private List<udtFileInfo> GetFilesToCache(int taskID = 0)
        {
            var filesToCache = new List<udtFileInfo>();

            var sql = " SELECT entry_id, dataset_id, job, client_path, server_path," +
                      "        parent_path, dataset_folder, results_folder_name, filename, queued, optional" +
                      " FROM V_MyEMSL_FileCache";

            if (taskID > 0)
            {
                sql += " WHERE Task_ID = " + taskID;
            }
            else
            {
                sql += " WHERE State = 1 AND " +
                       "       Dataset_ID IN (SELECT Dataset_ID " +
                                            " FROM V_MyEMSL_FileCache" +
                                            " WHERE State = 1" +
                                            " ORDER BY Entry_Id" +
                                            " OFFSET 0 ROWS" +
                                            " FETCH FIRST 1 ROW ONLY)";
            }

            var success = mDbTools.GetQueryResultsDataTable(sql, out var dataTable);

            if (success)
            {
                foreach (DataRow row in dataTable.Rows)
                {
                    var fileInfo = new udtFileInfo
                    {
                        EntryID = row["entry_id"].CastDBVal(0),
                        DatasetID = row["dataset_id"].CastDBVal(0),
                        Job = row["job"].CastDBVal(0),
                        ClientPath = row["client_path"].CastDBVal(""),
                        ServerPath = row["server_path"].CastDBVal(""),
                        ParentPath = row["parent_path"].CastDBVal("").TrimStart('\\'),
                        DatasetFolder = row["dataset_folder"].CastDBVal(""),
                        ResultsFolderName = row["results_folder_name"].CastDBVal(""),
                        Filename = row["filename"].CastDBVal(""),
                        Queued = row["queued"].CastDBVal(DateTime.Now),
                        Optional = IntToBool(row["optional"].CastDBVal(0))
                    };

                    filesToCache.Add(fileInfo);
                }
            }

            return filesToCache;
        }

        private double GetFreeDiskSpaceGB(string cacheFolderPath)
        {
            long freeSpaceBytes;

            try
            {
                var fiCacheFolder = new DirectoryInfo(cacheFolderPath);

                if (fiCacheFolder.FullName.StartsWith(@"\\"))
                {
                    // Network Drive
                    var folderName = fiCacheFolder.FullName;

                    if (!folderName.EndsWith("\\"))
                    {
                        folderName += '\\';
                    }

                    if (!DiskInfo.GetDiskFreeSpace(folderName, out freeSpaceBytes, out _, out _))
                    {
                        // Error calling the API
                        ReportError("Error using GetDiskFreeSpaceEx to determine the disk free space of " + folderName, true);
                        freeSpaceBytes = -1;
                    }
                }
                else
                {
                    var diCacheDrive = new DriveInfo(fiCacheFolder.FullName);
                    freeSpaceBytes = diCacheDrive.TotalFreeSpace;
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in GetFreeDiskSpaceGB for {0}: {1}", cacheFolderPath, ex.Message), true, ex);
                freeSpaceBytes = -1;
            }

            return BytesToGB(freeSpaceBytes);
        }

        private List<udtFileInfo> GetOldestCachedFiles(int maxFileCount)
        {
            var oldestCachedFiles = new List<udtFileInfo>();

            if (maxFileCount < 50)
                maxFileCount = 50;

            var sql = " SELECT entry_id, client_path, server_path, parent_path, dataset_folder, results_folder_name, filename" +
                      " FROM V_MyEMSL_FileCache " +
                      " WHERE State = 3 " +
                      " ORDER BY Queued " +
                      " OFFSET 0 ROWS " +
                      " FETCH FIRST " + maxFileCount + " ROWS ONLY ";

            var success = mDbTools.GetQueryResultsDataTable(sql, out var dataTable);

            if (success)
            {
                foreach (DataRow row in dataTable.Rows)
                {
                    var fileInfo = new udtFileInfo
                    {
                        EntryID = row["entry_id"].CastDBVal(0),
                        ClientPath = row["client_path"].CastDBVal(""),
                        ServerPath = row["server_path"].CastDBVal(""),
                        ParentPath = row["parent_path"].CastDBVal("").TrimStart('\\'),
                        DatasetFolder = row["dataset_folder"].CastDBVal(""),
                        ResultsFolderName = row["results_folder_name"].CastDBVal(""),
                        Filename = row["filename"].CastDBVal("")
                    };

                    oldestCachedFiles.Add(fileInfo);
                }
            }

            return oldestCachedFiles;
        }

        private void Initialize()
        {
            ErrorMessage = string.Empty;

            // Set up the loggers
            const string logFileNameBase = @"Logs\MyEMSLFileCacher";
            LogTools.CreateFileLogger(logFileNameBase, LogLevel);

            var hostName = System.Net.Dns.GetHostName();

            var dbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(mLogDBConnectionString, "MyEMSLMTSFileCacher");

            LogTools.CreateDbLogger(dbLoggerConnectionString, "MyEMSLFileCacher: " + hostName);

            // Make initial log entry
            var msg = string.Format("=== Started MyEMSL MTS File Cacher v{0} === ", System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            LogTools.LogMessage(msg);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(MTSConnectionString, "MyEMSLMTSFileCacher");

            mDbTools = DbToolsFactory.GetDBTools(connectionStringToUse);
            RegisterEvents(mDbTools);
        }

        /// <summary>
        /// Examines the free disk space in the cache folder
        /// Deletes old files if the free space is below the minimum
        /// </summary>
        private bool ManageCachedFiles()
        {
            const int FILE_COUNT_TO_RETRIEVE = 500;

            try
            {
                if (MinimumCacheFreeSpaceGB <= 0)
                    return true;

                var cleanupRequired = true;
                var cacheFolderPath = string.Empty;
                double currentFreeSpaceGB = -1;
                var iterations = 0;

                while (cleanupRequired)
                {
                    iterations++;

                    if (iterations > 25)
                    // This loop has run over 25 times
                    // This likely indicates a problem
                    {
                        ReportError("While loop in ManageCachedFile has run over 25 times; there is likely a problem", true);
                        return false;
                    }

                    // Query the database to retrieve a listing of the 500 oldest cached files

                    var oldestCachedFiles = GetOldestCachedFiles(FILE_COUNT_TO_RETRIEVE);

                    if (oldestCachedFiles.Count == 0)
                    {
                        if (string.IsNullOrWhiteSpace(cacheFolderPath))
                        {
                            // We can only examine the disk free space if there is at least one non-purged file on this server
                            // since we use that file to determine the path to the cached files
                            // Thus, treat this as "no purging needed"
                            return true;
                        }

                        ReportError(string.Format(
                            "Disk free space is {0:0.0} GB, which is below the threshold of {1} GB. " +
                            "However, no more files can be purged (none have State = 3 in MT_Main..V_MyEMSL_FileCache)",
                            currentFreeSpaceGB, MinimumCacheFreeSpaceGB), true);

                        return false;
                    }

                    if (string.IsNullOrWhiteSpace(cacheFolderPath))
                    {
                        if (Perspective == PerspectiveTypes.Server)
                        {
                            cacheFolderPath = oldestCachedFiles.First().ServerPath;
                            if (string.IsNullOrEmpty(cacheFolderPath))
                            {
                                ReportError(string.Format(
                                    "Server_Path is empty for EntryID {0}, {1}; Unable to manage cached files",
                                    oldestCachedFiles.First().EntryID, oldestCachedFiles.First().Filename));

                                return false;
                            }
                        }
                        else
                        {
                            cacheFolderPath = oldestCachedFiles.First().ClientPath;
                            if (string.IsNullOrEmpty(cacheFolderPath))
                            {
                                ReportError(string.Format(
                                    "Client_Path is empty for EntryID {0}, {1}; Unable to manage cached files",
                                    oldestCachedFiles.First().EntryID,
                                    oldestCachedFiles.First().Filename));

                                return false;
                            }
                        }
                    }

                    currentFreeSpaceGB = GetFreeDiskSpaceGB(cacheFolderPath);

                    if (currentFreeSpaceGB < 0)
                    {
                        // Error determining the free disk space
                        return false;
                    }

                    if (currentFreeSpaceGB > MinimumCacheFreeSpaceGB)
                    {
                        cleanupRequired = false;
                        continue;
                    }

                    // Need to delete some files
                    var dataToDeleteGB = MinimumCacheFreeSpaceGB - currentFreeSpaceGB;

                    ReportMessage(string.Format(
                        "Disk free space of {0:0.0} is below the threshold of {1} GB; purge required",
                        currentFreeSpaceGB, MinimumCacheFreeSpaceGB));

                    var success = PurgeOldFiles(oldestCachedFiles, cacheFolderPath, dataToDeleteGB);
                    if (!success)
                        return false;
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in ManageCachedFiles for server {0}: {1}", MTSServer, ex.Message), true, ex);
                return false;
            }

            return true;
        }

        private bool PreviewFilesToCache()
        {
            try
            {
                // Query MT_Main on the MTS server to look for any available files

                var filesToCache = GetFilesToCache();

                if (filesToCache.Count == 0)
                {
                    Console.WriteLine(MTSServer + " does not have any files that need to be cached");
                    return true;
                }

                Console.WriteLine("Files to cache for Dataset_ID: " + filesToCache.First().DatasetID);
                Console.WriteLine("Queued at: " + filesToCache.First().Queued);
                Console.WriteLine("Job" + "\t" + "File_Path");

                foreach (var targetFile in filesToCache)
                {
                    string targetPath;
                    if (Perspective == PerspectiveTypes.Server)
                        targetPath = Path.Combine(targetFile.ServerPath, targetFile.ParentPath);
                    else
                        targetPath = Path.Combine(targetFile.ClientPath, targetFile.ParentPath);

                    targetPath = Path.Combine(targetPath, targetFile.DatasetFolder, targetFile.ResultsFolderName, targetFile.Filename);

                    Console.WriteLine(targetFile.Job + "\t" +
                                      targetPath);
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in PreviewFilesToCache for server {0}: {1}", MTSServer, ex.Message), true, ex);
                return false;
            }

            return true;
        }

        private bool ProcessTask(int taskId, out int completionCode, out string completionMessage, out List<int> cachedFileIDs)
        {
            completionCode = 0;
            completionMessage = string.Empty;
            cachedFileIDs = new List<int>();

            try
            {
                var filesToCache = GetFilesToCache(taskId);

                if (filesToCache.Count == 0)
                {
                    completionCode = 1;
                    completionMessage = "GetFilesToCache did not find any queued files for this task";
                    return false;
                }

                // Look for the specified files in MyEMSL
                var firstFileToCache = filesToCache.First();
                var datasetID = firstFileToCache.DatasetID;

                var reader = new Reader
                {
                    IncludeAllRevisions = false,
                    MaxFileCount = 10000,
                    ThrowErrors = true,
                    TraceMode = TraceMode
                };

                // Attach the events
                RegisterEvents(reader);

                var archiveFiles = reader.FindFilesByDatasetID(datasetID);
                var archiveFileIDs = new Dictionary<long, ArchivedFileInfo>();

                var errorsLoggedToDB = 0;
                var validFileCountToCache = 0;

                // Filter archiveFiles using the files in filesToCache
                foreach (var targetFile in filesToCache)
                {
                    var archiveFile = (from item in archiveFiles
                                       where string.Equals(item.SubDirPath, targetFile.ResultsFolderName, StringComparison.InvariantCultureIgnoreCase) &&
                                             string.Equals(item.Filename, targetFile.Filename, StringComparison.InvariantCultureIgnoreCase)
                                       orderby item.FileID descending
                                       select item).ToList();

                    validFileCountToCache++;

                    if (archiveFile.Count == 0)
                    {
                        // Match not found

                        if (targetFile.Optional)
                        {
                            ReportMessage("Skipping optional file not found in MyEMSL: " + Path.Combine(targetFile.ResultsFolderName, targetFile.Filename));
                            validFileCountToCache--;
                            continue;
                        }

                        var logToDB = false;

                        if (errorsLoggedToDB < 50)
                        {
                            errorsLoggedToDB++;
                            logToDB = true;
                        }

                        ReportMessage(string.Format(
                               "Could not find file {0} in MyEMSL for dataset ID {1}",
                               Path.Combine(targetFile.ResultsFolderName, targetFile.Filename), datasetID),
                           BaseLogger.LogLevels.ERROR, logToDB);

                        continue;
                    }

                    var firstArchiveFile = archiveFile.First();

                    archiveFileIDs.Add(firstArchiveFile.FileID, firstArchiveFile);

                    cachedFileIDs.Add(targetFile.EntryID);
                }

                if (archiveFileIDs.Count > 0)
                {
                    // Download the files
                    var downloader = new Downloader();
                    RegisterEvents(downloader);

                    downloader.OverwriteMode = Downloader.Overwrite.IfChanged;

                    try
                    {
                        string cacheFolderPath;
                        if (Perspective == PerspectiveTypes.Server)
                            cacheFolderPath = firstFileToCache.ServerPath;
                        else
                            cacheFolderPath = firstFileToCache.ClientPath;

                        cacheFolderPath = Path.Combine(cacheFolderPath, firstFileToCache.ParentPath, firstFileToCache.DatasetFolder);
                        downloader.DownloadFiles(archiveFileIDs, cacheFolderPath);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception from downloader: " + ex.Message);
                    }
                }

                if (archiveFileIDs.Count == validFileCountToCache)
                    return true;

                completionCode = 2;
                completionMessage = string.Format(
                    "Unable to cache all of the requested files: {0} requested vs. {1} actually cached",
                    filesToCache.Count, archiveFileIDs.Count);

                return false;
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in ProcessTask for server {0}: {1}", MTSServer, ex.Message), true, ex);
                return false;
            }
        }

        private bool PurgeOldFiles(IEnumerable<udtFileInfo> oldestCachedFiles, string cacheFolderPath, double bytesToDeleteGB)
        {
            try
            {
                var filesDeleted = 0;
                long bytesDeleted = 0;
                var errorsLoggedToDB = 0;

                var purgedFiles = new List<int>();
                var parentFolders = new SortedSet<string>();

                foreach (var targetFileInfo in oldestCachedFiles)
                {
                    var filePath = Path.Combine(
                        cacheFolderPath,
                        targetFileInfo.ParentPath,
                        targetFileInfo.DatasetFolder,
                        targetFileInfo.ResultsFolderName,
                        targetFileInfo.Filename);

                    var targetFile = new FileInfo(filePath);
                    if (targetFile.Exists)
                    {
                        try
                        {
                            if (targetFile.IsReadOnly)
                                targetFile.IsReadOnly = false;

                            var fileSizeBytes = targetFile.Length;

                            if (targetFile.Directory != null)
                            {
                                // Add the parent directory of the target file (if not yet present)
                                parentFolders.Add(targetFile.Directory.FullName);
                            }

                            targetFile.Delete();
                            bytesDeleted += fileSizeBytes;
                            filesDeleted++;
                        }
                        catch (Exception ex)
                        {
                            var logToDB = false;

                            if (errorsLoggedToDB < 50)
                            {
                                errorsLoggedToDB++;
                                logToDB = true;
                            }

                            ReportError("Exception deleting file " + targetFile.FullName + ": " + ex.Message, logToDB);
                        }
                    }

                    // Add deleted files (and missing files) to purgedFiles
                    purgedFiles.Add(targetFileInfo.EntryID);

                    if (filesDeleted > 0 && BytesToGB(bytesDeleted) >= bytesToDeleteGB)
                    {
                        break;
                    }
                }

                if (filesDeleted > 0)
                {
                    var message = "Deleted " + filesDeleted + " files to free up " + BytesToGB(bytesDeleted).ToString("0.0") + " GB in " + cacheFolderPath;
                    if (Perspective == PerspectiveTypes.Server)
                        message += " on " + MTSServer;

                    ReportMessage(message, BaseLogger.LogLevels.INFO, true);
                }

                if (purgedFiles.Count > 0)
                {
                    // Update the purge state for these files using an update query
                    var sql =
                        "UPDATE T_MyEMSL_FileCache " +
                        "SET State = 5 " +
                        "WHERE (Entry_ID IN (" + string.Join(",", purgedFiles) + "))";

                    var sqlCommand = mDbTools.CreateCommand(sql);

                    var rowsUpdated = sqlCommand.ExecuteNonQuery();

                    if (rowsUpdated < purgedFiles.Count)
                    {
                        ReportMessage(
                            "The number of rows in T_MyEMSL_FileCache updated to state 5 is " + rowsUpdated +
                            ", which is less than the expected value of " + purgedFiles.Count, BaseLogger.LogLevels.WARN, true);
                    }
                }

                if (parentFolders.Count > 0)
                {
                    // Delete empty folders
                    foreach (var folderPath in parentFolders)
                    {
                        DeleteFolderIfEmpty(cacheFolderPath, folderPath);
                    }
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in PurgeOldFiles for server {0}: {1}", MTSServer, ex.Message), true, ex);
                return false;
            }

            return true;
        }

        private void RegisterEvents(MyEMSLBase processingClass)
        {
            base.RegisterEvents(processingClass);
            processingClass.MyEMSLOffline += MyEMSLOfflineHandler;
        }

        private void MyEMSLOfflineHandler(string message)
        {
            OnWarningEvent("MyEMSL is offline; unable to retrieve data: " + message);
        }

        private void ReportMessage(string message, BaseLogger.LogLevels logLevel = BaseLogger.LogLevels.INFO, bool logToDB = false)
        {
            if (logToDB)
                LogTools.WriteLog(LogTools.LoggerTypes.LogDb, logLevel, message);
            else
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, logLevel, message);

            OnStatusEvent(message);
        }

        private void ReportError(string message, bool logToDB = false, Exception ex = null)
        {
            if (logToDB)
                LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR, message);
            else
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, message);

            OnErrorEvent(message, ex);

            ErrorMessage = string.Copy(message);
        }

        private int RequestTask()
        {
            try
            {
                //Setup for execution of the stored procedure
                var cmd = mDbTools.CreateCommand(SP_NAME_REQUEST_TASK, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, mDbTools will auto-change "@return" to "_returnCode"
                var returnParam = mDbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                mDbTools.AddParameter(cmd, "@processorName", SqlType.VarChar, 128, ProcessorName);
                var taskAvailableParam = mDbTools.AddParameter(cmd, "@taskAvailable", SqlType.TinyInt, ParameterDirection.InputOutput);
                var taskIdParam = mDbTools.AddParameter(cmd, "@taskID", SqlType.Int, ParameterDirection.InputOutput);
                var messageParam = mDbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                ReportMessage(string.Format("Calling {0} on {1}", SP_NAME_REQUEST_TASK, MTSServer), BaseLogger.LogLevels.DEBUG);

                //Execute the SP (retry the call up to 4 times)
                cmd.CommandTimeout = 20;
                mDbTools.ExecuteSP(cmd, 4);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (returnCode == 0)
                {
                    var taskAvailable = Convert.ToInt16(taskAvailableParam.Value);

                    if (taskAvailable > 0)
                    {
                        var taskID = Convert.ToInt32(taskIdParam.Value);

                        ReportMessage(string.Format("Received cache task {0} from {1}", taskID, MTSServer));
                        return taskID;
                    }

                    ReportMessage(string.Format("{0} returned taskAvailable = 0 ", SP_NAME_REQUEST_TASK), BaseLogger.LogLevels.DEBUG);
                    return 0;
                }

                var outputMessage = messageParam.Value.CastDBVal<string>();
                var message = string.IsNullOrWhiteSpace(outputMessage) ? "Unknown error" : outputMessage;

                LogTools.LogError("Error {0} requesting a cache task: {1}", returnCode, message);
                return 0;
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Error in RequestTask for server {0}: {1}", MTSServer, ex.Message), true, ex);
                return 0;
            }
        }

        private void SetTaskComplete(int taskID, int completionCode, string completionMessage, IEnumerable<int> cachedFileIDs)
        {
            try
            {
                //Setup for execution of the stored procedure
                var cmd = mDbTools.CreateCommand(SP_NAME_SET_TASK_COMPLETE, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, mDbTools will auto-change "@return" to "_returnCode"
                var returnParam = mDbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                mDbTools.AddParameter(cmd, "@processorName", SqlType.VarChar, 128, ProcessorName);
                mDbTools.AddParameter(cmd, "@taskID", SqlType.Int).Value = taskID;
                mDbTools.AddParameter(cmd, "@CompletionCode", SqlType.Int).Value = completionCode;
                mDbTools.AddParameter(cmd, "@CompletionMessage", SqlType.VarChar, 255, completionMessage);
                mDbTools.AddParameter(cmd, "@CachedFileIDs", SqlType.VarChar, -1, string.Join(",", cachedFileIDs));
                var messageParam = mDbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                ReportMessage(string.Format("Calling {0} on {1}", SP_NAME_SET_TASK_COMPLETE, MTSServer), BaseLogger.LogLevels.DEBUG);

                //Execute the SP (retry the call up to 4 times)
                mDbTools.TimeoutSeconds = 20;
                mDbTools.ExecuteSP(cmd, 4);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (returnCode == 0)
                    return;

                var outputMessage = messageParam.Value.CastDBVal<string>();
                var message = string.IsNullOrWhiteSpace(outputMessage) ? "Unknown error" : outputMessage;

                LogTools.LogError("Error {0} setting cache task complete: {1}", returnCode, message);
            }
            catch (Exception ex)
            {
                ReportError(
                    string.Format("Error in SetTaskComplete for server {0}: {1}", MTSServer, ex.Message), true, ex);
            }
        }

        /// <summary>
        /// Initiate processing, which will contact the MTS Server to see if any files need to be cached
        /// </summary>
        /// <param name="previewMode">True to preview the files that would be downloaded</param>
        /// <returns>True if success, false if an error</returns>
        public bool Start(bool previewMode)
        {
            var success = true;

            if (previewMode)
            {
                success = PreviewFilesToCache();
                return success;
            }

            if (MinimumCacheFreeSpaceGB > 0)
            {
                success = ManageCachedFiles();
                if (!success)
                    return false;
            }

            var tasksProcessed = 0;
            while (success)
            {
                var taskID = RequestTask();

                if (taskID < 1)
                    break;

                success = ProcessTask(taskID, out var completionCode, out var completionMessage, out var cachedFileIDs);
                tasksProcessed++;

                if (success)
                {
                    SetTaskComplete(taskID, 0, completionMessage, cachedFileIDs);
                }
                else
                {
                    if (completionCode == 0)
                        completionCode = -1;

                    SetTaskComplete(taskID, completionCode, completionMessage, cachedFileIDs);
                }
            }

            if (tasksProcessed == 0)
            {
                ReportMessage("No tasks found for " + MTSServer, BaseLogger.LogLevels.DEBUG);
            }

            return success;
        }

        private bool IntToBool(int value)
        {
            return value != 0;
        }
    }
}
