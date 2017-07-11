using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.IO;
using PRISM;
using PRISMWin;

namespace MyEMSL_MTS_File_Cache_Manager
{
    class clsMyEMSLMTSFileCacher : clsEventNotifier
    {
        #region "Constants"

        public const string LOG_DB_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;";
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
        /// Client means that this program is running on a separate computer, and thus it should use UNC paths (e.g. \\proteinseqs\MyEMSL_Cache\)
        /// </remarks>
        public enum ePerspective
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

        private clsExecuteDatabaseSP m_ExecuteSP;

        #endregion

        #region "Properties"

        public string ErrorMessage { get; private set; }

        public string MTSConnectionString => "Data Source=" + MTSServer + ";Initial Catalog=MT_Main;Integrated Security=SSPI;";

        public int MinimumCacheFreeSpaceGB { get; set;  }

        public string MTSServer { get; }

        public ePerspective Perspective { get; }

        public string ProcessorName => "MyEMSLFileCacher_" + Environment.MachineName;

        /// <summary>
        /// Logging level; range is 1-5, where 5 is the most verbose
        /// </summary>
        /// <remarks>Levels are:
        /// DEBUG = 5,
        /// INFO = 4,
        /// WARN = 3,
        /// ERROR = 2,
        /// FATAL = 1</remarks>
        public clsLogTools.LogLevels LogLevel { get; set; }

        public bool TraceMode { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>If "serverName" is blank, then will auto-set Perspective to ePerspective.Server</remarks>
        public clsMyEMSLMTSFileCacher(string serverName, clsLogTools.LogLevels logLevel) :
            this(serverName, logLevel, LOG_DB_CONNECTION_STRING)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsMyEMSLMTSFileCacher(string serverName, clsLogTools.LogLevels logLevel, string logDbConnectionString)
        {
            if (string.IsNullOrWhiteSpace(serverName))
            {
                Perspective = ePerspective.Server;
                MTSServer = Environment.MachineName;
            }
            else
            {
                Perspective = ePerspective.Client;
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
                ReportError("Error in DeleteFolderIfEmpty for " + folderPath + ": " + ex.Message, false, ex);
            }
        }

        private DateTime GetDBDate(SqlDataReader reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return DateTime.Now;

            return Convert.ToDateTime(value);
        }

        private int GetDBInt(SqlDataReader reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return 0;

            return Convert.ToInt32(value);
        }

        private string GetDBString(SqlDataReader reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return string.Empty;

            return Convert.ToString(value);
        }

        /// <summary>
        /// Finds the files to cache for the specified cache task
        /// </summary>
        /// <param name="taskID"></param>
        /// <returns></returns>
        private List<udtFileInfo> GetFilesToCache(int taskID = 0)
        {

            var lstFiles = new List<udtFileInfo>();

            var sql = " SELECT Entry_ID, Dataset_ID, Job, Client_Path, Server_Path," +
                      " Parent_Path, Dataset_Folder, Results_Folder_Name, Filename, Queued, Optional" +
                      " FROM V_MyEMSL_FileCache";

            if (taskID > 0)
            {
                sql += " WHERE Task_ID = " + taskID;
            }
            else
            {
                sql += " WHERE State = 1 AND " +
                             " Dataset_ID IN (SELECT TOP 1 Dataset_ID FROM V_MyEMSL_FileCache WHERE State = 1)";
            }

            using (var cnDB = new SqlConnection(MTSConnectionString))
            {
                cnDB.Open();

                var cmd = new SqlCommand(sql, cnDB);
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var fileInfo = new udtFileInfo
                    {
                        EntryID = GetDBInt(reader, "Entry_ID"),
                        DatasetID = GetDBInt(reader, "Dataset_ID"),
                        Job = GetDBInt(reader, "Job"),
                        ClientPath = GetDBString(reader, "Client_Path"),
                        ServerPath = GetDBString(reader, "Server_Path"),
                        ParentPath = GetDBString(reader, "Parent_Path").TrimStart('\\'),
                        DatasetFolder = GetDBString(reader, "Dataset_Folder"),
                        ResultsFolderName = GetDBString(reader, "Results_Folder_Name"),
                        Filename = GetDBString(reader, "Filename"),
                        Queued = GetDBDate(reader, "Queued"),
                        Optional = TinyIntToBool(GetDBInt(reader, "Optional"))
                    };

                    lstFiles.Add(fileInfo);
                }
            }

            return lstFiles;
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

                    if (!clsDiskInfo.GetDiskFreeSpace(folderName, out freeSpaceBytes, out _, out _))
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
                ReportError("Error in GetFreeDiskSpaceGB for " + cacheFolderPath + ": " + ex.Message, true, ex);
                freeSpaceBytes = -1;
            }

            return BytesToGB(freeSpaceBytes);
        }

        private List<udtFileInfo> GetOldestCachedFiles(int maxFileCount)
        {

            var lstFiles = new List<udtFileInfo>();

            if (maxFileCount < 50)
                maxFileCount = 50;

            var sql = "SELECT TOP " + maxFileCount +
                      " Entry_ID, Client_Path, Server_Path, Parent_Path, Dataset_Folder, Results_Folder_Name, Filename" +
                      " FROM V_MyEMSL_FileCache " +
                      " WHERE State = 3 " +
                      " ORDER BY Queued";


            using (var cnDB = new SqlConnection(MTSConnectionString))
            {
                cnDB.Open();

                var cmd = new SqlCommand(sql, cnDB);
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var fileInfo = new udtFileInfo
                    {
                        EntryID = GetDBInt(reader, "Entry_ID"),
                        ClientPath = GetDBString(reader, "Client_Path"),
                        ServerPath = GetDBString(reader, "Server_Path"),
                        ParentPath = GetDBString(reader, "Parent_Path").TrimStart('\\'),
                        DatasetFolder = GetDBString(reader, "Dataset_Folder"),
                        ResultsFolderName = GetDBString(reader, "Results_Folder_Name"),
                        Filename = GetDBString(reader, "Filename")
                    };

                    lstFiles.Add(fileInfo);
                }
            }

            return lstFiles;
        }

        private void Initialize()
        {
            ErrorMessage = string.Empty;

            // Set up the loggers
            const string logFileName = @"Logs\MyEMSLFileCacher";
            clsLogTools.CreateFileLogger(logFileName, LogLevel);

            clsLogTools.CreateDbLogger(mLogDBConnectionString, "MyEMSLFileCacher: " + Environment.MachineName);

            // Make initial log entry
            var msg = "=== Started MyEMSL MTS File Cacher v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " ===== ";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

            m_ExecuteSP = new clsExecuteDatabaseSP(MTSConnectionString);
            m_ExecuteSP.DBErrorEvent += m_ExecuteSP_DBErrorEvent;

        }

        /// <summary>
        /// Examines the free disk space in the cache folder
        /// Deletes old files if the free space is below the minimum
        /// </summary>
        /// <returns></returns>
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

                    var lstFiles = GetOldestCachedFiles(FILE_COUNT_TO_RETRIEVE);

                    if (lstFiles.Count == 0)
                    {
                        if (string.IsNullOrWhiteSpace(cacheFolderPath))
                        {
                            // We can only examine the disk free space if there is at least one non-purged file on this server
                            // since we use that file to determine the path to the cached files
                            // Thus, treat this as "no purging needed"
                            return true;
                        }

                        ReportError(
                            "Disk free space is " + currentFreeSpaceGB.ToString("0.0") + " GB, which is below the threshold of " +
                            MinimumCacheFreeSpaceGB + " GB. " +
                            "However, no more files can be purged (none have State = 3 in MT_Main..V_MyEMSL_FileCache)", true);
                        return false;

                    }

                    if (string.IsNullOrWhiteSpace(cacheFolderPath))
                    {
                        if (Perspective == ePerspective.Server)
                        {
                            cacheFolderPath = lstFiles.First().ServerPath;
                            if (string.IsNullOrEmpty(cacheFolderPath))
                            {
                                ReportError("Server_Path is empty for EntryID " + lstFiles.First().EntryID + ", " + lstFiles.First().Filename +
                                            "; Unable to manage cached files");
                                return false;
                            }
                        }
                        else
                        {
                            cacheFolderPath = lstFiles.First().ClientPath;
                            if (string.IsNullOrEmpty(cacheFolderPath))
                            {
                                ReportError("Client_Path is empty for EntryID " + lstFiles.First().EntryID + ", " + lstFiles.First().Filename +
                                            "; Unable to manage cached files");
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

                    ReportMessage("Disk free space of " + currentFreeSpaceGB.ToString("0.0") + " is below the threshold of " + MinimumCacheFreeSpaceGB + " GB; purge required");

                    var success = PurgeOldFiles(lstFiles, cacheFolderPath, dataToDeleteGB);
                    if (!success)
                        return false;

                }


            }
            catch (Exception ex)
            {
                ReportError("Error in ManageCachedFiles for server " + MTSServer + ": " + ex.Message, true, ex);
                return false;
            }

            return true;
        }

        private bool PreviewFilesToCache()
        {
            try
            {
                // Query MT_Main on the MTS server to look for any available files

                var lstFiles = GetFilesToCache();

                if (lstFiles.Count == 0)
                {
                    Console.WriteLine(MTSServer + " does not have any files that need to be cached");
                    return true;
                }

                Console.WriteLine("Files to cache for Dataset_ID: " + lstFiles.First().DatasetID);
                Console.WriteLine("Queued at: " + lstFiles.First().Queued);
                Console.WriteLine("Job" + "\t" + "File_Path");

                foreach (var udtFile in lstFiles)
                {
                    string targetPath;
                    if (Perspective == ePerspective.Server)
                        targetPath = Path.Combine(udtFile.ServerPath, udtFile.ParentPath);
                    else
                        targetPath = Path.Combine(udtFile.ClientPath, udtFile.ParentPath);

                    targetPath = Path.Combine(targetPath, udtFile.DatasetFolder, udtFile.ResultsFolderName, udtFile.Filename);


                    Console.WriteLine(udtFile.Job + "\t" +
                                      targetPath);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error in PreviewFilesToCache for server " + MTSServer + ": " + ex.Message, true, ex);
                return false;
            }

            return true;
        }

        private bool ProcessTask(int taskId, out int completionCode, out string completionMessage, out List<int> lstCachedFileIDs)
        {
            completionCode = 0;
            completionMessage = string.Empty;
            lstCachedFileIDs = new List<int>();

            try
            {
                var lstFilesToCache = GetFilesToCache(taskId);

                if (lstFilesToCache.Count == 0)
                {
                    completionCode = 1;
                    completionMessage = "GetFilesToCache did not find any queued files for this task";
                    return false;
                }

                // Look for the specified files in MyEMSL
                var firstFileToCache = lstFilesToCache.First();
                var datasetID = firstFileToCache.DatasetID;

                var reader = new MyEMSLReader.Reader
                {
                    IncludeAllRevisions = false,
                    MaxFileCount = 10000,
                    ThrowErrors = true,
                    TraceMode = TraceMode
                };

                // Attach the events
                RegisterEvents(reader);

                var lstArchiveFiles = reader.FindFilesByDatasetID(datasetID);
                var lstArchiveFileIDs = new List<Int64>();

                var errorsLoggedToDB = 0;
                var validFileCountToCache = 0;

                // Filter lstArchiveFiles using the files in lstFilesToCache
                foreach (var udtFile in lstFilesToCache)
                {
                    var archiveFile = (from item in lstArchiveFiles
                                       where string.Equals(item.SubDirPath, udtFile.ResultsFolderName, StringComparison.InvariantCultureIgnoreCase) &&
                                             string.Equals(item.Filename, udtFile.Filename, StringComparison.InvariantCultureIgnoreCase)
                                       select item).ToList();

                    validFileCountToCache++;

                    if (archiveFile.Count == 0)
                    {
                        // Match not found

                        if (udtFile.Optional)
                        {
                            ReportMessage("Skipping optional file not found in MyEMSL: " + Path.Combine(udtFile.ResultsFolderName, udtFile.Filename));
                            validFileCountToCache--;
                            continue;
                        }

                        var logToDB = false;

                        if (errorsLoggedToDB < 50)
                        {
                            errorsLoggedToDB++;
                            logToDB = true;
                        }

                        ReportMessage(
                            "Could not find file " + Path.Combine(udtFile.ResultsFolderName, udtFile.Filename) + " in MyEMSL for dataset " +
                            datasetID + " in MyEMSL", clsLogTools.LogLevels.ERROR, logToDB);

                        continue;
                    }

                    lstArchiveFileIDs.Add(archiveFile.First().FileID);

                    lstCachedFileIDs.Add(udtFile.EntryID);
                }

                if (lstArchiveFileIDs.Count > 0)
                {
                    // Download the files

                    var downloader = new MyEMSLReader.Downloader();
                    RegisterEvents(downloader);

                    downloader.OverwriteMode = MyEMSLReader.Downloader.Overwrite.IfChanged;

                    try
                    {
                        string cacheFolderPath;
                        if (Perspective == ePerspective.Server)
                            cacheFolderPath = firstFileToCache.ServerPath;
                        else
                            cacheFolderPath = firstFileToCache.ClientPath;

                        cacheFolderPath = Path.Combine(cacheFolderPath, firstFileToCache.ParentPath, firstFileToCache.DatasetFolder);
                        downloader.DownloadFiles(lstArchiveFileIDs, cacheFolderPath);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception from downloader: " + ex.Message);
                    }

                }

                if (lstArchiveFileIDs.Count == validFileCountToCache)
                    return true;

                completionCode = 2;
                completionMessage = "Unable to cache all of the requested files: " + lstFilesToCache.Count + " requested vs. " + lstArchiveFileIDs.Count + " actually cached";
                return false;

            }
            catch (Exception ex)
            {
                ReportError("Error in ProcessTask for server " + MTSServer + ": " + ex.Message, true, ex);
                return false;
            }

        }

        private bool PurgeOldFiles(IEnumerable<udtFileInfo> lstFiles, string cacheFolderPath, double bytesToDeleteGB)
        {
            try
            {
                var filesDeleted = 0;
                long bytesDeleted = 0;
                var errorsLoggedToDB = 0;

                var lstPurgedFiles = new List<int>();
                var lstParentFolders = new SortedSet<string>();

                foreach (var udtFile in lstFiles)
                {
                    var filePath = Path.Combine(
                        cacheFolderPath,
                        udtFile.ParentPath,
                        udtFile.DatasetFolder,
                        udtFile.ResultsFolderName,
                        udtFile.Filename);

                    var fiFile = new FileInfo(filePath);
                    if (fiFile.Exists)
                    {
                        try
                        {
                            if (fiFile.IsReadOnly)
                                fiFile.IsReadOnly = false;

                            var fileSizeBytes = fiFile.Length;

                            if (fiFile.Directory != null && !lstParentFolders.Contains(fiFile.Directory.FullName))
                                lstParentFolders.Add(fiFile.Directory.FullName);

                            fiFile.Delete();
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

                            ReportError("Exception deleting file " + fiFile.FullName + ": " + ex.Message, logToDB);
                        }

                    }

                    // Add deleted files (and missing files) to lstPurgedFiles
                    lstPurgedFiles.Add(udtFile.EntryID);

                    if (filesDeleted > 0 && BytesToGB(bytesDeleted) >= bytesToDeleteGB)
                    {
                        break;
                    }
                }

                if (filesDeleted > 0)
                {
                    var message = "Deleted " + filesDeleted + " files to free up " + BytesToGB(bytesDeleted).ToString("0.0") + " GB in " + cacheFolderPath;
                    if (Perspective == ePerspective.Server)
                        message += " on " + MTSServer;

                    ReportMessage(message, clsLogTools.LogLevels.INFO, true);
                }

                if (lstPurgedFiles.Count > 0)
                {
                    // Update the purge state for these files using an update query
                    var sql = " UPDATE T_MyEMSL_FileCache" +
                              " SET State = 5" +
                              " WHERE (Entry_ID IN (" + string.Join(",", lstPurgedFiles) + "))";

                    using (var cnDB = new SqlConnection(MTSConnectionString))
                    {
                        cnDB.Open();

                        var cmd = new SqlCommand(sql, cnDB);
                        var rowsUpdated = cmd.ExecuteNonQuery();

                        if (rowsUpdated < lstPurgedFiles.Count)
                        {
                            ReportMessage(
                                "The number of rows in T_MyEMSL_FileCache updated to state 5 is " + rowsUpdated +
                                ", which is less than the expected value of " + lstPurgedFiles.Count, clsLogTools.LogLevels.WARN, true);
                        }

                    }
                }


                if (lstParentFolders.Count > 0)
                {
                    // Delete empty folders
                    foreach (var folderPath in lstParentFolders)
                    {
                        DeleteFolderIfEmpty(cacheFolderPath, folderPath);
                    }

                }
            }
            catch (Exception ex)
            {
                ReportError("Error in PurgeOldFiles for server " + MTSServer + ": " + ex.Message, true, ex);
                return false;
            }

            return true;
        }

        private void ReportMessage(string message, clsLogTools.LogLevels logLevel = clsLogTools.LogLevels.INFO, bool logToDB = false)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, logLevel, message);

            if (logToDB)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, logLevel, message);

            OnStatusEvent(message);
        }

        private void ReportError(string message, bool logToDB = false, Exception ex = null)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, message);

            if (logToDB)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message);

            OnErrorEvent(message, ex);

            ErrorMessage = string.Copy(message);
        }

        private int RequestTask()
        {
            var taskID = 0;

            try
            {
                //Setup for execution of the stored procedure
                var cmd = new SqlCommand();
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = SP_NAME_REQUEST_TASK;

                    cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction =
                        ParameterDirection.ReturnValue;

                    cmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128)).Value =
                        ProcessorName;

                    cmd.Parameters.Add(new SqlParameter("@taskAvailable", SqlDbType.TinyInt)).Direction =
                        ParameterDirection.Output;

                    cmd.Parameters.Add(new SqlParameter("@taskID", SqlDbType.Int)).Direction =
                        ParameterDirection.Output;

                    cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction =
                        ParameterDirection.Output;

                }

                ReportMessage("Calling " + cmd.CommandText + " on " + MTSServer, clsLogTools.LogLevels.DEBUG);

                //Execute the SP (retry the call up to 4 times)
                m_ExecuteSP.TimeoutSeconds = 20;
                var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

                if (resCode == 0)
                {
                    var taskAvailable = Convert.ToInt16(cmd.Parameters["@taskAvailable"].Value);

                    if (taskAvailable > 0)
                    {
                        taskID = Convert.ToInt32(cmd.Parameters["@taskID"].Value);

                        ReportMessage("Received cache task " + taskID + " from " + MTSServer);
                    }
                    else
                    {
                        ReportMessage(cmd.CommandText + " returned taskAvailable = 0 ", clsLogTools.LogLevels.DEBUG);
                    }
                }
                else
                {
                    var Msg = "Error " + resCode + " requesting a cache task: " + (string)cmd.Parameters["@message"].Value;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
                    taskID = 0;
                }

            }
            catch (Exception ex)
            {
                ReportError("Error in RequestTask for server " + MTSServer + ": " + ex.Message, true, ex);
                taskID = 0;
            }

            return taskID;
        }

        private void SetTaskComplete(int taskID, int completionCode, string completionMessage, IEnumerable<int> lstCachedFileIDs)
        {
            try
            {
                //Setup for execution of the stored procedure
                var cmd = new SqlCommand();
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = SP_NAME_SET_TASK_COMPLETE;

                    cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction =
                        ParameterDirection.ReturnValue;

                    cmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128)).Value = ProcessorName;
                    cmd.Parameters.Add(new SqlParameter("@taskID", SqlDbType.Int)).Value = taskID;
                    cmd.Parameters.Add(new SqlParameter("@CompletionCode", SqlDbType.Int)).Value = completionCode;
                    cmd.Parameters.Add(new SqlParameter("@CompletionMessage", SqlDbType.VarChar, 255)).Value = completionMessage;

                    cmd.Parameters.Add(new SqlParameter("@CachedFileIDs", SqlDbType.VarChar, -1)).Value = string.Join(",", lstCachedFileIDs);

                    cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction =
                        ParameterDirection.Output;

                }

                ReportMessage("Calling " + cmd.CommandText + " on " + MTSServer, clsLogTools.LogLevels.DEBUG);

                //Execute the SP (retry the call up to 4 times)
                m_ExecuteSP.TimeoutSeconds = 20;
                var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

                if (resCode != 0)
                {
                    var Msg = "Error " + resCode + " setting cache task complete: " + (string)cmd.Parameters["@message"].Value;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
                }

            }
            catch (Exception ex)
            {
                ReportError("Error in SetTaskComplete for server " + MTSServer + ": " + ex.Message, true, ex);
            }

        }

        /// <summary>
        /// Initiate processing, which will contact the MTS Server to see if any files need to be cached
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool Start()
        {
            var success = Start(false);
            return success;
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

                int completionCode;
                string completionMessage;
                List<int> lstCachedFileIDs;

                success = ProcessTask(taskID, out completionCode, out completionMessage, out lstCachedFileIDs);
                tasksProcessed += 1;

                if (success)
                {
                    SetTaskComplete(taskID, 0, completionMessage, lstCachedFileIDs);
                }
                else
                {
                    if (completionCode == 0)
                        completionCode = -1;

                    SetTaskComplete(taskID, completionCode, completionMessage, lstCachedFileIDs);
                }
            }

            if (tasksProcessed == 0)
            {
                ReportMessage("No tasks found for " + MTSServer, clsLogTools.LogLevels.DEBUG);
            }

            return success;
        }

        private bool TinyIntToBool(int value)
        {
            if (value == 0)
                return false;

            return true;
        }

    }
}
