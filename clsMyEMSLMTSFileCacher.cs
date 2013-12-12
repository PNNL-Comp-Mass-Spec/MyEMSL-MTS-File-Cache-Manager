using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.IO;
using System.Runtime.InteropServices;

namespace MyEMSL_MTS_File_Cache_Manager
{
	class clsMyEMSLMTSFileCacher
	{
		#region "Constants"

		public const string LOG_DB_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;";
		public const int DEFAULT_MINIMUM_CACHE_FREE_SPACE_GB = 75;

		protected const string SP_NAME_REQUEST_TASK = "RequestMyEMSLCacheTask";
		protected const string SP_NAME_SET_TASK_COMPLETE = "SetMyEMSLCacheTaskComplete";

		/// <summary>
		/// Maximum number of files to archive
		/// </summary>
		/// <remarks>
		/// Since data package uploads always work with the entire data package folder and all subfolders,
		///   this a maximum cap on the number of files that will be stored in MyEMSL for a given data package
		/// If a data package has more than 600 files, then zip up groups of files before archiving to MyEMSL
		/// </remarks>
		protected const int MAX_FILES_TO_ARCHIVE = 600;

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

		public struct udtFileInfo
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
		}


		#endregion

		#region "Class variables"

		protected string mLogDBConnectionString;

		protected PRISM.DataBase.clsExecuteDatabaseSP m_ExecuteSP;
		protected DateTime mLastStatusUpdate;

		protected double mPercentComplete;
		protected DateTime mLastProgressUpdateTime;

		#endregion

		#region "Properties"


		public string ErrorMessage
		{
			get;
			private set;
		}

		public string MTSConnectionString
		{
			get
			{
				return "Data Source=" + this.MTSServer + ";Initial Catalog=MT_Main;Integrated Security=SSPI;";
			}
		}

		public int MinimumCacheFreeSpaceGB
		{
			get;
			set;
		}

		public string MTSServer
		{
			get;
			private set;
		}

		public ePerspective Perspective
		{
			get;
			private set;
		}

		public string ProcessorName
		{
			get
			{
				return "MyEMSLFileCacher_" + Environment.MachineName;
			}
		}


		/// <summary>
		/// Logging level; range is 1-5, where 5 is the most verbose
		/// </summary>
		/// <remarks>Levels are:
		/// DEBUG = 5,
		/// INFO = 4,
		/// WARN = 3,
		/// ERROR = 2,
		/// FATAL = 1</remarks>
		public clsLogTools.LogLevels LogLevel
		{
			get;
			set;
		}

		#endregion

		#region "Win32 API"

		// Pinvoke for API function
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		[return: MarshalAs(UnmanagedType.Bool)]
		public static extern bool GetDiskFreeSpaceEx(
			string lpDirectoryName,
			out ulong lpFreeBytesAvailable,
			out ulong lpTotalNumberOfBytes,
			out ulong lpTotalNumberOfFreeBytes);

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
				this.Perspective = ePerspective.Server;
				this.MTSServer = Environment.MachineName;
			}
			else
			{
				this.Perspective = ePerspective.Client;
				this.MTSServer = serverName;
			}

			this.LogLevel = logLevel;
			mLogDBConnectionString = logDbConnectionString;

			Initialize();
		}

		protected double BytesToGB(long bytes)
		{
			return bytes / 1024.0 / 1024.0 / 1024.0;
		}

		protected void DeleteFolderIfEmpty(string cacheFolderPath, string folderPath)
		{
			try
			{
				var diFolderToDelete = new DirectoryInfo(folderPath);
				var diCacheFolder = new DirectoryInfo(cacheFolderPath);

				if (diFolderToDelete.Exists)
				{
					if (String.Compare(diFolderToDelete.FullName, diCacheFolder.FullName, System.StringComparison.OrdinalIgnoreCase) == 0)
					{
						// Do not delete the cache folder
						return;
					}

					if (diFolderToDelete.GetFileSystemInfos().Length == 0)
					{
						// Folder is safe to delete
						string parentFolder = string.Empty;
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
				ReportError("Error in DeleteFolderIfEmpty for " + folderPath + ": " + ex.Message, false);
			}
		}

		protected DateTime GetDBDate(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return DateTime.Now;
			else
				return (DateTime)value;

		}

		protected int GetDBInt(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return 0;
			else
				return (int)value;

		}

		protected string GetDBString(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return string.Empty;
			else
				return (string)value;

		}

		/// <summary>
		/// Finds the next set of files that would be cached
		/// </summary>
		/// <returns></returns>
		protected List<udtFileInfo> GetFilesToCache()
		{
			return GetFilesToCache(0);
		}

		/// <summary>
		/// Finds the files to cache for the specified cache task
		/// </summary>
		/// <param name="taskID"></param>
		/// <returns></returns>
		protected List<udtFileInfo> GetFilesToCache(int taskID)
		{

			var lstFiles = new List<udtFileInfo>();

			var sql = " SELECT Entry_ID, Dataset_ID, Job, Client_Path, Server_Path," +
					  " Parent_Path, Dataset_Folder, Results_Folder_Name, Filename, Queued" +
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

			using (var cnDB = new SqlConnection(this.MTSConnectionString))
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
						Queued = GetDBDate(reader, "Queued")
					};

					lstFiles.Add(fileInfo);
				}
			}

			return lstFiles;
		}

		private double GetFreeDiskSpaceGB(string cacheFolderPath)
		{
			Int64 freeSpaceBytes = 0;

			try
			{
				var fiCacheFolder = new DirectoryInfo(cacheFolderPath);

				if (fiCacheFolder.FullName.StartsWith(@"\\"))
				{
					// Network Drive
					string folderName = fiCacheFolder.FullName;

					if (!folderName.EndsWith("\\"))
					{
						folderName += '\\';
					}

					ulong free = 0, dummy1 = 0, dummy2 = 0;

					if (GetDiskFreeSpaceEx(folderName, out free, out dummy1, out dummy2))
					{
						freeSpaceBytes = (Int64)free;
					}
					else
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
				ReportError("Error in GetFreeDiskSpaceGB for " + cacheFolderPath + ": " + ex.Message, true);
				freeSpaceBytes = -1;
			}

			return BytesToGB(freeSpaceBytes);
		}

		protected List<udtFileInfo> GetOldestCachedFiles(int maxFileCount)
		{

			var lstFiles = new List<udtFileInfo>();

			if (maxFileCount < 50)
				maxFileCount = 50;

			var sql = "SELECT TOP " + maxFileCount +
					  " Entry_ID, Client_Path, Server_Path, Parent_Path, Dataset_Folder, Results_Folder_Name, Filename" +
					  " FROM V_MyEMSL_FileCache " +
					  " WHERE State = 3 " +
					  " ORDER BY Queued";


			using (var cnDB = new SqlConnection(this.MTSConnectionString))
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

		protected void Initialize()
		{
			this.ErrorMessage = string.Empty;
			this.mLastStatusUpdate = DateTime.UtcNow;

			mPercentComplete = 0;
			mLastProgressUpdateTime = DateTime.UtcNow;

			// Set up the loggers
			const string logFileName = @"Logs\MyEMSLFileCacher";
			clsLogTools.CreateFileLogger(logFileName, this.LogLevel);

			clsLogTools.CreateDbLogger(mLogDBConnectionString, "MyEMSLFileCacher: " + Environment.MachineName);

			// Make initial log entry
			string msg = "=== Started MyEMSL MTS File Cacher v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " ===== ";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			m_ExecuteSP = new PRISM.DataBase.clsExecuteDatabaseSP(this.MTSConnectionString);
			m_ExecuteSP.DBErrorEvent += new PRISM.DataBase.clsExecuteDatabaseSP.DBErrorEventEventHandler(m_ExecuteSP_DBErrorEvent);

		}
		
		/// <summary>
		/// Examines the free disk space in the cache folder
		/// Deletes old files if the free space is below the minimum
		/// </summary>
		/// <returns></returns>
		protected bool ManageCachedFiles()
		{
			const int FILE_COUNT_TO_RETRIEVE = 500;

			try
			{
				if (this.MinimumCacheFreeSpaceGB <= 0)
					return true;

				bool cleanupRequired = true;
				string cacheFolderPath = string.Empty;
				double currentFreeSpaceGB = -1;
				int iterations = 0;

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
							this.MinimumCacheFreeSpaceGB + " GB. " +
							"However, no more files can be purged (none have State = 3 in MT_Main..V_MyEMSL_FileCache)", true);
						return false;

					}

					if (string.IsNullOrWhiteSpace(cacheFolderPath))
					{
						if (this.Perspective == ePerspective.Server)
							cacheFolderPath = lstFiles.First().ServerPath;
						else
							cacheFolderPath = lstFiles.First().ClientPath;
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
					double dataToDeleteGB = MinimumCacheFreeSpaceGB - currentFreeSpaceGB;

					ReportMessage("Disk free space of " + currentFreeSpaceGB.ToString("0.0") + " is below the threshold of " + MinimumCacheFreeSpaceGB + " GB; purge required");

					var success = PurgeOldFiles(lstFiles, cacheFolderPath, dataToDeleteGB);
					if (!success)
						return false;

				}


			}
			catch (Exception ex)
			{
				ReportError("Error in ManageCachedFiles: " + ex.Message, true);
				return false;
			}

			return true;
		}

		protected bool PreviewFilesToCache()
		{
			try
			{
				// Query MT_Main on the MTS server to look for any available files

				var lstFiles = GetFilesToCache();

				if (lstFiles.Count == 0)
				{
					Console.WriteLine(this.MTSServer + " does not have any files that need to be cached");
					return true;
				}

				Console.WriteLine("Files to cache for Dataset_ID: " + lstFiles.First().DatasetID);
				Console.WriteLine("Queued at: " + lstFiles.First().Queued);
				Console.WriteLine("Job" + "\t" + "File_Path");

				foreach (var udtFile in lstFiles)
				{
					string targetPath;
					if (this.Perspective == ePerspective.Server)
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
				ReportError("Error in RequestTask: " + ex.Message, true);
				return false;
			}

			return true;
		}

		protected bool ProcessTask(int taskId, out int completionCode, out string completionMessage, out List<int> lstCachedFileIDs)
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
				int datasetID = firstFileToCache.DatasetID;

				var reader = new MyEMSLReader.Reader
				{
					IncludeAllRevisions = false,
					ThrowErrors = true,
					MaxFileCount = 10000
				};

				// Attach the events
				reader.ErrorEvent += reader_ErrorEvent;
				reader.MessageEvent += reader_MessageEvent;
				reader.ProgressEvent += reader_ProgressEvent;


				var lstArchiveFiles = reader.FindFilesByDatasetID(datasetID);
				var lstArchiveFileIDs = new List<Int64>();

				int errorsLoggedToDB = 0;

				// Filter lstArchiveFiles using the files in lstFilesToCache
				foreach (var udtFile in lstFilesToCache)
				{
					var archiveFile = (from item in lstArchiveFiles
									   where String.Compare(item.SubDirPath, udtFile.ResultsFolderName, StringComparison.OrdinalIgnoreCase) == 0 &&
											 String.Compare(item.Filename, udtFile.Filename, StringComparison.OrdinalIgnoreCase) == 0
									   select item).ToList();

					if (archiveFile.Count == 0)
					{
						// Match not found

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

					downloader.ErrorEvent += reader_ErrorEvent;
					downloader.MessageEvent += reader_MessageEvent;
					downloader.ProgressEvent += reader_ProgressEvent;

					downloader.OverwriteMode = MyEMSLReader.Downloader.Overwrite.IfChanged;

					try
					{
						string cacheFolderPath;
						if (this.Perspective == ePerspective.Server)
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

				if (lstArchiveFileIDs.Count == lstFilesToCache.Count)
					return true;

				completionCode = 2;
				completionMessage = "Unable to cache all of the requested files: " + lstFilesToCache.Count + " requested vs. " + lstArchiveFileIDs.Count + " actually cached";
				return false;

			}
			catch (Exception ex)
			{
				ReportError("Error in ProcessTask: " + ex.Message, true);
				return false;
			}
			
		}

		private bool PurgeOldFiles(IEnumerable<udtFileInfo> lstFiles, string cacheFolderPath, double bytesToDeleteGB)
		{
			try
			{
				int filesDeleted = 0;
				long bytesDeleted = 0;
				int errorsLoggedToDB = 0;

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

							long fileSizeBytes = fiFile.Length;

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
					if (this.Perspective == ePerspective.Server)
						message += " on " + this.MTSServer;

					ReportMessage(message, clsLogTools.LogLevels.INFO, true);
				}

				if (lstPurgedFiles.Count > 0)
				{
					// Update the purge state for these files using an update query
					var sql = " UPDATE T_MyEMSL_FileCache" +
					          " SET State = 5" +
					          " WHERE (Entry_ID IN (" + string.Join(",", lstPurgedFiles) + "))";

					using (var cnDB = new SqlConnection(this.MTSConnectionString))
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
				ReportError("Error in PurgeOldFiles: " + ex.Message, true);
				return false;
			}

			return true;
		}

		protected void ReportMessage(string message)
		{
			ReportMessage(message, clsLogTools.LogLevels.INFO, logToDB: false);
		}

		protected void ReportMessage(string message, clsLogTools.LogLevels logLevel)
		{
			ReportMessage(message, logLevel, logToDB: false);
		}

		protected void ReportMessage(string message, clsLogTools.LogLevels logLevel, bool logToDB)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, logLevel, message);

			if (logToDB)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, logLevel, message);

			OnMessage(new MessageEventArgs(message));
		}

		protected void ReportError(string message)
		{
			ReportError(message, false);
		}

		protected void ReportError(string message, bool logToDB)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, message);

			if (logToDB)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message);

			OnErrorMessage(new MessageEventArgs(message));

			this.ErrorMessage = string.Copy(message);
		}

		protected int RequestTask()
		{
			int taskID = 0;

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
						this.ProcessorName;

					cmd.Parameters.Add(new SqlParameter("@taskAvailable", SqlDbType.TinyInt)).Direction =
						ParameterDirection.Output;

					cmd.Parameters.Add(new SqlParameter("@taskID", SqlDbType.Int)).Direction =
						ParameterDirection.Output;

					cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction =
						ParameterDirection.Output;

				}

				ReportMessage("Calling " + cmd.CommandText + " on " + this.MTSServer, clsLogTools.LogLevels.DEBUG);

				//Execute the SP (retry the call up to 4 times)
				m_ExecuteSP.TimeoutSeconds = 20;
				var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

				if (resCode == 0)
				{
					Int16 taskAvailable = Convert.ToInt16(cmd.Parameters["@taskAvailable"].Value);

					if (taskAvailable > 0)
					{
						taskID = Convert.ToInt32(cmd.Parameters["@taskID"].Value);

						ReportMessage("Received cache task " + taskID + " from " + this.MTSServer, clsLogTools.LogLevels.INFO);
					}
					else
						ReportMessage(cmd.CommandText + " returned taskAvailable = 0 ", clsLogTools.LogLevels.DEBUG);

				}
				else
				{
					string Msg = "Error " + resCode + " requesting a cache task: " + (string)cmd.Parameters["@message"].Value;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
					taskID = 0;
				}

			}
			catch (Exception ex)
			{
				ReportError("Error in RequestTask: " + ex.Message, true);
				taskID = 0;
			}

			return taskID;
		}

		protected int SetTaskComplete(int taskID, int completionCode, string completionMessage, List<int> lstCachedFileIDs)
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

					cmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128)).Value = this.ProcessorName;
					cmd.Parameters.Add(new SqlParameter("@taskID", SqlDbType.Int)).Value = taskID;
					cmd.Parameters.Add(new SqlParameter("@CompletionCode", SqlDbType.Int)).Value = completionCode;
					cmd.Parameters.Add(new SqlParameter("@CompletionMessage", SqlDbType.VarChar, 255)).Value = completionMessage;

					cmd.Parameters.Add(new SqlParameter("@CachedFileIDs", SqlDbType.VarChar, -1)).Value = string.Join(",", lstCachedFileIDs);
					
					cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction =
						ParameterDirection.Output;

				}

				ReportMessage("Calling " + cmd.CommandText + " on " + this.MTSServer, clsLogTools.LogLevels.DEBUG);

				//Execute the SP (retry the call up to 4 times)
				m_ExecuteSP.TimeoutSeconds = 20;
				var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

				if (resCode != 0)
				{
					string Msg = "Error " + resCode + " setting cache task complete: " + (string)cmd.Parameters["@message"].Value;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
					taskID = 0;
				}

			}
			catch (Exception ex)
			{
				ReportError("Error in SetTaskComplete: " + ex.Message, true);
				taskID = 0;
			}

			return taskID;
		}

		/// <summary>
		/// Initiate processing, which will contact the MTS Server to see if any files need to be cached
		/// </summary>
		/// <returns>True if success, false if an error</returns>
		public bool Start()
		{
			bool success = Start(false);
			return success;
		}

		/// <summary>
		/// Initiate processing, which will contact the MTS Server to see if any files need to be cached
		/// </summary>
		/// <param name="previewMode">True to preview the files that would be downloaded</param>
		/// <returns>True if success, false if an error</returns>
		public bool Start(bool previewMode)
		{
			bool success = true;

			if (previewMode)
			{
				success = PreviewFilesToCache();
				return success;
			}

			if (this.MinimumCacheFreeSpaceGB > 0)
			{
				success = ManageCachedFiles();
				if (!success)
					return false;
			}

			bool taskAvailable = true;
			while (success && taskAvailable)
			{
				int taskID = RequestTask();

				if (taskID < 1)
					taskAvailable = false;
				else
				{
					int completionCode;
					string completionMessage;
					List<int> lstCachedFileIDs;

					success = ProcessTask(taskID, out completionCode, out completionMessage, out lstCachedFileIDs);

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

			}

			return success;
		}

		#region "Events"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;

		#endregion

		#region "Event Handlers"


		private void m_ExecuteSP_DBErrorEvent(string Message)
		{
			ReportError("Stored procedure execution error: " + Message, true);
		}


		void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			ReportError("MyEMSLReader error: " + e.Message);
		}

		void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			ReportMessage(e.Message);
		}

		void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
		{
			if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
			{
				if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
				{
					Console.WriteLine("Percent complete: " + e.PercentComplete.ToString("0.0") + "%");
					mPercentComplete = e.PercentComplete;
					mLastProgressUpdateTime = DateTime.UtcNow;
				}
			}
		}

		public void OnErrorMessage(MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

		public void OnMessage(MessageEventArgs e)
		{
			if (MessageEvent != null)
				MessageEvent(this, e);
		}

		public void OnProgressUpdate(ProgressEventArgs e)
		{
			if (ProgressEvent != null)
				ProgressEvent(this, e);
		}

		#endregion

	}
}
