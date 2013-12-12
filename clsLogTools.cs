
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using log4net;

// Configure log4net using the .log4net file
[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Logging.config", Watch = true)]

namespace MyEMSL_MTS_File_Cache_Manager
{
	public class clsLogTools
	{
		//*********************************************************************************************************
		// Wraps Log4Net functions
		//**********************************************************************************************************

		#region "Enums"
			public enum LogLevels
			{
				DEBUG = 5,
				INFO = 4,
				WARN = 3,
				ERROR = 2,
				FATAL = 1
			}

			public enum LoggerTypes
			{
				LogFile,
				LogDb,
				LogSystem
			}
		#endregion

		#region "Class variables"
			private static readonly ILog m_FileLogger = LogManager.GetLogger("FileLogger");
			private static readonly ILog m_DbLogger = LogManager.GetLogger("DbLogger");
			private static readonly ILog m_SysLogger = LogManager.GetLogger("SysLogger");
			private static readonly ILog m_FtpFileLogger = LogManager.GetLogger("FtpFileLogger");
			private static string m_FileDate;
			private static string m_BaseFileName;
			private static log4net.Appender.FileAppender m_FileAppender;
			private static log4net.Appender.RollingFileAppender m_FtpLogFileAppender;
			private static bool m_FtpLogEnabled = false;
		#endregion

		#region "Properties"
			public static bool FileLogDebugEnabled
			{
				get { return m_FileLogger.IsDebugEnabled; }
			}
		#endregion

		#region "Methods"
			/// <summary>
			/// Writes a message to the logging system
			/// </summary>
			/// <param name="LoggerType">Type of logger to use</param>
			/// <param name="LogLevel">Level of log reporting</param>
			/// <param name="InpMsg">Message to be logged</param>
			public static void WriteLog(LoggerTypes LoggerType, LogLevels LogLevel, string InpMsg)
			{
				ILog MyLogger;

				//Establish which logger will be used
				switch (LoggerType)
				{
					case LoggerTypes.LogDb:
						MyLogger = m_DbLogger;
						break;
					case LoggerTypes.LogFile:
						MyLogger = m_FileLogger;
						// Check to determine if a new file should be started
						string TestFileDate = DateTime.Now.ToString("MM-dd-yyyy");
						if (TestFileDate != m_FileDate)
						{
							m_FileDate = TestFileDate;
							ChangeLogFileName();
						}
						break;
					case LoggerTypes.LogSystem:
						MyLogger = m_SysLogger;
						break;
					default:
						throw new Exception("Invalid logger type specified");
				}
				
				//Send the log message
				switch (LogLevel)
				{
					case LogLevels.DEBUG:
						if (MyLogger.IsDebugEnabled) MyLogger.Debug(InpMsg);
						break;
					case LogLevels.ERROR:					
						if (MyLogger.IsErrorEnabled) MyLogger.Error(InpMsg);
						break;
					case LogLevels.FATAL:
						if (MyLogger.IsFatalEnabled) MyLogger.Fatal(InpMsg);
						break;
					case LogLevels.INFO:
						if (MyLogger.IsInfoEnabled) MyLogger.Info(InpMsg);
						break;
					case LogLevels.WARN:
						if (MyLogger.IsWarnEnabled) MyLogger.Warn(InpMsg);
						break;
					default:
						throw new Exception("Invalid log level specified");
				}
			}	// End sub

			/// <summary>
			/// Overload to write a message and exception to the logging system
			/// </summary>
			/// <param name="LoggerType">Type of logger to use</param>
			/// <param name="LogLevel">Level of log reporting</param></param>
			/// <param name="InpMsg">Message to be logged</param></param>
			/// <param name="Ex">Exception to be logged</param></param>
			public static void WriteLog(LoggerTypes LoggerType, LogLevels LogLevel, string InpMsg, Exception Ex)
			{
				ILog MyLogger = default(ILog);

				//Establish which logger will be used
				switch (LoggerType)
				{
					case LoggerTypes.LogDb:
						MyLogger = m_DbLogger;
						break;
					case LoggerTypes.LogFile:
						MyLogger = m_FileLogger;
						// Check to determine if a new file should be started
						string TestFileDate = DateTime.Now.ToString("MM-dd-yyyy");
						if (TestFileDate != m_FileDate)
						{
							m_FileDate = TestFileDate;
							ChangeLogFileName();
						}
						break;
					case LoggerTypes.LogSystem:
						MyLogger = m_SysLogger;
						break;
					default:
						throw new Exception("Invalid logger type specified");
				}

			
				//Send the log message
				switch (LogLevel)
				{
					case LogLevels.DEBUG:
						if (MyLogger.IsDebugEnabled) MyLogger.Debug(InpMsg, Ex);
						break;
					case LogLevels.ERROR:					
						if (MyLogger.IsErrorEnabled) MyLogger.Error(InpMsg, Ex);
						break;
					case LogLevels.FATAL:
						if (MyLogger.IsFatalEnabled) MyLogger.Fatal(InpMsg, Ex);
						break;
					case LogLevels.INFO:
						if (MyLogger.IsInfoEnabled) MyLogger.Info(InpMsg, Ex);
						break;
					case LogLevels.WARN:
						if (MyLogger.IsWarnEnabled) MyLogger.Warn(InpMsg, Ex);
						break;
					default:
						throw new Exception("Invalid log level specified");
				}
			}	// End sub

			/// <summary>
			/// Writes an FTP transaction message to the FTP logger
			/// </summary>
			/// <param name="inpMsg">Message to log</param>
			public static void WriteFtpLog(string inpMsg)
			{
				if (!m_FtpLogEnabled) return;

				if (m_FtpFileLogger.IsDebugEnabled) m_FtpFileLogger.Debug(inpMsg);
			}	// End sub

			/// <summary>
			/// Changes the base log file name
			/// </summary>
			public static void ChangeLogFileName()
			{
				//Get a list of appenders
				List<log4net.Appender.IAppender> AppendList = FindAppenders("FileAppender");
				if (AppendList == null)
				{
					WriteLog(LoggerTypes.LogSystem, LogLevels.WARN, "Unable to change file name. No appender found");
					return;
				}

				foreach (log4net.Appender.IAppender SelectedAppender in AppendList)
				{
					//Convert the IAppender object to a FileAppender
					log4net.Appender.FileAppender AppenderToChange = SelectedAppender as log4net.Appender.FileAppender;
					if (AppenderToChange == null)
					{
						WriteLog(LoggerTypes.LogSystem, LogLevels.ERROR, "Unable to convert appender");
						return;
					}
					//Change the file name and activate change
					AppenderToChange.File = m_BaseFileName + "_" + m_FileDate + ".txt";
					AppenderToChange.ActivateOptions();
				}
			}	// End sub

			/// <summary>
			/// Gets the specified appender
			/// </summary>
			/// <param name="AppendName">Name of appender to find</param>
			/// <returns>List(IAppender) objects if found; NULL otherwise</returns></returns>
			private static List<log4net.Appender.IAppender> FindAppenders(string AppendName)
			{
				//Get a list of the current loggers
				ILog[] LoggerList = LogManager.GetCurrentLoggers();
				if (LoggerList.GetLength(0) < 1) return null;

				//Create a List of appenders matching the criteria for each logger
				List<log4net.Appender.IAppender> RetList = new List<log4net.Appender.IAppender>();
				foreach (ILog TestLogger in LoggerList)
				{
					foreach (log4net.Appender.IAppender TestAppender in TestLogger.Logger.Repository.GetAppenders())
					{
						if (TestAppender.Name == AppendName) RetList.Add(TestAppender);
					}
				}

				//Return the list of appenders, if any found
				if (RetList.Count > 0)
				{
					return RetList;
				}
				else
				{
					return null;
				}
			}	// End sub

			/// <summary>
			/// Sets the file logging level via an integer value (Overloaded)
			/// </summary>
			/// <param name="InpLevel">"InpLevel">Integer corresponding to level (1-5, 5 being most verbose)</param>
			public static void SetFileLogLevel(int InpLevel)
			{
				Type LogLevelEnumType = typeof(LogLevels);

				//Verify input level is a valid log level
				if (!Enum.IsDefined(LogLevelEnumType, InpLevel))
				{
					WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, "Invalid value specified for level: " + InpLevel.ToString());
					return;
				}

				//Convert input integer into the associated enum
				LogLevels Lvl = (LogLevels)Enum.Parse(LogLevelEnumType, InpLevel.ToString());

				SetFileLogLevel(Lvl);
			}	// End sub

			/// <summary>
			/// Sets file logging level based on enumeration (Overloaded)
			/// </summary>
			/// <param name="InpLevel">LogLevels value defining level (Debug is most verbose)</param>
			public static void SetFileLogLevel(LogLevels InpLevel)
			{
				log4net.Repository.Hierarchy.Logger LogRepo = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;

				switch (InpLevel)
				{
					case LogLevels.DEBUG:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["DEBUG"];
						break;
					case LogLevels.ERROR:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["ERROR"];
						break;
					case LogLevels.FATAL:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["FATAL"];
						break;
					case LogLevels.INFO:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["INFO"];
						break;
					case LogLevels.WARN:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["WARN"];
						break;
				}
			}	// End sub

			/// <summary>
			/// Creates a file appender
			/// </summary>
			/// <param name="LogfileName">Log file name for the appender to use</param>
			/// <returns>A configured file appender</returns>
			private static log4net.Appender.FileAppender CreateFileAppender(string LogfileName)
			{
				log4net.Appender.FileAppender ReturnAppender = new log4net.Appender.FileAppender();

				ReturnAppender.Name = "FileAppender";
				m_FileDate = DateTime.Now.ToString("MM-dd-yyyy");
				m_BaseFileName = LogfileName;
				ReturnAppender.File = m_BaseFileName + "_" + m_FileDate + ".txt";
				ReturnAppender.AppendToFile = true;
				log4net.Layout.PatternLayout Layout = new log4net.Layout.PatternLayout();
				Layout.ConversionPattern = "%date{MM/dd/yyyy HH:mm:ss}, %message, %level,%newline";
				Layout.ActivateOptions();
				ReturnAppender.Layout = Layout;
				ReturnAppender.ActivateOptions();

				return ReturnAppender;
			}	// End sub

			/// <summary>
			/// Creates a file appender for FTP transaction logging
			/// </summary>
			/// <param name="logFileName">Log file name for the appender to use</param>
			/// <returns>A configured file appender</returns>
			private static log4net.Appender.RollingFileAppender CreateFtpLogfileAppender(string logFileName)
			{
				log4net.Appender.RollingFileAppender ReturnAppender = new log4net.Appender.RollingFileAppender();

				ReturnAppender.Name = "RollingFileAppender";
				//m_FileDate = DateTime.Now.ToString("MM-dd-yyyy");
				//m_BaseFileName = logFileName;
				ReturnAppender.File = "FTPLog_";
				ReturnAppender.AppendToFile = true;
				ReturnAppender.RollingStyle = log4net.Appender.RollingFileAppender.RollingMode.Date;
				ReturnAppender.DatePattern = "yyyyMMdd";
				log4net.Layout.PatternLayout Layout = new log4net.Layout.PatternLayout();
				Layout.ConversionPattern = "%message%newline";
				Layout.ActivateOptions();
				ReturnAppender.Layout = Layout;
				ReturnAppender.ActivateOptions();

				return ReturnAppender;
			}	// End sub

			/// <summary>
			/// Configures the file logger
			/// </summary>
			/// <param name="LogFileName">Base name for log file</param>
			/// <param name="LogLevel">Debug level for file logger</param>
			public static void CreateFileLogger(string LogFileName, int LogLevel)
			{
				log4net.Repository.Hierarchy.Logger curLogger = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;
				m_FileAppender = CreateFileAppender(LogFileName);
				curLogger.AddAppender(m_FileAppender);
				SetFileLogLevel(LogLevel);
			}	// End sub

			/// <summary>
			/// Configures the file logger
			/// </summary>
			/// <param name="LogFileName">Base name for log file</param>
			/// <param name="LogLevel">Debug level for file logger</param>
			public static void CreateFileLogger(string LogFileName, LogLevels LogLevel)
			{
				CreateFileLogger(LogFileName, (int)LogLevel);
			}

			/// <summary>
			/// Configures the FTP logger
			/// </summary>
			/// <param name="logFileName">Name of FTP log file</param>
			public static void CreateFtpLogFileLogger(string logFileName)
			{
				log4net.Repository.Hierarchy.Logger curLogger = (log4net.Repository.Hierarchy.Logger)m_FtpFileLogger.Logger;
				m_FtpLogFileAppender = CreateFtpLogfileAppender(logFileName);
				curLogger.AddAppender(m_FtpLogFileAppender);
				curLogger.Level = log4net.Core.Level.Debug;
				m_FtpLogEnabled = true;
			}	// End sub

			/// <summary>
			/// Configures the Db logger
			/// </summary>
			/// <param name="ConnStr">Database connection string</param>
			/// <param name="ModuleName">Module name used by logger</param></param>
			public static void CreateDbLogger(string ConnStr, string ModuleName)
			{
				log4net.Repository.Hierarchy.Logger curLogger = (log4net.Repository.Hierarchy.Logger)m_DbLogger.Logger;
				curLogger.Level = log4net.Core.Level.Info;
				curLogger.AddAppender(CreateDbAppender(ConnStr,ModuleName));
				curLogger.AddAppender(m_FileAppender);
			}	// End sub

			/// <summary>
			/// Creates a database appender
			/// </summary>
			/// <param name="ConnStr">Database connection string</param>
			/// <param name="ModuleName">Module name used by logger</param>
			/// <returns>ADONet database appender</returns>
			public static log4net.Appender.AdoNetAppender CreateDbAppender(string ConnStr, string ModuleName)
			{
				log4net.Appender.AdoNetAppender ReturnAppender = new log4net.Appender.AdoNetAppender();

				ReturnAppender.BufferSize = 1;
				ReturnAppender.ConnectionType = "System.Data.SqlClient.SqlConnection, System.Data, Version=1.0.3300.0, Culture=neutral, PublicKeyToken=b77a5c561934e089";
				ReturnAppender.ConnectionString = ConnStr;
				ReturnAppender.CommandType = CommandType.StoredProcedure;
				ReturnAppender.CommandText = "PostLogEntry";

				//Type parameter
				log4net.Appender.AdoNetAppenderParameter TypeParam = new log4net.Appender.AdoNetAppenderParameter();
				TypeParam.ParameterName = "@type";
				TypeParam.DbType = DbType.String;
				TypeParam.Size = 50;
				TypeParam.Layout = CreateLayout("%level");
				ReturnAppender.AddParameter(TypeParam);

				//Message parameter
				log4net.Appender.AdoNetAppenderParameter MsgParam = new log4net.Appender.AdoNetAppenderParameter();
				MsgParam.ParameterName = "@message";
				MsgParam.DbType = DbType.String;
				MsgParam.Size = 4000;
				MsgParam.Layout = CreateLayout("%message");
				ReturnAppender.AddParameter(MsgParam);

				//PostedBy parameter
				log4net.Appender.AdoNetAppenderParameter PostByParam = new log4net.Appender.AdoNetAppenderParameter();
				PostByParam.ParameterName = "@postedBy";
				PostByParam.DbType = DbType.String;
				PostByParam.Size = 128;
				PostByParam.Layout = CreateLayout(ModuleName);
				ReturnAppender.AddParameter(PostByParam);

				ReturnAppender.ActivateOptions();

				return ReturnAppender;
			}	// End sub

			/// <summary>
			/// Creates a layout object for a Db appender parameter
			/// </summary>
			/// <param name="LayoutStr">Name of parameter</param>
			/// <returns></returns>
			private static log4net.Layout.IRawLayout CreateLayout(string LayoutStr)
			{
				log4net.Layout.RawLayoutConverter LayoutConvert = new log4net.Layout.RawLayoutConverter();
				log4net.Layout.PatternLayout ReturnLayout = new log4net.Layout.PatternLayout();
				ReturnLayout.ConversionPattern = LayoutStr;
				ReturnLayout.ActivateOptions();
				log4net.Layout.IRawLayout retItem = (log4net.Layout.IRawLayout)LayoutConvert.ConvertFrom(ReturnLayout);
				return retItem;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
