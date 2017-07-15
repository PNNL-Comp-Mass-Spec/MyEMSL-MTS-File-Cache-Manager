rem Primary Location
xcopy debug\MyEMSL_MTS_File_Cache_Manager.exe \\ProteinSeqs\dms_programs\MyEMSL_Cache_Manager\ /y /d
xcopy debug\*.dll \\ProteinSeqs\dms_programs\MyEMSL_Cache_Manager\ /y /d

rem Backup copy
xcopy debug\MyEMSL_MTS_File_Cache_Manager.exe \\Sylvester\dms_programs\MyEMSL_Cache_Manager\ /y /d
xcopy debug\*.dll \\Sylvester\dms_programs\MyEMSL_Cache_Manager\ /y /d

pause
