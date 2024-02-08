/*backup*/
 
--full
 
BACKUP DATABASE [TEST_new] 
TO  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_full.bak' 
WITH NOFORMAT
, NOINIT
,  NAME = N'TEST_new-Полная База данных Резервное копирование'
, SKIP
, NOREWIND
, NOUNLOAD
,  STATS = 10
GO
 
--detailed
 
BACKUP DATABASE [TEST_new]
TO  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_detailed.bak' 
WITH  DIFFERENTIAL 
, NOFORMAT
, NOINIT
,  NAME = N'TEST_new-Полная База данных Резервное копирование'
, SKIP
, NOREWIND
, NOUNLOAD
,  STATS = 10
GO
 
--log
 
BACKUP LOG [TEST_new] 
TO  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_log.bak' 
WITH NOFORMAT
, NOINIT
,  NAME = N'TEST_new-Полная База данных Резервное копирование'
, SKIP
, NOREWIND
, NOUNLOAD
,  STATS = 10
GO
 
/*восстановление*/
 
USE [master]
BACKUP LOG [TEST_new] 
TO  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_LogBackup_2017-02-24_14-15-30.bak' 
WITH NOFORMAT
, NOINIT
,  NAME = N'TEST_new_LogBackup_2017-02-24_14-15-30'
, NOSKIP
, NOREWIND
, NOUNLOAD
,  NORECOVERY 
,  STATS = 5;
 
RESTORE DATABASE [TEST_new] 
FROM  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_full.bak' 
WITH  FILE = 1
,  NORECOVERY
,  NOUNLOAD
,  STATS = 5;
 
RESTORE DATABASE [TEST_new] 
FROM  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_detailed.bak' 
WITH  FILE = 1
,  NORECOVERY
,  NOUNLOAD
,  STATS = 5;
 
RESTORE LOG [TEST_new] 
FROM  DISK = N'C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\Backup\TEST_new_log.bak' 
WITH  FILE = 1
,  NOUNLOAD
,  STATS = 5;
 
GO
 
/*move*/
 
ALTER DATABASE BR SET OFFLINE;
 
ALTER DATABASE BR MODIFY FILE 
( 
NAME='BR'
, FILENAME='C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\DATA\BR.mdf' 
);
ALTER DATABASE BR MODIFY FILE 
( 
NAME='BR_log'
, FILENAME='C:\Program Files\Microsoft SQL Server 2008\MSSQL10_50.MSSQLSERVER2008\MSSQL\DATA\BR_log.ldf' 
);
 
ALTER DATABASE BR SET ONLINE;
