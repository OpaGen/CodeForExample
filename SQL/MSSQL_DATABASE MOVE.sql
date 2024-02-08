USE master;

GO

ALTER DATABASE ANALYTICS
MODIFY FILE ( name='ANALYTICS', filename='F:\MSSQL\DATA\ANALYTICS.mdf' );

GO

ALTER DATABASE ANALYTICS SET OFFLINE WITH ROLLBACK IMMEDIATE;

GO

ALTER DATABASE ANALYTICS SET ONLINE;