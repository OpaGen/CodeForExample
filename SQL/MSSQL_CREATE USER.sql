USE master;

GO

CREATE LOGIN [CORP\polishchuk_rv] FROM WINDOWS WITH DEFAULT_DATABASE=Analitika;

USE Analitika;

GO

CREATE USER [CORP\polishchuk_rv] FOR LOGIN [CORP\polishchuk_rv];
EXEC sp_addrolemember 'db_datareader', [CORP\polishchuk_rv];

USE TEST;

GO

CREATE USER [CORP\polishchuk_rv] FOR LOGIN [CORP\polishchuk_rv];
EXEC sp_addrolemember 'db_datareader', [CORP\polishchuk_rv];