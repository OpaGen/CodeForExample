USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_BASE_SIZE_MS]    Script Date: 28.02.2017 10:58:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_BASE_SIZE_MS] AS

BEGIN

 

SET NOCOUNT ON;

DECLARE @db_table TABLE 
(
db VARCHAR(50)
, all_space REAL
, used_space REAL
);
DECLARE @v_db VARCHAR(50)
DECLARE @v_dbsize REAL;
DECLARE @v_logsize REAl;
DECLARE @v_maxsize REAl;
DECLARE @v_sql NVARCHAR(MAX);
DECLARE @v_sql_params NVARCHAR(100)=N'@v_dbsize REAL OUT, @v_logsize REAL OUT, @v_maxsize REAL OUT';

DECLARE db_cur CURSOR LOCAL 
READ_ONLY 
FORWARD_ONLY
FAST_FORWARD 
FOR

SELECT name as db
FROM msdb.sys.databases
WHERE 1=1
AND database_id>4
AND name<>'SSISDB'
ORDER BY name;

OPEN db_cur;

FETCH NEXT
FROM db_cur
INTO @v_db;

WHILE @@FETCH_STATUS=0

	BEGIN
		
		--DECLARE @xx REAL;

		SET @v_sql=
		'SELECT @v_dbsize = SUM(IIF(status & 64 = 0, size, 0))
		, @v_logsize = SUM(IIF(status & 64 <> 0, size, 0))
		, @v_maxsize = SUM(maxsize)
		FROM ' + @v_db + '.dbo.sysfiles;';

		--PRINT @v_sql;

		EXEC master.dbo.sp_executesql @v_sql, @v_sql_params, @v_dbsize OUT, @v_logsize OUT, @v_maxsize OUT;

		/*
		SELECT @v_dbsize
		, @v_logsize
		, @v_maxsize;
		*/

		SET @v_sql=
		'SELECT ' + QUOTENAME(@v_db, '''') + ' as db
		, CONVERT(REAL, ' + STR(@v_maxsize * 8192 / POWER(1024.0, 3)) + ') as all_space
		, LTRIM((CONVERT(REAL, ' + STR(@v_dbsize - @v_logsize) + ')) * 8192 / POWER(1024.0, 3)) as used_space';

		--PRINT @v_sql;

		INSERT INTO @db_table

		(
		db
		, all_space
		, used_space
		)

		EXEC master.dbo.sp_sqlexec @v_sql;

		FETCH NEXT
		FROM db_cur
		INTO @v_db;

	END;

CLOSE db_cur;
DEALLOCATE db_cur;

SELECT *
FROM @db_table
WHERE used_space/all_space > 0.8;

END