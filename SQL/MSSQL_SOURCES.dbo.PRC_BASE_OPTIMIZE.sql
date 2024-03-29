USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_CLEAR_CACHE]    Script Date: 01.03.2017 8:58:58 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_CLEAR_CACHE] AS

BEGIN

 

SET NOCOUNT ON;

DECLARE @v_database_id SMALLINT;
DECLARE @v_sql NVARCHAR(MAX);

DECLARE cur CURSOR LOCAL 
READ_ONLY 
FORWARD_ONLY 
FAST_FORWARD
FOR

SELECT database_id
FROM master.sys.databases
WHERE 1=1
AND database_id>4		--кроме системных баз
ORDER BY database_id;

OPEN cur;
FETCH NEXT FROM cur
INTO @v_database_id;

WHILE @@FETCH_STATUS=0

	BEGIN
		
		SET @v_sql=CONCAT( N'DBCC FLUSHPROCINDB(', STR( @v_database_id ), ');');
		EXEC master.dbo.sp_executesql @v_sql;
		--PRINT @v_sql;

		FETCH NEXT FROM cur
		INTO @v_database_id;

	END;

CLOSE cur;
DEALLOCATE cur;

END;