USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_OPTIMIZE]    Script Date: 23.06.2017 9:14:19 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[PRC_OPTIMIZE] @p_type BIT

--0 реорганизация
--1 перестроение

WITH RECOMPILE

AS

BEGIN

 

SET NOCOUNT ON;

DECLARE @v_type_optimize VARCHAR(10);
DECLARE @v_tbl_name VARCHAR(100);
DECLARE @v_sql VARCHAR(MAX);
DECLARE @v_db_name VARCHAR(50);
DECLARE @v_table TABLE(table_name VARCHAR(50));

IF @p_type=0

	SET @v_type_optimize='REORGANIZE';

ELSE

	SET @v_type_optimize='REBUILD';


DECLARE db_cur CURSOR LOCAL 
READ_ONLY 
FORWARD_ONLY 
FAST_FORWARD
FOR

SELECT name
FROM master.sys.databases
WHERE 1=1
AND database_id>4		--кроме системных баз
ORDER BY database_id;

OPEN db_cur;
FETCH NEXT FROM db_cur
INTO @v_db_name;

WHILE @@FETCH_STATUS=0

BEGIN

	SET @v_sql=
	(
	'SELECT name
	FROM ' + @v_db_name + '.sys.tables'
	);

	INSERT INTO @v_table
	EXECUTE msdb.dbo.sp_sqlexec @v_sql;

	DECLARE table_cur CURSOR LOCAL 
	READ_ONLY 
	FORWARD_ONLY 
	FAST_FORWARD
	FOR

	SELECT *
	FROM @v_table;

	OPEN table_cur
	FETCH FROM table_cur 
	INTO @v_tbl_name

	WHILE @@FETCH_STATUS=0

		BEGIN

		SET @v_sql=
			'BEGIN TRANSACTION'
			+ CHAR(13) +
			'	ALTER INDEX ALL ON ' + @v_db_name + '.dbo.' + @v_tbl_name + ' ' + @v_type_optimize + ';'
			+ CHAR(13) +
			'	UPDATE STATISTICS ON ' + @v_db_name + '.dbo.' + @v_tbl_name + ';'
			+ CHAR(13) +
			'COMMIT TRANSACTION;'
			;
		PRINT @v_sql;
		--EXEC master.dbo.sp_executesql @v_sql;

		FETCH FROM table_cur 
		INTO @v_tbl_name

		END;

	CLOSE table_cur
	DEALLOCATE table_cur;

	FETCH NEXT FROM db_cur
	INTO @v_db_name;

END

CLOSE db_cur;
DEALLOCATE db_cur;

END

