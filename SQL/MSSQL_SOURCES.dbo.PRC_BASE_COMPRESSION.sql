USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_BASE_COMPRESSION]    Script Date: 23.06.2017 9:13:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_BASE_COMPRESSION] @type BIT

--1 лог базы
--0 файл базы

WITH RECOMPILE

AS

BEGIN

 

SET NOCOUNT ON;

BEGIN TRY

	DECLARE	@v_db_name NVARCHAR(50);
	DECLARE @v_file_name NVARCHAR(256);
	DECLARE @v_sql NVARCHAR(MAX);
	DECLARE @v_statement NVARCHAR(MAX);
	DECLARE @v_sql_params NVARCHAR(512)=N'@v_file_name NVARCHAR(256) OUT';
		
	DECLARE db_cur CURSOR 
	LOCAL 
	READ_ONLY 
	FORWARD_ONLY 
	FAST_FORWARD 
	FOR

	SELECT name
	FROM master.dbo.sysdatabases;
	
	OPEN db_cur
	FETCH NEXT FROM db_cur 
	INTO @v_db_name;

	WHILE @@FETCH_STATUS = 0 

		BEGIN
		
			SET @v_statement=QUOTENAME( @v_db_name, '' ) + '.sys.sp_executesql';	--запуск в новой базе
			--добавил перебор всех файлов после создания секционированной таблицы и распралелливания записи в tempdb (динамичеки получается использовать тольк глобальный курсор)
			SET @v_sql=
			'DECLARE fl_cur CURSOR GLOBAL 
			READ_ONLY 
			FORWARD_ONLY 
			FAST_FORWARD
			FOR
			SELECT name
			FROM ' + @v_db_name + '.sys.database_files' +
			' WHERE type=' + STR( @type ) + 
			' ORDER BY name;';

			--PRINT @v_sql;
			--EXEC master.dbo.sp_executesql @v_sql, @v_sql_params, @v_file_name OUT;
			EXEC master.dbo.sp_executesql @v_sql;

			OPEN fl_cur;
			FETCH NEXT FROM fl_cur
			INTO @v_file_name;

			WHILE @@FETCH_STATUS=0

				BEGIN

					--PRINT @v_file_name;
					SET @v_sql='DBCC SHRINKFILE ( ' + QUOTENAME( @v_file_name, '' ) + ' , 1, TRUNCATEONLY ) WITH NO_INFOMSGS;';

					--PRINT @v_sql;

					--BEGIN TRANSACTION;

						EXEC @v_statement @v_sql;

					--COMMIT TRANSACTION;

				FETCH NEXT FROM fl_cur
				INTO @v_file_name;

				END;
		
			CLOSE fl_cur;
			DEALLOCATE fl_cur;

			--дополнительно сжимаем всю БД, если нужно
			IF @type=0

				BEGIN

					SET @v_sql='DBCC SHRINKDATABASE(' + @v_db_name + ');';
					EXEC @v_statement @v_sql;

				END;

		FETCH NEXT FROM db_cur 
		INTO @v_db_name;

		END;

	CLOSE db_cur;
	DEALLOCATE db_cur;

END TRY

BEGIN CATCH
	
	EXECUTE SOURCES.dbo.PRC_ERROR_INFO;
	THROW;

END CATCH;

END