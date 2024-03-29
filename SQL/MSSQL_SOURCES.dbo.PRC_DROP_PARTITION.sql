USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_DROP_PARTITION]    Script Date: 18.03.2019 9:42:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_DROP_PARTITION]

	(
	@p_source_table NVARCHAR(128)
	, @p_id SQL_VARIANT
	, @p_online BIT=1
	)

WITH RECOMPILE

AS

BEGIN

SET NOCOUNT ON;

 

DECLARE @v_id SQL_VARIANT;
DECLARE @v_prt_nmb INT;
DECLARE @v_database NVARCHAR(32);
DECLARE @v_sql NVARCHAR(MAX);
DECLARE @v_sql_params NVARCHAR(128);
DECLARE @v_statement NVARCHAR(MAX);
DECLARE @v_source_table NVARCHAR(128);
DECLARE @v_source_table_name NVARCHAR(128);
DECLARE @v_source_schema NVARCHAR(32);
DECLARE @v_partition_function NVARCHAR(128);
DECLARE @v_temp_table NVARCHAR(128);
DECLARE @v_temp_table_name NVARCHAR(128);
DECLARE @v_online BIT;
DECLARE @v_online_text NVARCHAR(4);
DECLARE @v_pk_name NVARCHAR(256);

SET @v_id=@p_id;
SET @v_source_table=@p_source_table;
SET @v_database=PARSENAME( @v_source_table, 3 );
SET @v_source_table_name=PARSENAME( @v_source_table, 1 );
SET @v_source_schema=PARSENAME( @v_source_table, 2 );
SET @v_statement=CONCAT( @v_database, N'.sys.sp_executesql' );
SET @v_temp_table=CONCAT( @v_source_table, N'_TEMP' );
SET @v_temp_table_name=CONCAT( @v_source_table_name, N'_TEMP' );
--получение функции секционирования
EXEC SOURCES.dbo.PRC_GET_PRN_FNC @v_source_table, @v_partition_function OUT;
SET @v_sql=CONCAT( 'SELECT @v_prt_nmb=$PARTITION.', @v_partition_function, '( ', QUOTENAME( CAST( @v_id as NVARCHAR(50) ), '''' ), ' )' );
SET @v_sql_params=N'@v_prt_nmb INT OUT';
EXEC @v_statement @v_sql, @v_sql_params, @v_prt_nmb OUT;
SET @v_online=@p_online;
SET @v_online_text=IIF( @p_online=1, N'ON', N'OFF' );

--------------------------------------------------------------- выход из процедуры ----------------------------------------------------------------

IF @v_prt_nmb IS NULL

	BEGIN

		RAISERROR( 'Секции для этого идентификатора не существует в данной схеме секционирования', 15, 0 );
		RETURN;

	END;

----------------------------------------------------- создаем темповую таблицу такого же вида -----------------------------------------------------

BEGIN TRY

	BEGIN TRANSACTION

		EXEC SOURCES.dbo.PRC_DROP_TBL @v_temp_table;		--удаляем на всякий случай
		EXEC SOURCES.dbo.PRC_CRT_TMP_TBL @v_source_table;	--создаем темповую
	COMMIT TRANSACTION;

END TRY

BEGIN CATCH

	IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;		--в верхней транзакции для каждой процедуры уже есть откат
	THROW;

END CATCH;

--------------------------------------------- секционируем таблицу и убираем индексы, чтобы свитчнуть секции ---------------------------------------

BEGIN TRY

	BEGIN TRANSACTION
		
		DECLARE @v_partition_column NVARCHAR(64);
		EXEC SOURCES.dbo.PRC_GET_PRT_CLMN @v_source_table, @v_partition_column OUT;									--получаем столбец секционирования
		--EXEC SOURCES.dbo.PRC_DROP_CNSTR_AND_INDX @v_temp_table, 0, @v_online;										--на всякий случай удаляем все индексы
		EXEC SOURCES.dbo.PRC_CRT_CNSTR_AND_INDX @v_source_table, @v_temp_table, @v_partition_column, 1				--создаем только первичный ключ для открепления (другие индексы не нужны)

		------------------------------------------------------ получаем название первичного ключа -----------------------------------------------------------

		SET @v_sql_params=N'@v_pk_name NVARCHAR(256) OUT';

		SET @v_sql=
			N'
			SELECT @v_pk_name=i.name
			FROM sys.indexes i
			WHERE 1=1
			AND i.[type]=1

			AND i.[object_id] = OBJECT_ID( ' + QUOTENAME( CONCAT( @v_source_schema, '.', @v_temp_table_name ), '''' ) + N' )
			ORDER BY index_id;
			';

		EXEC @v_statement @v_sql, @v_sql_params, @v_pk_name OUT;

		SET @v_sql=
			'
			ALTER INDEX ' + @v_pk_name + ' ON ' + @v_temp_table + '		--сжатие секций по индексам
			REBUILD PARTITION=ALL WITH( DATA_COMPRESSION=PAGE, ONLINE=' + @v_online_text + ');
			';

		EXEC @v_statement @v_sql;																					--сжимаем секции (таблицу сжимать не нужно)

	COMMIT TRANSACTION;

END TRY

BEGIN CATCH

	IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;		--в верхней транзакции для каждой процедуры уже есть откат
	THROW;

END CATCH;

------------------------------------------------------------------- свитчим секции -----------------------------------------------------------------

SET @v_sql=
'
BEGIN TRY

	BEGIN TRANSACTION

		ALTER TABLE ' + @v_source_table  + '
		SWITCH PARTITION ' + CONCAT( STR( @v_prt_nmb ), ' TO ', @v_temp_table, ' PARTITION ', STR( @v_prt_nmb ) ) + ';

	COMMIT TRANSACTION;

END TRY

BEGIN CATCH

	IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;
	THROW;

END CATCH;
';

--PRINT @v_sql;

EXEC @v_statement @v_sql;

------------------------------------------------------------------ удаляем таблицу -----------------------------------------------------------------

BEGIN TRY

	BEGIN TRANSACTION

		EXEC SOURCES.dbo.PRC_DROP_TBL @v_temp_table;		--удаляем на всякий случай

	COMMIT TRANSACTION;

END TRY

BEGIN CATCH

	IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;		--в верхней транзакции для каждой процедуры уже есть откат
	THROW;

END CATCH;

END