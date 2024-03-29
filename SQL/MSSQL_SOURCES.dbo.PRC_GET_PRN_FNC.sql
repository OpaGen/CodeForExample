USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_GET_PRN_FNC]    Script Date: 18.03.2019 9:42:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_GET_PRN_FNC] 	

	(
	@p_source_table NVARCHAR(128)
	, @p_partition_function NVARCHAR(128) OUT
	) 
	
AS

BEGIN

SET NOCOUNT ON;

 

DECLARE @v_database NVARCHAR(32);
DECLARE @v_sql NVARCHAR(MAX);
DECLARE @v_sql_params NVARCHAR(128);
DECLARE @v_statement NVARCHAR(MAX);
DECLARE @v_source_table NVARCHAR(128);
DECLARE @v_source_table_name NVARCHAR(128);
DECLARE @v_source_schema NVARCHAR(32);
DECLARE @v_partition_function NVARCHAR(128);

SET @v_source_table=@p_source_table;
SET @v_database=PARSENAME( @v_source_table, 3 );
SET @v_source_table_name=PARSENAME( @v_source_table, 1 );
SET @v_source_schema=PARSENAME( @v_source_table, 2 );
SET @v_statement=CONCAT( @v_database, N'.sys.sp_executesql' );

------------------------------------------------------- получение функции секционирования ---------------------------------------------------------

SET @v_sql=
	N'
	SELECT @v_partition_function=pfs.name

	FROM sys.tables tbl

	INNER JOIN sys.indexes idx 
	ON idx.[object_id] = tbl.[object_id]

	INNER JOIN sys.data_spaces dts 
	ON dts.data_space_id = idx.data_space_id

	LEFT OUTER JOIN sys.partition_schemes prs 
	ON prs.data_space_id = dts.data_space_id

	LEFT OUTER JOIN sys.partition_functions pfs 
	ON pfs.function_id = prs.function_id

	WHERE tbl.object_id=OBJECT_ID( ' + QUOTENAME( CONCAT( @v_source_schema, '.', @v_source_table_name ), '''' ) + N' );
	';

SET @v_sql_params=N'@v_partition_function NVARCHAR(128) OUT';
EXEC @v_statement @v_sql, @v_sql_params, @v_partition_function OUT;

-------------------------------------------------------------- выход из процедуры -----------------------------------------------------------------

IF @v_partition_function='' OR @v_partition_function IS NULL

	BEGIN

		RAISERROR( 'Нет функции секционирования на таблице', 15, 0 );
		RETURN;

	END;

------------------------------------------------------------- назначение параметра ----------------------------------------------------------------

--PRINT @v_partition_function;
SET @p_partition_function=@v_partition_function;

END;