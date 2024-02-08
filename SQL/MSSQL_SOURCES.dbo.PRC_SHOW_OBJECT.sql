USE SOURCES;

GO

CREATE PROCEDURE dbo.PRC_SHOW_OBJECT

	(
	@p_object NVARCHAR(128)
	) 
	
WITH RECOMPILE

AS

BEGIN

SET NOCOUNT ON;

 

DECLARE @v_database NVARCHAR(32);
DECLARE @v_sql NVARCHAR(MAX);
DECLARE @v_sql_params NVARCHAR(128);
DECLARE @v_statement NVARCHAR(MAX);
DECLARE @v_object NVARCHAR(128);
DECLARE @v_object_name NVARCHAR(128);
DECLARE @v_object_schema NVARCHAR(32);
DECLARE @v_object_name_with_schema NVARCHAR(256);
DECLARE @v_sql_out NVARCHAR(MAX);

SET @v_object=@p_object;
SET @v_database=PARSENAME( @v_object, 3 );
SET @v_object_name=PARSENAME( @v_object, 1 );
SET @v_object_schema=PARSENAME( @v_object, 2 );
SET @v_statement=CONCAT( @v_database, N'.sys.sp_executesql' );
SET @v_object_name_with_schema=CONCAT( @v_object_schema, '.', @v_object_name );

SET @v_sql=	N'SELECT @v_sql_out=OBJECT_DEFINITION( OBJECT_ID( ' + QUOTENAME( @v_object_name_with_schema, '''' ) + ' ) )';

SET @v_sql_params=N'@v_sql_out NVARCHAR(MAX) OUT';
EXEC @v_statement @v_sql, @v_sql_params, @v_sql_out OUT;

SELECT @v_sql_out;

END;