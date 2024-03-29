USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_PRINT_QUERY]    Script Date: 20.06.2017 9:12:19 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[PRC_PRINT_QUERY] @SPID int
WITH EXECUTE AS OWNER
 
AS
DECLARE @sql_handle BINARY(20)
, @stmt_start INT
, @stmt_end INT;

SELECT @sql_handle = sql_handle
, @stmt_start = stmt_start/2
, @stmt_end = CASE WHEN stmt_end = -1 THEN -1 ELSE stmt_end/2 END
FROM master.dbo.sysprocesses
WHERE spid = @SPID AND ecid = 0;

DECLARE @line NVARCHAR(4000);

SET @line = (SELECT SUBSTRING([text], COALESCE(NULLIF(@stmt_start, 0), 1)
, CASE @stmt_end 
	WHEN -1 
	THEN DATALENGTH([text]) 
	ELSE (@stmt_end - @stmt_start) END) 
FROM ::fn_get_sql(@sql_handle));

PRINT @line;