USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_CURRENT_QUERY]    Script Date: 20.06.2017 9:12:11 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_CURRENT_QUERY] 
WITH EXECUTE AS OWNER
AS
BEGIN

	SELECT loginame
	, hostname
	, spid
	, db_name(dbid) as db
	, lastwaittype
	, cpu
	, physical_io
	, last_batch
	, status
	, cmd
	, blocked
	, waittime
	FROM msdb.sys.sysprocesses
	WHERE (status='suspended' or status='runnable' or status='running' )
	ORDER BY spid; 

END
