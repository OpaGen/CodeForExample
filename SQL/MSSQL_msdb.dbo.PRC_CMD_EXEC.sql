USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_CMD_EXEC]    Script Date: 20.06.2017 9:12:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_CMD_EXEC] @p_cmd NVARCHAR(1024)
WITH EXECUTE AS OWNER

AS
 
BEGIN

SET NOCOUNT ON;

DECLARE @v_cmd NVARCHAR(1024);
SET @v_cmd=@p_cmd;

EXEC master.dbo.xp_cmdshell @v_cmd;

END;