USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_EXECUTE_SSIS_PACKAGE]    Script Date: 20.06.2017 9:12:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_EXECUTE_SSIS_PACKAGE]

@ssis_package VARCHAR(255), @bit_parametr TINYINT=64

WITH EXECUTE AS OWNER

AS

 
BEGIN

DECLARE @cmd VARCHAR(255);


IF @bit_parametr=64

SET @cmd='dtexec /sql "'+ @ssis_package +'"';

ELSE

SET @cmd='"C:\Program Files (x86)\Microsoft SQL Server\100\DTS\Binn\DTExec.exe" /sql '+ @ssis_package +'';

EXEC xp_cmdshell @cmd;

END;