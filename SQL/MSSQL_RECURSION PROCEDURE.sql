USE [DEV]
GO
/****** Object:  StoredProcedure [dbo].[PRC_REC_TEST]    Script Date: 28.10.2016 17:49:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_REC_TEST] @i_out INT OUT, @i INT

 AS

BEGIN
	
	IF @i>31 RETURN; 
	
	PRINT 'in - i is ' + STR(@i) + ', i_out is ' + STR(ISNULL(@i_out, 0));
	SET @i+=1;
	SET @i_out=@i;
	PRINT 'out - i is ' + STR(@i) + ', i_out is ' + STR(ISNULL(@i_out, 0));
	EXEC DEV.dbo.PRC_REC_TEST @i_out, @i;

END

DECLARE @i_out INT;

EXEC DEV.dbo.PRC_REC_TEST @i_out OUT, 1;

PRINT @i_out;