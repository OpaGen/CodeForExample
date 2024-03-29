USE [SOURCES]
GO
/****** Object:  StoredProcedure [dbo].[PRC_DROP_TBL]    Script Date: 23.06.2017 9:13:59 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[PRC_DROP_TBL] @p_source_table NVARCHAR(128)
WITH RECOMPILE

AS

BEGIN

 

SET NOCOUNT ON;

DECLARE @v_sql NVARCHAR(MAX);
DECLARE @v_source_table NVARCHAR(128);

SET @v_source_table=@p_source_table;

IF OBJECT_ID( @p_source_table ) IS NOT NULL

	BEGIN

		SET @v_sql=CONCAT( N'DROP TABLE ', @v_source_table, N';' );

		BEGIN TRY

			BEGIN TRANSACTION

				EXEC master.dbo.sp_executesql @v_sql;

			COMMIT TRANSACTION;

		END TRY

		BEGIN CATCH

			IF @@TRANCOUNT>0 ROLLBACK TRANSACTION;
			THROW;

		END CATCH;

	END;

END