USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_FIND_IN_JOBS]    Script Date: 20.06.2017 9:12:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER procedure [dbo].[PRC_FIND_IN_JOBS]
(@command varchar(4000))
with execute as owner
as

--EXEC [don].[prc_FindInJobs] 'PRC_IncomesExportTD'

begin
	select
		j.name job_name
		,js.step_id
		,js.step_name
		,js.command
	from msdb..sysjobs j
	join msdb..sysjobsteps js
		on js.job_id=j.job_id
	where js.command like '%'+@command+'%';
end