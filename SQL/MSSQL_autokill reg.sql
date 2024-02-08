USE [MERCH]
GO

/****** Object:  StoredProcedure [dbo].[PRC_AUTOKILL]    Script Date: 08.02.2019 9:05:07 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:		<OLIFIR_PG>
-- Create date: <11.12.2018>
-- Description:	<Автоотстрел с сервака>
-- =============================================
CREATE PROCEDURE [dbo].[PRC_AUTOKILL]
AS
BEGIN
	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SET NOCOUNT ON;
	
-- MERCH.DBO.T_AUTOKILL_EXCLUSION таблица с исключениями

if OBJECT_ID('tempdb..#sleep_spid') is not null drop table #sleep_spid ; 

-- ЗАВИСЛО С ПРОШЛЫХ СУТОК (последние 24 часа)
SELECT 
	s.loginame
,	[last_batch]
,	s.SPID 
,	( select sql_handle from sys.sysprocesses where spid= s.SPID) as sql_now_moment
into #sleep_spid
from msdb.sys.sysprocesses as m
join (select distinct loginame, spid from msdb.sys.sysprocesses  where loginame <> '') as s -- для пустых значений spid
	on m.spid = s.spid
where dbid>4
and [last_batch]<dateadd(day,-1,getdate())
and s.loginame <> 'sa'
GROUP BY  s.loginame,[last_batch], s.SPID


if OBJECT_ID('tempdb..#cpu_spid') is not null drop table #cpu_spid ; 
select  --Те кто уже 10 минут потребляют большое кол-во CPU
stage1.loginame
, SPID 
,	( select sql_handle from sys.sysprocesses where spid= stage1.SPID) as sql_now_moment
into #cpu_spid
from (			
		SELECT 
			s.loginame
			, hostname
			, m.spid
			, db_name(dbid) as db
			, lastwaittype
			, CAST(cpu AS REAL) AS cpu
			, physical_io
			, last_batch
			, status
			, cmd
			, blocked
			, waittime
		FROM msdb.sys.sysprocesses as m
		join (select distinct loginame, spid from msdb.sys.sysprocesses  where loginame <> '') as s -- для пустых значений spid
			on m.spid = s.spid
		WHERE ( status='suspended' or status='runnable' or status='running' )
		 and cpu >= 199483647 
		and last_batch <dateadd(MI,-60,getdate())
	) as stage1
--left join MERCH.DBO.T_AUTOKILL_EXCLUSION as E
--	on E.loginame = stage1.loginame
where 1=1
	--  and e.loginame is null
GROUP BY  stage1.loginame, SPID
 

--определяем наглых по дельте
if OBJECT_ID('tempdb..#deltaCPU_spid') is not null drop table #deltaCPU_spid ; 
 SELECT
	STAGE2.loginame
 ,	spid
 ,	(cpu / SUM_CPU) as prc_cpu
  ,	(physical_io / SUM_HDD) as prc_HDD
 ,	( select sql_handle from sys.sysprocesses where spid= stage2.spid) as sql_now_moment
into #deltaCPU_spid
 FROM (
		 SELECT
			LOGINAME
		 ,	SPID
		 ,	SUM(cpu) AS CPU
		 ,	SUM(physical_io) AS physical_io
		 ,	SUM(CPU)  OVER ( PARTITION BY 1) AS SUM_CPU
		 ,	SUM(physical_io)  OVER (PARTITION BY 1 ) AS SUM_HDD
		 FROM (
				SELECT s.loginame
			   , hostname
			   , m.spid
			   , db_name(dbid) as db
			   , lastwaittype
			   , CAST(cpu AS REAL) AS cpu
			   , physical_io
			   , last_batch
			   , status
			   , cmd
			   , blocked
			   , waittime
			   FROM msdb.sys.sysprocesses as m
				join (select distinct loginame, spid from msdb.sys.sysprocesses  where loginame <> '') as s -- для пустых значений spid
					on m.spid = s.spid
			   WHERE ( status='suspended' or status='runnable' or status='running' )
			 ) AS STAGE1
		GROUP BY
			LOGINAME
		 ,	SPID
		 ,	CPU
		 ,	physical_io
	  ) AS STAGE2
--left join  MERCH.DBO.T_AUTOKILL_EXCLUSION as E
--	on E.loginame = stage2.loginame
where 1=1
	  --and e.loginame is null
	  and ( (SUM_CPU > 2013188*1.2) OR (SUM_HDD > 3332561 * 1.2) ) -- Выше средней нагрузки на сервер
	  and ((cpu / SUM_CPU) > 0.3 or (physical_io / SUM_HDD) > 0.5); -- потребляют больше 30% от всей нагрузки


--Формируем финальную таблицу
if OBJECT_ID('tempdb..#Final') is not null drop table #Final ; 

select 
	stage2.*
,	concat ( SUBSTRING (loginame, CHARINDEX('\',loginame) +1, charindex(' ',loginame)-6), '@magnit.ru') as email
,	(select text from sys.dm_exec_sql_text( sql_now_moment )) as sql_sample
,		ROW_NUMBER() over (partition by 1 order by loginame, SPID, prc) as RN 
into #Final
from (
			select
				stage1.*
			,	ROW_NUMBER() over (partition by loginame, SPID order by loginame, SPID, prc) as RN_Unic	
			
			from (
					select
					loginame, SPID ,1 as sql_id, 'Спит с прошлых суток' as prc , sql_now_moment
					from #sleep_spid
					union all
					select
					loginame, SPID ,2 as sql_id, 'Использование CPU больше часа' as prc, sql_now_moment
					from #cpu_spid
					union all
					select
					loginame, SPID ,3 as sql_id, 'Высокая нагрузка на сервер' as prc, sql_now_moment
					from #deltaCPU_spid
				) as stage1
	  ) AS STAGE2
WHERE RN_UNIC = 1 and (select text from sys.dm_exec_sql_text( sql_now_moment )) is not null 
 --Цикл для письма, потом заменим на расстрел

--DROP TABLE MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY ;
--CREATE TABLE MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY 
--(
--	DAY_ID DATETIME NOT NULL
--,	SPID INT NOT NULL
--,	USER_LOGIN NVARCHAR(MAX) NULL
--,	SQL_QUERY NVARCHAR(MAX) NULL
--,	SQL_QUERY_ID INT NOT NULL IDENTITY (1,1)
--)	 
--ALTER TABLE MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY  DROP COLUMN nvDAY_ID  
--ALTER TABLE MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY  ADD HOUR_ID INT NULL
--ALTER TABLE MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY  ADD HOUR_ID INT NULL
 
 select *
 ,	STRINGAGG
 from #Final as f
where f.loginame = 'corp\olifir_pg'

 IF (SELECT COUNT(1) AS CNT FROM #Final) > 0
 BEGIN
 		 DECLARE @RN INT;
		 SET @RN = 1;
		 DECLARE @SQL_SAMPLE NVARCHAR(MAX);
		 DECLARE @email NVARCHAR(MAX);
		 declare @sql varbinary(max);
		 declare @sql_text  NVARCHAR(MAX);
		 SET @SQL_SAMPLE = '<html> <body>' + 'Тяжелые или долгие запросы запушенные на сервере Hyper-prod2 под вашей УЗ </p>';
			
			 WHILE 1=1
					 BEGIN
						--КИДАЕМ ГРЕШНИКОВ В ИСТОРИЮ
						--set @sql =  (SELECT sql_now_moment FROM #Final WHERE RN = @RN);
						set @sql_text = (select text from sys.dm_exec_sql_text( (SELECT sql_now_moment FROM #Final WHERE RN = @RN) ));

			
						INSERT INTO MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY --select * from  MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY
						SELECT
							GETDATE() AS DAY_iD
						,	SPID
						,	LOGINAME AS USER_LOGIN
						,	@sql_text as sql_query
						,	sql_id
						,	prc as sql_name
						FROM #Final
						WHERE RN = @RN;
						--select * from #final

						--ФОРМИРУЕМ ПИСЬМО
						--SET @SQL_SAMPLE = @SQL_SAMPLE  + coalesce((SELECT CONCAT('',replace(LOGINAME,' ',''),' ',prc,' SPID = ', SPID ,'</p>',' :: Запрос SELECT SQL_QUERY FROM MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY WHERE SQL_QUERY_ID = ',CAST ( (SELECT MAX(SQL_QUERY_ID) AS SQLQID FROM MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY) AS NVARCHAR(MAX)) ,'</p>','</p>','***','</p>' )   FROM #Final WHERE RN = @RN),' ') ;
						SET @SQL_SAMPLE = @SQL_SAMPLE  + coalesce((SELECT CONCAT('',replace(LOGINAME,' ',''),' ',prc,' SPID = ', SPID ,'</p>',' :: Запрос SELECT SQL_QUERY FROM MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY WHERE SQL_QUERY_ID = ',CAST ( (SELECT MAX(SQL_QUERY_ID) AS SQLQID FROM MERCH.DBO.T_SQL_QUERY_AUTOKILL_HISTORY) AS NVARCHAR(MAX)) ,'</p>','</p>','***','</p>' )   FROM #Final WHERE RN = @RN),' ') ;
												
						DELETE FROM #Final WHERE RN = @RN;
						IF @@ROWCOUNT=0 BREAK;
						SET @RN = @RN + 1;
					 END

 

--SET @SQL_SAMPLE = @SQL_SAMPLE  + ' </body></html>';
--EXEC MSDB.DBO.SP_SEND_DBMAIL 
--@PROFILE_NAME='LOGISTSEND',
----@RECIPIENTS='olifir_pg@magnit.ru' ,
--@RECIPIENTS='tkachenko_av@magnit.ru;dudliy_na@magnit.ru;ermolich_da@magnit.ru;olifir_pg@magnit.ru; ekaterinichev_ee@magnit.ru; koltanyuk_vv@magnit.ru' ,
--@SUBJECT='Подозрительные запросы на сервере',
--@BODY=  @SQL_SAMPLE,
--@BODY_FORMAT='HTML';
 END;

 if OBJECT_ID('tempdb..#deltaCPU_spid') is not null drop table #deltaCPU_spid ; 
 if OBJECT_ID('tempdb..#cpu_spid') is not null drop table #cpu_spid ; 
 if OBJECT_ID('tempdb..#sleep_spid') is not null drop table #sleep_spid ; 
 if OBJECT_ID('tempdb..#Final') is not null drop table #Final ;

END

GO
 