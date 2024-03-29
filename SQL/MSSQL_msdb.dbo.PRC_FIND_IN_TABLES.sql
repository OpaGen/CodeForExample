USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_FIND_IN_TABLES]    Script Date: 20.06.2017 9:12:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[PRC_FIND_IN_TABLES](@find varchar(4000)) 
with execute as owner
as

--exec [DEV].[don].[prc_FindInTables] ''

begin
	declare @db_name varchar(255)
		,@sql varchar(4000)
		;
	create table #t([db_name] varchar(255),table_name sysname,column_name sysname,column_type nvarchar(max))
	Declare db cursor for
	select
		'['+name+']'
	from master.sys.databases
	where name not in ('master','tempdb','model','msdb');
	
	open db;
	Fetch next from db into @db_name;
	
	while @@FETCH_STATUS=0
	begin
		set @sql='select 
					'''+@db_name+''' db_name
					,tb.name
					,c.name
					,tp.name
				from '+@db_name+'.sys.tables tb
				join '+@db_name+'.sys.columns c
					on tb.object_id=c.object_id
				join '+@db_name+'.sys.types tp
					on tp.user_type_id=c.user_type_id
						and tp.system_type_id=c.system_type_id
				where c.name like ''%'+@find+'%''';
		insert into #t([db_name],table_name,column_name,column_type)
		EXEC(@sql);
		Fetch next from db into @db_name;
	end
	
	close db;
	deallocate db;
	select * 
	from #t;
end