USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[PRC_FIND_IN_PROCS]    Script Date: 20.06.2017 9:12:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





ALTER procedure [dbo].[PRC_FIND_IN_PROCS](@find varchar(4000))
with execute as owner
as

--EXEC [DEV].[don].[prc_FindInProcs] 'WHS_SHED'

begin
    declare @db_name varchar(255)
        ,@sql varchar(4000)
        ;
    create table #t([db_name] varchar(255),shema_name sysname,proc_name sysname,definit nvarchar(max))
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
                    ,sh.name
                    ,o.name
                    , m.definition
                from '+@db_name+'.sys.sql_modules m
                join '+@db_name+'.sys.objects o
                    on o.object_id=m.object_id
                join '+@db_name+'.sys.schemas sh
                    on sh.schema_id=o.schema_id
                where definition like ''%'+@find+'%''';
        insert into #t([db_name],shema_name,proc_name,definit)
        exec(@sql);
        Fetch next from db into @db_name;
    end

    close db;
    deallocate db;
    select * 
	from #t;
end 

