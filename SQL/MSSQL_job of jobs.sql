-- Список джобов для запуска
DECLARE ListJobs CURSOR FOR
    SELECT JOB_NAME, FROM_STEP FROM AS_ET_ListJobs
    ORDER BY NUM;
   
-- @JobName - Название текущего джоба которого нужно запустить
DECLARE @JobName VARCHAR(50), @StepName VARCHAR(255), @SQL VARCHAR(255);

OPEN ListJobs;
FETCH NEXT FROM ListJobs INTO @JobName, @StepName;            -- Получаем имя текущего джоба
WHILE @@FETCH_STATUS = 0 BEGIN
    UPDATE  AS_ET_ListJobs
    SET IS_READY = 0
    WHERE JOB_NAME = @JobName;
    ---------------------------------------------------------------------------
    IF @StepName IS NULL
        EXEC msdb.dbo.sp_start_job @JobName;        -- запускаем джоб
    ELSE
        EXEC msdb.dbo.sp_start_job @JobName, @step_name = @StepName;    -- запускаем джоб
    ---------------------------------------------------------------------------
    -- ждем его окончания:
    WHILE (SELECT IS_READY FROM AS_ET_ListJobs WHERE JOB_NAME = @JobName) = 0
        WAITFOR DELAY '00:01:00';
    ---------------------------------------------------------------------------
    FETCH NEXT FROM ListJobs INTO @JobName, @StepName;            -- Получаем имя текущего джоба
END;
CLOSE ListJobs;
DEALLOCATE ListJobs;