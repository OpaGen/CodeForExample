COLLECT STATISTICS
COLUMN (index columns),
COLUMN (index columns)) ON table;
COMMIT;

COLLECT STATISTICS 
--USING NO THRESHOLD		не обрщает внимания на пороги, которые указаны в системных таблицах (необходимое изменение данных для пересчета)
--USING THRESHOLD 10 PERCENT AND 30 DAYS		--использовать порог в 10% изменения данных или 30 дней после последнего сбора
--FOR CURRENT	--не запоминать подсказски для следующего сбора статистики
COLUMN ( ACT_TYPE_ID , MKTNG_ACTN_TMPLTS_ID ) 
, COLUMN ( ART_ID ) 
, COLUMN ( ACT_TYPE_ID ) 
, COLUMN ( WHS_ID ) 
, COLUMN ( MKTNG_ACTN_TMPLTS_ID ) 
, COLUMN ( PARTITION ) 
ON PRD_DB_DM.D_AUM_WHS_ART_WEEK;