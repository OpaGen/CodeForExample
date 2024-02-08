USE MK;

GO

SELECT d.DAY_ID
, s.row_count
, s.*
FROM sys.dm_db_partition_stats s

INNER JOIN SOURCES.dbo.V_DAYS d
ON ( $PARTITION.[MK_PRT_FNC](DAY_ID)=s.partition_number )

WHERE 1=1
AND s.object_id = OBJECT_ID( 'TD_T_RDM_5179_UN' )
AND s.row_count>0

ORDER BY d.DAY_ID;