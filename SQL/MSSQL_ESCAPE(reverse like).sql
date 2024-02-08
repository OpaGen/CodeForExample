SELECT ssis.name
, CONVERT( VARCHAR(MAX), CONVERT( VARBINARY(MAX), ssis.packagedata ) ) 
FROM msdb.dbo.sysssispackages AS ssis
WHERE 1=1
AND CONVERT( VARCHAR(MAX), CONVERT( VARBINARY(MAX), ssis.packagedata ) ) 
LIKE CONCAT( '%', 'ab!%', '%' ) ESCAPE '!'	--исключит ! и учтет %, как строчное значение