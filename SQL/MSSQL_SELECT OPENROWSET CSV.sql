BULK
INSERT Analitika.dbo.rc_square
FROM 'C://Path_on_sql_server/file.csv'
WITH
(FIELDTERMINATOR = ';',
ROWTERMINATOR = '\n');