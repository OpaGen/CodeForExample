SELECT * FROM OPENROWSET('Microsoft.ACE.OLEDB.12.0',
'Excel 12.0;Database=K:\Path_on_MSSQL_SERVER\file.xlsx', [Лист1$]);