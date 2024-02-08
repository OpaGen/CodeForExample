EXEC sp_configure 'max degree of parallelism';

SELECT value_in_use 
  FROM sys.configurations 
  WHERE name = 'max degree of parallelism';
  
GO
  
  sp_configure 'max degree of parallelism', 0;
  
GO

RECONFIGURE WITH OVERRIDE;