DECLARE @c int, @f int, @g int;

SET @c=1;
SET @f=2; 
SET @g=3;

IF @c=1 GOTO c;
IF @f=2 GOTO f;
IF @g=3 GOTO g;

c: PRINT '1'
f: PRINT '2'
g: PRINT '3'