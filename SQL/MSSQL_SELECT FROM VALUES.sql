SELECT name
, value
FROM 
	(
	VALUES ('speed', 1)
	, ('ram', 1)
	, ('hd', 1)
	, ('screen', 1)
	) s 
		
(name, value);