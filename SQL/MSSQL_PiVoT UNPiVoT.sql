SELECT *
FROM 

	(
	SELECT day_id 
	, day_id as day_id1
	FROM MONITOR.dbo.v_days
	WHERE day_id BETWEEN '2016-01-01' AND '2016-01-05' 
	) t

PIVOT

	(
	MAX(day_id)
	FOR day_id IN ([2016-01-01], [2016-01-02], [2016-01-03], [2016-01-04], [2016-01-05])
	) pvt;

SELECT day_id as day_id1
, MAX(CASE WHEN day_id='2016-01-01' THEN day_id END) as [2016-01-01]
, MAX(CASE WHEN day_id='2016-01-02' THEN day_id END) as [2016-01-02]
, MAX(CASE WHEN day_id='2016-01-03' THEN day_id END) as [2016-01-03]
, MAX(CASE WHEN day_id='2016-01-04' THEN day_id END) as [2016-01-04]
, MAX(CASE WHEN day_id='2016-01-05' THEN day_id END) as [2016-01-05]

FROM MONITOR.dbo.v_days

WHERE day_id BETWEEN '2016-01-01' AND '2016-01-05'
GROUP BY day_id;


SELECT *
FROM 

	(
	SELECT *
	FROM 

		(
		SELECT day_id 
		, day_id as day_id1
		FROM MONITOR.dbo.v_days
		WHERE day_id BETWEEN '2016-01-01' AND '2016-01-05' 
		) t

	PIVOT

		(
		MAX(day_id)
		FOR day_id IN ([2016-01-01], [2016-01-02], [2016-01-03], [2016-01-04], [2016-01-05])
		) pvt
	) t

UNPIVOT

	(
	day_id2
	FOR day_id IN ([2016-01-01], [2016-01-02], [2016-01-03], [2016-01-04], [2016-01-05])
	) unpvt