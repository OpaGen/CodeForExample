SELECT BRANCH_ID
, WHS_ID
, LAG( WHS_ID, 1, -1 ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) LAG
, LEAD( WHS_ID, 1, -1 ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) LEAD
, FIRST_VALUE( WHS_ID ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) FIRST_VALUE
, LAST_VALUE( WHS_ID ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) LAST_VALUE	--иначе вернет предыдущее значение
, CUME_DIST() OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) as CUME_DIST	--положение в выборке, как отношение всех строк в окне со значением, меньше текущего (по сортировке) ко всем строкам окна
, PERCENT_RANK() OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) as PERCENT_RANK	--похожа на функцию выше
, PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY WHS_ID ) OVER ( PARTITION BY BRANCH_ID ) as PERCENTILE_CONT	--возвращает значение из процентиля (аргумент функции), котороые больше или равно по данным функции CUME_DIST с таким же окном и сортировкой
, PERCENTILE_DISC(0.5) WITHIN GROUP ( ORDER BY WHS_ID ) OVER ( PARTITION BY BRANCH_ID ) as PERCENTILE_DISC	--аналог функции выше, но в случае четного количества элементов в выборке, возьмет среднее между элементом от аргумента CUBE_DIST и следующим элементом, как и происходит в медиане

FROM SOURCES.dbo.V_WHS
WHERE 1=1
AND BRANCH_ID<>-1
ORDER BY BRANCH_ID
, WHS_ID;