SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (10 PERCENT);		--приблизительно

SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (1000 ROWS);		--приблизительно

SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (1000 ROWS) REPEATABLE (5);	--тот же набор возвращает при вызове