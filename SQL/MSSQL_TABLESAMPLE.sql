SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (10 PERCENT);		--��������������

SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (1000 ROWS);		--��������������

SELECT *
FROM Sourses_out.dbo.TD_V_ART_EXT TABLESAMPLE (1000 ROWS) REPEATABLE (5);	--��� �� ����� ���������� ��� ������