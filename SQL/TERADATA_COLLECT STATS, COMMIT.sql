COLLECT STATISTICS
COLUMN (index columns),
COLUMN (index columns)) ON table;
COMMIT;

COLLECT STATISTICS 
--USING NO THRESHOLD		�� ������� �������� �� ������, ������� ������� � ��������� �������� (����������� ��������� ������ ��� ���������)
--USING THRESHOLD 10 PERCENT AND 30 DAYS		--������������ ����� � 10% ��������� ������ ��� 30 ���� ����� ���������� �����
--FOR CURRENT	--�� ���������� ���������� ��� ���������� ����� ����������
COLUMN ( ACT_TYPE_ID , MKTNG_ACTN_TMPLTS_ID ) 
, COLUMN ( ART_ID ) 
, COLUMN ( ACT_TYPE_ID ) 
, COLUMN ( WHS_ID ) 
, COLUMN ( MKTNG_ACTN_TMPLTS_ID ) 
, COLUMN ( PARTITION ) 
ON PRD_DB_DM.D_AUM_WHS_ART_WEEK;