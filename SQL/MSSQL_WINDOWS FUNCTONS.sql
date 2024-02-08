SELECT BRANCH_ID
, WHS_ID
, LAG( WHS_ID, 1, -1 ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) LAG
, LEAD( WHS_ID, 1, -1 ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) LEAD
, FIRST_VALUE( WHS_ID ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) FIRST_VALUE
, LAST_VALUE( WHS_ID ) OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) LAST_VALUE	--����� ������ ���������� ��������
, CUME_DIST() OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) as CUME_DIST	--��������� � �������, ��� ��������� ���� ����� � ���� �� ���������, ������ �������� (�� ����������) �� ���� ������� ����
, PERCENT_RANK() OVER ( PARTITION BY BRANCH_ID ORDER BY WHS_ID ) as PERCENT_RANK	--������ �� ������� ����
, PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY WHS_ID ) OVER ( PARTITION BY BRANCH_ID ) as PERCENTILE_CONT	--���������� �������� �� ���������� (�������� �������), �������� ������ ��� ����� �� ������ ������� CUME_DIST � ����� �� ����� � �����������
, PERCENTILE_DISC(0.5) WITHIN GROUP ( ORDER BY WHS_ID ) OVER ( PARTITION BY BRANCH_ID ) as PERCENTILE_DISC	--������ ������� ����, �� � ������ ������� ���������� ��������� � �������, ������� ������� ����� ��������� �� ��������� CUBE_DIST � ��������� ���������, ��� � ���������� � �������

FROM SOURCES.dbo.V_WHS
WHERE 1=1
AND BRANCH_ID<>-1
ORDER BY BRANCH_ID
, WHS_ID;