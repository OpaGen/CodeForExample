SELECT 1
/*��������� ���������*/
, BITAND( 1, 1 )
, BITNOT( 2 )
, BITOR( 1, 1 )
, BITXOR( 1, 2 )
--, CAMSET( 'asd' )	--������� � jpg
/*����� ������*/
, CHAR_LENGTH( 'asd' )
, CHARACTER_LENGTH( 'asd' )
, CHARACTERS( 'asd' )
, CHARS( 'asd' )
, CHAR2HEXINT( 'asd' )	--������� � hex
, CHR( 456 )	--������ CHAR � MS SQL
--, COLLAPSE_L
--, COLLAPSE_U
, CORR( NUM, NUM+100 )		--����������� ���������� �������
, COVAR_POP( NUM, NUM+100 )	--����������
, COVAR_SAMP( NUM, NUM+100 )	--����������
--, CREATEXML( '<Greeting>Hello World</Greeting>' )		--������� xml
, ACCOUNT
, CURRENT_ROLE
, CURRENT_USER
, DEGREES( 3.14 )	--����������� �� ������ � �������
, RADIANS( 180 )		--����������� �� �������� � �������
, EDITDISTANCE( 'abc', 'abd' )		--������� �������� ����������� ��� ��������
--, EMPTY_BLOB()
--, EMPTY_CLOB()
, EXP( 1 )		--e � �������
--, GETBIT( 'abc', 1 )
, IDENTIFYDATABASE( 1 )
--, IDENTIFYSESSION( 1, 526725 )
--, IDENTIFYTABLE( 100 )
, IDENTIFYUSER( 1 )
, INITCAP( '������' )
, INSTR( 'abc', 'b', 1 )
, KURTOSIS( num )
, MEDIAN( NUM )
FROM PRD_VD_DM.V_NUMBERS
WHERE 1=1
AND NUM<10;

SELECT DECODE( NUM, 1, 'HAVE FOUND 1' )	--����� �� �������
, CSUM( NUM, NUM )	--������������� �����
, DEFAULT( NUM )	--�������� �� ��������� ��� �������
, NUM
, LAST( PERIOD( DATE '2016-01-01', DATE '2016-01-31' ) )		--��������� ���� � ������� (��� ������)
, LAST_DAY( CURRENT_DATE )		--��������� ���� � �������
, LN( 2.72 )
, LOG( 10 )
, LPAD( '123', 10, '0' )		--��������� �������� ��� ������������ �������
, RPAD( '123', 10, '0' )		--��������� �������� ��� ������������ �������
/*�����������/�������������*/
, LZCOMP( 'abc' )
, LZCOMP_L( 'abc' )
, LZDECOMP( LZCOMP( 'abc' ) )
, LZDECOMP_L( LZCOMP_L( 'abc' ) )
FROM PRD_VD_DM.V_NUMBERS
WHERE 1=1
AND NUM<10;

SELECT DAY_ID
, DAY_OF_WEEK
--, MONTHS_BETWEEN( DAY_ID, DAY_ID )		--������� ����� �����
, MAVG( DAY_OF_WEEK, 3 , DAY_ID ) as mavg_	--���������� ������� �� �������������� ���������� ��������� ����� �� ������� ������
, MSUM( DAY_OF_WEEK, 3 , DAY_ID ) as msum_	--���������� ����� ����� ������� ��������� � ��������� �� ������� �� ��������
, MDIFF( DAY_OF_WEEK, 3 , DAY_ID ) as mdiff_	--���������� ������� ����� ������� ��������� � ��������� �� ������� �� ��������
, MLINREG( DAY_OF_WEEK, 3 , DAY_ID ) as mlinreg_	--���������� �������� ���������
, NEXT_DAY( DAY_ID, 'WED' )		--����� ����� ����
, NUMTODSINTERVAL( 10, 'DAY' )
, NUMTODSINTERVAL( 10, 'HOUR' )
, NUMTODSINTERVAL( 10, 'MINUTE' )
, NUMTODSINTERVAL( 10, 'SECOND' )
, NUMTOYMINTERVAL( 2017, 'YEAR' )
, NUMTOYMINTERVAL( 10, 'MONTH' )
, NVL( 1, NULL )		--���������� ����� ��������� �������� ����� �������
, NVL2( 1, NULL, 2 )		--������ 3-�, ���� 1-� ����������, ����� 2-�
, NVP('100:Jugal#9004','100','#',':',1)		--�� ���������� ����
, OADD_MONTHS( CAST( '2016-02-29' as DATE ), 1 )		--��������� ����� � ����������� �� ���
, ADD_MONTHS( CAST( '2016-02-29' as DATE ), 1 )			--��������� �����
, OREPLACE( 'asdf', 'f', 'g' )		--������ REPLACE
, OTRANSLATE( 'abcdef', 'abc', 'xyz' )		--�������� ������� �������� 
, PERIOD( DATE '2016-01-01', DATE '2016-01-31' ) P_INTERSECT PERIOD( DATE '2016-01-10', DATE '2016-01-20' )		--����������� ��������
, PERIOD( DATE '2016-01-01', DATE '2016-01-31' ) P_NORMALIZE PERIOD( DATE '2016-01-10', DATE '2016-01-20' )		--���������� ���� ��������
, POSITION( 'b' IN 'abc' )		--���������� ����� ������� ��������� �������� � ��������
, REVERSE( 'abc' )
, ROTATELEFT( 1024, 10 )		--��������� ������� 2��
, ROTATERIGHT( 1, 10 )		--����������� ������� 2��
, SHIFTRIGHT( 1024, 2 )
, SHIFTLEFT( 2, 2 )
, SIGN( DAY_OF_WEEK )
FROM PRD_VD_DM.V_DAYS
WHERE 1=1
AND MONTH_ID=201703;

SELECT REGR_AVGX( NUM, NUM )	--�������
, REGR_AVGY( NUM, NUM )		--�������
, REGR_COUNT( NUM, NUM )	--��������
, REGR_INTERCEPT( NUM, NUM )
, REGR_R2( NUM, NUM )
, REGR_SLOPE( NUM, NUM )
, REGR_SXX( NUM, NUM )
, REGR_SXY( NUM, NUM )
, REGR_SYY( NUM, NUM )
, SOUNDEX( 'abc' )
, STRTOK( 'AAL53CCW_400-5-13-3', '-', 3 )		--�� ����������� ������� ���������� �������
, TITLE( NUM )
FROM PRD_VD_DM.V_NUMBERS
WHERE NUM IN ( 3, 4 );

SELECT SysSpatial.SPHERICALDISTANCE( w.LATITUDE, w.LONGITUDE, ww.LATITUDE, ww.LONGITUDE )
, SysSpatial.SPHEROIDALDISTANCE( w.LATITUDE, w.LONGITUDE, ww.LATITUDE, ww.LONGITUDE )
FROM PRD_VD_DM.V_WHS w
CROSS JOIN PRD_VD_DM.V_WHS ww
WHERE 1=1
AND w.WHS_ID=999
AND ww.WHS_ID=1000;

SELECT  * 
FROM TABLE 
	(
	STRTOK_SPLIT_TO_TABLE( 1 , '2017-03-20' , '-'  ) 
	RETURNS ( OUTKEY INT , TOKENNUM INT , TOKEN2 VARCHAR ( 20 ) CHARACTER SET unicode )
	) d;
	
SELECT TD_SUNDAY( CURRENT_DATE )
, TD_MONDAY( CURRENT_DATE )
, TD_TUESDAY( CURRENT_DATE )
, TD_WEDNESDAY( CURRENT_DATE )
, TD_THURSDAY( CURRENT_DATE )
, TD_FRIDAY( CURRENT_DATE )
, TD_SATURDAY( CURRENT_DATE )
, TD_WEEK_BEGIN( CURRENT_DATE )
, TD_WEEK_END( CURRENT_DATE )
, TD_MONTH_BEGIN( CURRENT_DATE )
, TD_MONTH_END( CURRENT_DATE )
, TD_QUARTER_BEGIN(  CURRENT_DATE )
, TD_QUARTER_END( CURRENT_DATE )
, TD_YEAR_BEGIN( CURRENT_DATE )
, TD_YEAR_END( CURRENT_DATE );

SELECT TO_BYTE( 2 )
, TO_CHAR( CURRENT_DATE )
, TO_DATE( '2017-03-24', 'YYYY-MM-dd' )
, TO_NUMBER( '123' )
, TRUNC( CURRENT_DATE )
, TS_COMPRESS( CURRENT_TIME )
--, TS_DECOMPRESS( TS_COMPRESS( CURRENT_TIME ) )
, TYPE( 'asdf' )