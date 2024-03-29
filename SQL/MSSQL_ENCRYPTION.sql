USE [SOURCES]
GO

/****** Object:  View [dbo].[V_DAYS]    Script Date: 04.07.2017 13:51:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[V_TEST_ENCRYPTION] 
WITH ENCRYPTION		--������� �����������, ����� ������ ���� ������������� � ���������

AS

SELECT DAY_ID
, DAY_MONTH_STR
, DAY_MONTH_NUMBER
, DAY_YEAR_STR
, DAY_YEAR_NUMBER
, DAY_DATE_STR
, DAY_OF_WEEK
, DAY_OF_WEEK_SHORT_NAME
, DAY_OF_WEEK_LONG_NAME
, DAY_FIRST_DTM
, DAY_LAST_DTM
, YEAR_ID
, QUARTER_ID
, QUARTER_NUMBER
, MONTH_ID
, MONTH_NUMBER
, MN
, MONTH_DAYS_COUNT
, WEEK_ID
, WEEK_ID_2
, WEEK_ID_3
, WEEK_ID_4
, WEEK_YEAR_NUMBER
, WEEK_MONTH_NUMBER
, WEEK_ISO_YEAR
, WEEK_ISO_NUMBER
, IS_HOLID
, PREV_YEAR_DAY
, PREV_YEAR
, PREV_MONTH_DAY
, PREV_MONTH
, PREV_WEEK_DAY
, PREV_WEEK_ID
, PREV_WEEK_ID2
, LAST_YEAR_WEEK_ID_2
, PREV_WEEK_ID3
, PREV_DAY
, IS_PREV_HOLID
FROM SOURCES.dbo.TD_V_DAYS WITH (NOLOCK)

WITH CHECK OPTION;

GO


