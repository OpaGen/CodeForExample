DECLARE
@CURSOR1            CURSOR,
@ART_ID            VARCHAR(MAX),
@D_1 AS DATE,
@D_2 AS DATE;

SET @D_1 = GETDATE()-20;

SET @D_2 =  GETDATE();

--truncate table Forecast_Gif.dbo.GPC_SALE_TT_TD

SET @CURSOR1  = CURSOR SCROLL FOR

SELECT DISTINCT aart as art_id
FROM  Forecast_Gif..temp_art_id_list


OPEN @CURSOR1;
FETCH NEXT FROM @CURSOR1 INTO   @ART_ID
WHILE @@FETCH_STATUS = 0
BEGIN
---------------------------- тело курсора ------------------------


EXEC procedure_name '24377', @ART_ID, @D_1, @D_2;

------------------------------------------------------------------

FETCH NEXT FROM @CURSOR1 INTO  @ART_ID
END;
