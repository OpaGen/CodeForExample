SELECT
  SIGN(-123)                                         as SIGN
, TRUNC (32.976)                                as TRUNC
, ROUND(345.175)                               as ROUND
, GREATEST(12,10)          as GREATEST
, LEAST (12,10)                 as LEAST
, TO_NUMBER ('4769.96', '9999.99')  as TO_NUMBER
, CEILING( 5.4)                                            as CEIL
, FLOOR(3.86)                                      as FLOOR
;