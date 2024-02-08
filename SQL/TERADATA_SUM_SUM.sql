SELECT day_id
, CSUM(week_year_number, day_id) as cumulative_sum
, week_year_number

FROM v_days

WHERE year_id=2015

ORDER BY day_id