%spark3.pyspark
import traceback
import subprocess
from pyspark.sql.functions import col, udf, trim
import datetime
from pyspark.sql.types import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import math 
import pyspark.sql.functions as f

dt = spark.read.option("delimiter",";").format("csv").load("/user/olifir_pg/result20220621.csv")
dt = dt.select(*[trim(col(i)).alias(i) for i in dt.columns])
dt = dt.select(
    col('_c0').cast('int').alias('year_begin')
    ,col('_c1').cast('int').alias('week_begin')
    ,col('_c2').cast('int').alias('year_end')
    ,col('_c3').cast('int').alias('week_end')
    ,col('_c4').cast('date').alias('begin_date')
    ,col('_c5').cast('date').alias('end_date')
    ,col('_c6').cast('int').alias('nodeid')
    ,col('_c7').cast('string').alias('g_code')
    ,col('_c8').cast('string').alias('sku')
    ,col('_c9').cast('string').alias('tk2')
    ,col('_c10').cast('string').alias('tk3')
    ,col('_c11').cast('string').alias('tk4')
    ,col('_c12').cast('string').alias('tk5')
    ,col('_c13').cast('string').alias('promo_type')
    ,col('_c14').cast('int').alias('oos_st2')
    ,col('_c15').cast('float').alias('fcst')
    ,col('_c16').cast('int').alias('sales_fact')
    )\
.where("g_code <> 'G_CODE'")

dt.repartition(1).write.format("delta").mode("overwrite").save("/data/base/dixy/result20220621")

#dt.show()

df_save_jesus = spark.read.format('parquet').load('/data/inc/olifir_pg/test_drop/df_save_jesus')
df_save_jesus.write.option("header",True).format("csv").save('/data/inc/olifir_pg/test_drop/mycsv2.csv')