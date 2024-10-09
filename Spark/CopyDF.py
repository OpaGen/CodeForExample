from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark
import numpy as np
import datetime 
import logging
import sys
import jaydebeapi
import os
import jpype
import sys
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("etl_coupon_vend_mk_tva")\
.config("spark.eventLog.enabled", "false")\
.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

p_calc_dt = spark.read.format("delta").load("/data/base/d_market_company_coef_coupon_vend").agg(f.max("day_id_update")).take(1)[0][0]

df_art = spark.read.format("delta").load("/data/base/d_market_company_coef_coupon_vend")\
.where(f"day_id_update = '{p_calc_dt}'")\
.select(
      "day_id_update"
    , "art_id"
    , "market_company_id"
    , "frmt_id"
    , "type_k"
    , "k_w0_ceiling"
    , "k_with_ceiling"
    , "k_mc"
)
if df_art.count() > 0:
    
    with jaydebeapi.connect('com.teradata.jdbc.TeraDriver', 'jdbc:teradata://tdtva/CHARSET=UTF16,TMODE=ANSI', ["SVC_TABLEAU_USERS","svctableau"], jars=["/opt/packages/terajdbc4.jar", "/opt/packages/tdgssconfig.jar"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"delete from ANP.T_MARKET_COMPANY_COEF_COUPON_VEND_STAGE ;")
    df_art\
    .repartition(1)\
    .write\
    .format("jdbc")\
    .option("url","jdbc:teradata://tdtva/CHARSET=UTF16,TMODE=ANSI,LOGMECH=TD2")\
    .option("user","svc_tableau_srv")\
    .option("password", "svctableausrv")\
    .option("dbtable", "ANP.T_MARKET_COMPANY_COEF_COUPON_VEND_STAGE")\
    .option("driver", "com.teradata.jdbc.TeraDriver").mode("append").save()

    with jaydebeapi.connect('com.teradata.jdbc.TeraDriver', 'jdbc:teradata://tdtva/CHARSET=UTF16,TMODE=ANSI', ["SVC_TABLEAU_USERS","svctableau"], jars=["/opt/packages/terajdbc4.jar", "/opt/packages/tdgssconfig.jar"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CALL ANP.PRC_MARKET_COMPANY_COEF_COUPON_VEND_UPDATE('{p_calc_dt}');")
    
    print(1)