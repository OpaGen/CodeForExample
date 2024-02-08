
from pyspark import StorageLevel
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import pyspark
import numpy as np
import subprocess
import sys
import datetime
import logging
import math
from delta.tables import *

def getBit(num, i):
    str = "000" + ("{0:b}".format(num))
    return (str)[-(i+1)]

spark = SparkSession.builder.appName("elasticity_fit") \
       .config("spark.eventLog.enabled", "false") \
       .getOrCreate()


spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions",2000)
spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

spark.conf.set("spark.sql.shuffle.partitions",2000)
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

cnt_part = int(spark.conf.get("spark.executor.instances"))
cnt_shuffle = int(spark.conf.get("spark.sql.shuffle.partitions"))
cnt_part_c = int(spark.conf.get("spark.executor.cores"))
cnt_part_imp = cnt_part*cnt_part_c

sql = """
    SELECT m.WHS_ID
        ,CASE WHEN s.GS_FO_ID IN (1071315/*Крымский федеральный округ*/, 482104/*Северо-Кавказский федеральный округ*/) THEN 481752 /*Южный федеральный округ*/
              WHEN s.GS_FO_ID IN (482364/*Сибирский федеральный округ*/) THEN 481965 /*Уральский федеральный округ*/
              ELSE s.GS_FO_ID END AS GS_FO_ID
         ,m.FRMT_ID
        ,m.SUBFRMT_ID             
    FROM ANP.V_ELASTICY_WHS_GS_FO s
    INNER JOIN PRD_VD_DM.V_WHS m ON s.GS_MAIN_ID = m.GS_MAIN_ID
    WHERE s.GS_TYPE_ID = 318
"""

df = spark.read.format("jdbc")\
    .option("url","jdbc:teradata://tdtva/CHARSET=UTF16,TMODE=ANSI")\
    .option("user","anp_infa")\
    .option("password", "anp_infa")\
    .option("dbtable", f"({sql}) emp") \
    .option("driver", "com.teradata.jdbc.TeraDriver")\
.load()

df = df.toDF(*[c.lower() for c in df.columns])

df.repartition(cnt_part_imp).write.format("delta").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_gs_fo_whs") 

df_art_ext = spark.read.format('delta').load('/data/base/d_art_ext').persist()

vol_grp_art = df_art_ext\
.withColumn("direction_type", f.when(df_art_ext.direction.isin(3,4), f.lit(34)).otherwise(f.lit(12)))\
.withColumn("brand", f.expr("replace(replace(replace(replace(replace(lower(art_grp_2_lvl_1_name), 'c', 'с'), ' ооо', ''), ' оао', ''), ' кк', ''), ' мк', '')"))\
.select(
        df_art_ext.art_id,
        df_art_ext.art_grp_lvl_3_id.alias("grp_art_id"),
        f.col("direction_type"),
        f.col("brand")
    )
    
df_rep_elasticity = spark.read.format('delta').load('/data/base/d_rep_elasticity').where(f"scnd_sale_qnty > 0 and scnd_client_cnt > 0 and scnd_txn_cnt > 0 and scnd_price > 0 and frst_sale_qnty > 0 and frst_client_cnt > 0 and frst_txn_cnt > 0 and frst_price > 0 and scnd_day_cnt_rest_sales > 0 and (cast(scnd_price as float) / frst_price - 1 <= 0) and frst_begin_dt not between '2020-03-09' and '2020-05-03' and frst_end_dt not between '2020-03-09' and '2020-05-03' and scnd_begin_dt not between '2020-03-09' and '2020-05-03' and scnd_end_dt not between '2020-03-09' and '2020-05-03' and frst_begin_dt not between '2022-02-24' and '2022-03-22' and frst_end_dt not between '2022-02-24' and '2022-03-22' and scnd_begin_dt not between '2022-02-24' and '2022-03-22' and scnd_end_dt not between '2022-02-24' and '2022-03-22'")

p_date_calc = df_rep_elasticity \
.join(f.broadcast(vol_grp_art), on = ["art_id"] , how = "inner")\
.agg(f.max(df_rep_elasticity.scnd_begin_dt))

p_date_calc = p_date_calc.take(1)[0][0]
p_date_calc_begin = (p_date_calc - datetime.timedelta(days = (365*3) + 1)).strftime("%Y-%m-%d")
#datetime.datetime.strptime('2018-10-12', "%Y-%m-%d")
#COMPLETE VOL_TREND_PRIMARY
vol_trend_primary = df_rep_elasticity\
.join(f.broadcast(vol_grp_art), on = ["art_id"], how = "inner" )\
.where(f"scnd_begin_dt >= '{p_date_calc_begin}' ")\
.select (
           df_rep_elasticity.whs_id,
           df_rep_elasticity.art_id,
           df_rep_elasticity.algorithm_type,
           df_rep_elasticity.scnd_reason_type_id,
           df_rep_elasticity.scnd_holiday_id,
           df_rep_elasticity.frst_price.cast("decimal(18,6)"),
           df_rep_elasticity.scnd_price.cast("decimal(18,6)"),
           df_rep_elasticity.frst_sale_qnty,
           df_rep_elasticity.scnd_sale_qnty,
           df_rep_elasticity.frst_day_cnt_rest_sales,
           df_rep_elasticity.scnd_day_cnt_rest_sales,
           df_rep_elasticity.frst_client_cnt,
           df_rep_elasticity.scnd_client_cnt,
           df_rep_elasticity.frst_txn_cnt,
           df_rep_elasticity.scnd_txn_cnt,
           df_rep_elasticity.ssn_coef_sale_txn,
           df_rep_elasticity.frst_begin_dt,
           df_rep_elasticity.frst_end_dt,
           df_rep_elasticity.scnd_begin_dt,
           df_rep_elasticity.scnd_end_dt
    )

vol_trend_primary.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_primary")
vol_trend_primary = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_primary').repartition("whs_id")

print("Собрали vol_trend_primary")
#print(vol_trend_primary.count())

#COMPLETE VOL_ACTION_DMP

df_masm_mc_action_header = spark.read.format('delta').load('/data/base/d_masm_mc_action_header').where(f"status_id in (411,421,700,701,702 , 710, 711) and end_dt >= '{p_date_calc_begin}'")

window_row_num = Window.partitionBy(df_masm_mc_action_header.mc_id).orderBy(df_masm_mc_action_header.load_dtm.desc())

df_masm_mc_action_header = df_masm_mc_action_header\
.withColumn("row_number", f.row_number().over(window_row_num))\
.where(f"row_number=1")

df_masm_mc_action_whs = spark.read.format('delta').load('/data/base/d_masm_mc_action_whs').select("mc_id" , "whs_id")
df_masm_mc_action = spark.read.format('delta').load('/data/base/d_masm_mc_action').where(f"lower(name) like '%дмп%'").select("action_id")

vol_action_dmp = df_masm_mc_action_header\
.join(f.broadcast(vol_grp_art), on = ["art_id"], how = "inner")\
.join(df_masm_mc_action_whs, on = ["mc_id"], how = "inner")\
.join(df_masm_mc_action, on = ["action_id"], how = "inner")\
.select(
        df_masm_mc_action_header.begin_dt.alias("action_begin_dt"),
        df_masm_mc_action_header.end_dt.alias("action_end_dt"),
        df_masm_mc_action_whs.whs_id,
        df_masm_mc_action_header.art_id
    ).distinct()

vol_action_dmp.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_action_dmp")
vol_action_dmp = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_action_dmp').repartition("whs_id")

print("Собрали vol_action_dmp")
#print(vol_action_dmp.count())

#COMPLETE VOL_DMP
vol_dmp = vol_trend_primary\
.join(vol_action_dmp, on = ["whs_id", "art_id"], how = "inner")\
.withColumn("min_end_dt", f.when(vol_action_dmp.action_end_dt <= vol_trend_primary.scnd_end_dt,vol_action_dmp.action_end_dt ).otherwise(vol_trend_primary.scnd_end_dt))\
.withColumn("max_beg_dt", f.when(vol_action_dmp.action_begin_dt >= vol_trend_primary.scnd_begin_dt,vol_action_dmp.action_begin_dt ).otherwise(vol_trend_primary.scnd_begin_dt))\
.withColumn("check_one", f.when( (vol_action_dmp.action_begin_dt <= vol_trend_primary.scnd_end_dt) & (vol_action_dmp.action_end_dt >= vol_trend_primary.scnd_begin_dt) , f.lit(1)))\
.withColumn("check_two", f.when( ((f.datediff(f.col("min_end_dt"),f.col("max_beg_dt")) + 1.0) / (f.datediff(vol_trend_primary.scnd_end_dt,vol_trend_primary.scnd_begin_dt) + 1.0)) >= f.lit(0.7),f.lit(1) ) )\
.withColumn("is_vol_dmp",f.lit(1))\
.where((f.col("check_one") == f.lit(1)) & (f.col("check_two") == f.lit(1)))\
.select(
        vol_trend_primary.whs_id,
        vol_trend_primary.art_id,
        vol_trend_primary.scnd_begin_dt,
        vol_trend_primary.algorithm_type,
        f.col("is_vol_dmp")
    ).distinct()

vol_dmp.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_dmp")
vol_dmp = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_dmp').repartition("whs_id")

print("Собрали vol_dmp")
#print(vol_dmp.count())

    
#COMPLETE VOL_DMP_PREV
vol_dmp_prev = vol_trend_primary\
.join(vol_action_dmp, on = ["whs_id", "art_id"], how = "inner")\
.withColumn("min_end_dt", f.when(vol_action_dmp.action_end_dt <= vol_trend_primary.frst_end_dt,vol_action_dmp.action_end_dt ).otherwise(vol_trend_primary.frst_end_dt))\
.withColumn("max_beg_dt", f.when(vol_action_dmp.action_begin_dt >= vol_trend_primary.frst_begin_dt,vol_action_dmp.action_begin_dt ).otherwise(vol_trend_primary.frst_begin_dt))\
.withColumn("check_one", f.when( (vol_action_dmp.action_begin_dt <= vol_trend_primary.frst_end_dt) & (vol_action_dmp.action_end_dt >= vol_trend_primary.frst_begin_dt) , f.lit(1)))\
.withColumn("check_two", f.when( ((f.datediff(f.col("min_end_dt"),f.col("max_beg_dt")) + f.lit(1.0)) / (f.datediff(vol_trend_primary.frst_end_dt,vol_trend_primary.frst_begin_dt) + f.lit(1.0))) >= f.lit(0.7),f.lit(1) ) )\
.withColumn("is_vol_dmp_prev",f.lit(1))\
.where((f.col("check_one") == f.lit(1)) & (f.col("check_two") == f.lit(1)))\
.select(
        vol_trend_primary.whs_id,
        vol_trend_primary.art_id,
        vol_trend_primary.frst_begin_dt,
        vol_trend_primary.algorithm_type,
        f.col("is_vol_dmp_prev")
    ).distinct()

vol_dmp_prev.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_dmp_prev")
vol_dmp_prev = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_dmp_prev').repartition("whs_id")

print("Собрали vol_dmp_prev")
#print(vol_dmp_prev.count())


#COMPLETE VOL_ACTION_RC
df_rc_calendar = spark.read.format('delta').load('/data/base/d_rc_calendar').where(f"market_company_id in (1,21) and end_dt >= '{p_date_calc_begin}'")

vol_action_rc = df_rc_calendar\
.join(vol_grp_art, on = ["art_id"], how = "inner")\
.select (
        df_rc_calendar.beg_dt,
        df_rc_calendar.end_dt,
        df_rc_calendar.whs_id,
        df_rc_calendar.art_id
    ).distinct()

vol_action_rc.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_action_rc")
vol_action_rc = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_action_rc').repartition("whs_id")

print("Собрали vol_action_rc")
#print(vol_action_rc.count())

    
#COMPLETE VOL_RC   
vol_rc = vol_trend_primary\
.join(vol_action_rc, on = ["whs_id", "art_id"], how = "inner")\
.withColumn("min_end_dt", f.when(vol_action_rc.end_dt <= vol_trend_primary.scnd_end_dt,vol_action_rc.end_dt ).otherwise(vol_trend_primary.scnd_end_dt))\
.withColumn("max_beg_dt", f.when(vol_action_rc.beg_dt >= vol_trend_primary.scnd_begin_dt,vol_action_rc.beg_dt ).otherwise(vol_trend_primary.scnd_begin_dt))\
.withColumn("check_one", f.when( (vol_action_rc.beg_dt <= vol_trend_primary.scnd_end_dt) & (vol_action_rc.end_dt >= vol_trend_primary.scnd_begin_dt) , f.lit(1)))\
.withColumn("check_two", f.when( ((f.datediff(f.col("min_end_dt"),f.col("max_beg_dt")) + f.lit(1.0)) / (f.datediff(vol_trend_primary.scnd_end_dt,vol_trend_primary.scnd_begin_dt) + 1.0)) >= f.lit(0.3),f.lit(1) ) )\
.withColumn("is_vol_rc",f.lit(1))\
.where((f.col("check_one") == f.lit(1)) & (f.col("check_two") == f.lit(1)))\
.select(
        vol_trend_primary.whs_id,
        vol_trend_primary.art_id,
        vol_trend_primary.scnd_begin_dt,
        f.col("is_vol_rc")
    ).distinct()

vol_rc.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_rc")
vol_rc = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_rc').repartition("whs_id")

print("Собрали vol_rc")
#print(vol_rc.count())

   
#COMPLETE VOL_RC_PREV
vol_rc_prev = vol_trend_primary\
.join(vol_action_rc, on = ["whs_id", "art_id"], how = "inner")\
.withColumn("min_end_dt", f.when(vol_action_rc.end_dt <= vol_trend_primary.frst_end_dt,vol_action_rc.end_dt ).otherwise(vol_trend_primary.frst_end_dt))\
.withColumn("max_beg_dt", f.when(vol_action_rc.beg_dt >= vol_trend_primary.frst_begin_dt,vol_action_rc.beg_dt ).otherwise(vol_trend_primary.frst_begin_dt))\
.withColumn("check_one", f.when( (vol_action_rc.beg_dt <= vol_trend_primary.frst_end_dt) & (vol_action_rc.end_dt >= vol_trend_primary.frst_begin_dt) , f.lit(1)))\
.withColumn("check_two", f.when( ((f.datediff(f.col("min_end_dt"),f.col("max_beg_dt")) + f.lit(1.0)) / (f.datediff(vol_trend_primary.frst_end_dt,vol_trend_primary.frst_begin_dt) + 1.0)) >= f.lit(0.3),f.lit(1) ) )\
.withColumn("is_vol_rc_prev",f.lit(1))\
.where((f.col("check_one") == f.lit(1)) & (f.col("check_two") == f.lit(1)))\
.select(
        vol_trend_primary.whs_id,
        vol_trend_primary.art_id,
        vol_trend_primary.frst_begin_dt,
        f.col("is_vol_rc_prev")
    ).distinct()

vol_rc_prev.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_rc_prev")
vol_rc_prev = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_rc_prev').repartition("whs_id")

print("Собрали vol_rc_prev")
#print(vol_rc_prev.count())

 
p_wo_dmp = 1  

#COMPLETE vol_trend_temp
df_whs = spark.read.format('delta').load('/data/base/d_whs')
#.where(f"frmt_id = 2")

vol_gs_fo_whs = spark.read.format('delta').load('/data/inc/olifir_pg/elasticity/p_df_vol_gs_fo_whs').repartition("whs_id")


vol_gs_fo_whs = vol_gs_fo_whs\
.join(f.broadcast(df_whs) , on = ["whs_id"], how = "inner")\
.select(
    vol_gs_fo_whs.whs_id,
    vol_gs_fo_whs.gs_fo_id,
    vol_gs_fo_whs.frmt_id,
    vol_gs_fo_whs.subfrmt_id
    )
    
    
vol_trend_temp = vol_trend_primary \
.join(vol_grp_art, on = ["art_id"], how = "inner")\
.join(vol_gs_fo_whs, on = ["whs_id"], how = "inner")\
.join(vol_dmp, on = ["whs_id","art_id","scnd_begin_dt","algorithm_type"], how = "left")\
.join(vol_dmp_prev, on = ["whs_id","art_id","frst_begin_dt","algorithm_type"], how = "left")\
.join(vol_rc, on = ["whs_id","art_id","scnd_begin_dt"], how = "left")\
.join(vol_rc_prev, on = ["whs_id","art_id","frst_begin_dt"], how = "left")\
.withColumn("aum_type_id" , f.when(vol_trend_primary.scnd_reason_type_id.isin(21001,24062) ,f.lit(1)).otherwise(f.when(vol_trend_primary.scnd_reason_type_id == f.lit(21007),f.lit(2)).otherwise(f.when(vol_trend_primary.scnd_reason_type_id == f.lit(21004),f.lit(3)).otherwise(f.lit(4)))))\
.withColumn("scnd_holiday_id",f.coalesce(vol_trend_primary.scnd_holiday_id,f.lit(0)))\
.withColumn("disc",  (vol_trend_primary.scnd_price.cast("decimal(18,6)")  /  vol_trend_primary.frst_price.cast("decimal(18,6)") - f.lit(1.00).cast("decimal(18,6)")).cast("decimal(18,6)"))\
.withColumn("skidgr",f.when(f.abs(f.round(f.col("disc"),5)) > f.lit(0.8), f.floor(f.round(f.col("disc") * 10,5)) * 100).otherwise(f.when(f.abs(f.round(f.col("disc"),5)) > f.lit(0.5), f.floor(f.round(f.col("disc") * 20,5)) * 50).otherwise(f.floor(f.round(f.col("disc") * 40,5)) * 25)) )\
.withColumn("prir_sale",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_day_cnt_rest_sales / vol_trend_primary.scnd_day_cnt_rest_sales) - 1.00000)\
.withColumn("prir_client",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_client_cnt / vol_trend_primary.scnd_client_cnt) - 1.00000 )\
.withColumn("prir_txn",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_txn_cnt / vol_trend_primary.scnd_txn_cnt)  - 1.00000 )\
.withColumn("prir_ssn",(vol_trend_primary.scnd_sale_qnty / (vol_trend_primary.frst_sale_qnty * f.when(vol_trend_primary.ssn_coef_sale_txn == f.lit(0), None).otherwise(vol_trend_primary.ssn_coef_sale_txn))) - 1 )\
.where(f"prir_sale >=0 and is_vol_dmp is null and is_vol_dmp_prev is null and is_vol_rc is null and is_vol_rc_prev is null")\
.select(
    vol_trend_primary.whs_id,
    vol_trend_primary.art_id,
    vol_grp_art.grp_art_id,
    vol_grp_art.brand,
    vol_gs_fo_whs.gs_fo_id,
    vol_gs_fo_whs.frmt_id,
    vol_trend_primary.algorithm_type,
    f.col("aum_type_id"),
    f.col("scnd_holiday_id"),
    vol_grp_art.direction_type,
    f.col("disc"),
    f.col("skidgr"),
    f.col("prir_sale"),
    f.col("prir_client"),
    f.col("prir_txn"),
    f.col("prir_ssn")
    )
    
vol_trend_temp.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_temp")
vol_trend_temp = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_temp').repartition("whs_id")

print("Собрали vol_trend_temp")


p_brand = 1
subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/df_elasticity_raw_fltr/".strip().split())

while p_brand >=0:
    print("p_brand = " , p_brand)

    p_bitmap = 3
    p_max_s = 4
    p_min_s = 1

    #добавить очистку
    subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare/".strip().split())
    subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl/".strip().split())


    while p_bitmap >=0:
        
        subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare/".strip().split())
        
        vol_trend_prepare = vol_trend_temp \
        .withColumn("gs_fo_id_", f.expr("case when " + getBit(p_bitmap,0)+" = 0 then -1 else gs_fo_id end"))\
        .withColumn("frmt_id_", f.expr("case when " + getBit(p_bitmap,1)+" = 0 then -1 else frmt_id end"))\
        .withColumn("aum_type_id", f.lit(-1))\
        .withColumn("scnd_holiday_id", f.lit(-1))\
        .withColumn("direction_type", f.lit(-1))\
        .withColumn("flag_sale", f.lit(1))\
        .withColumn("flag_client", f.lit(1))\
        .withColumn("flag_txn", f.lit(1))\
        .withColumn("flag_ssn", f.expr("case when prir_ssn is null then 0 else 1 end"))\
        .withColumn("flag_step", f.lit(0))\
        .withColumn("brand_", f.when(f.lit(p_brand) == f.lit(1), f.lit("-1")).otherwise(vol_trend_temp.brand))\
        .select(
            vol_trend_temp.whs_id,
            vol_trend_temp.art_id,
            vol_trend_temp.grp_art_id, 
            f.col("brand_").alias("brand"), 
            f.col("gs_fo_id_").alias("gs_fo_id"), 
            f.col("frmt_id_").alias("frmt_id"),  
            vol_trend_temp.algorithm_type,
            f.col("aum_type_id"),
            f.col("scnd_holiday_id"),
            f.col("direction_type"),
            vol_trend_temp.disc,
            vol_trend_temp.skidgr, 
            vol_trend_temp.prir_sale,
            vol_trend_temp.prir_client,
            vol_trend_temp.prir_txn,
            vol_trend_temp.prir_ssn,
            f.col("flag_sale"),
            f.col("flag_client"),
            f.col("flag_txn"),
            f.col("flag_ssn"),
            f.col("flag_step")
            )
        
        vol_trend_prepare.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare")
        vol_trend_prepare = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare').repartition("whs_id")
    
        print("p_bitmap = " , p_bitmap)
        #print("vol_trend_prepare = " , vol_trend_prepare.count())
    
        s = p_min_s
    
        while s<= p_max_s:
            print("s = " , s)
        
            if (s == 1):
                tr = 3.0
            elif (s == 2):
                tr = 2.5
            elif (s == 3):
                tr = 2.0
            elif (s == 4):
                tr = 2.0
            
            subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl/".strip().split())
            
            vol_trend_lvl = vol_trend_prepare \
            .where( vol_trend_prepare.flag_step == f.lit(s - 1))\
            .withColumn("prir_sale_", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.prir_sale).otherwise(None))\
            .withColumn("prir_client_", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.prir_client).otherwise(None))\
            .withColumn("prir_txn_", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.prir_txn).otherwise(None))\
            .withColumn("prir_ssn_", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.prir_ssn).otherwise(None))\
            .groupBy(
                 vol_trend_prepare.skidgr        
                ,vol_trend_prepare.grp_art_id  
                ,vol_trend_prepare.brand
                ,vol_trend_prepare.gs_fo_id       
                ,vol_trend_prepare.frmt_id      
                ,vol_trend_prepare.algorithm_type  
                ,vol_trend_prepare.aum_type_id    
                ,vol_trend_prepare.scnd_holiday_id 
                ,vol_trend_prepare.direction_type
                )\
            .agg(
                f.avg(f.col("prir_sale_")).alias("avg_prir_sale"),
                f.stddev_pop(f.col("prir_sale_")).alias("std_prir_sale"),
                f.avg(f.col("prir_client_")).alias("avg_prir_client"),
                f.stddev_pop(f.col("prir_client_")).alias("std_prir_client"),    
                f.avg(f.col("prir_txn_")).alias("avg_prir_txn"),
                f.stddev_pop(f.col("prir_txn_")).alias("std_prir_txn"),
                f.avg(f.col("prir_ssn_")).alias("avg_prir_ssn"),
                f.stddev_pop(f.col("prir_ssn_")).alias("std_prir_ssn")
                )
    
            vol_trend_lvl.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl")
            vol_trend_lvl = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl').repartition("grp_art_id")
            
            #print("vol_trend_lvl = " , vol_trend_lvl.count())
            
            df_temp_vol_trend_prepare = vol_trend_prepare \
            .join(vol_trend_lvl, on = ["skidgr","grp_art_id","brand","gs_fo_id","frmt_id","algorithm_type","aum_type_id","scnd_holiday_id","direction_type"], how = "inner")\
            .where(vol_trend_prepare.flag_step == f.lit(s - 1))\
            .withColumn("flag_sale", f.when(vol_trend_prepare.prir_sale.between(f.round(vol_trend_lvl.avg_prir_sale - tr * vol_trend_lvl.std_prir_sale,6), f.round(vol_trend_lvl.avg_prir_sale + tr * vol_trend_lvl.std_prir_sale,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_client", f.when(vol_trend_prepare.prir_client.between(f.round(vol_trend_lvl.avg_prir_client - tr * vol_trend_lvl.std_prir_client,6), f.round(vol_trend_lvl.avg_prir_client + tr * vol_trend_lvl.std_prir_client,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_txn", f.when(vol_trend_prepare.prir_txn.between(f.round(vol_trend_lvl.avg_prir_txn - tr * vol_trend_lvl.std_prir_txn,6), f.round(vol_trend_lvl.avg_prir_txn + tr * vol_trend_lvl.std_prir_txn,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_ssn", f.when(vol_trend_prepare.prir_ssn.between(f.round(vol_trend_lvl.avg_prir_ssn - tr * vol_trend_lvl.std_prir_ssn,6), f.round(vol_trend_lvl.avg_prir_ssn + tr * vol_trend_lvl.std_prir_ssn,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_step_", f.lit(s))\
            .select(
                vol_trend_prepare.whs_id,
                vol_trend_prepare.art_id,
                vol_trend_prepare.grp_art_id, 
                vol_trend_prepare.brand,
                vol_trend_prepare.gs_fo_id, 
                vol_trend_prepare.frmt_id, 
                vol_trend_prepare.algorithm_type,
                vol_trend_prepare.aum_type_id, 
                vol_trend_prepare.scnd_holiday_id, 
                vol_trend_prepare.direction_type, 
                vol_trend_prepare.disc,
                vol_trend_prepare.skidgr,
                vol_trend_prepare.prir_sale,
                vol_trend_prepare.prir_client,
                vol_trend_prepare.prir_txn,
                vol_trend_prepare.prir_ssn,
                f.col("flag_sale"),
                f.col("flag_client"),
                f.col("flag_txn"),
                f.col("flag_ssn"),
                f.col("flag_step_").alias("flag_step")
            )
        
            df_temp_vol_trend_prepare.repartition(cnt_part_imp).write.format("parquet").mode("append").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare")
            vol_trend_prepare = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare').repartition("whs_id")
            
            #print("vol_trend_prepare_temp = " , df_temp_vol_trend_prepare.count())
            
            s+=1
            
            #-------------end while s<= p_max_s
        
        vol_trend = vol_trend_prepare \
        .where( vol_trend_prepare.flag_step == f.lit(p_max_s))\
        .withColumn("prir_sale_discr", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_sale_", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.prir_sale).otherwise(None))\
        .withColumn("prir_sale_cnt", f.when( vol_trend_prepare.flag_sale == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_client_discr", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_client_", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.prir_client).otherwise(None))\
        .withColumn("prir_client_cnt", f.when( vol_trend_prepare.flag_client == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_txn_discr", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_txn_", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.prir_txn).otherwise(None))\
        .withColumn("prir_txn_cnt", f.when( vol_trend_prepare.flag_txn == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_ssn_discr", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_ssn_", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.prir_ssn).otherwise(None))\
        .withColumn("prir_ssn_cnt", f.when( vol_trend_prepare.flag_ssn == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .groupBy(
             vol_trend_prepare.skidgr        
            ,vol_trend_prepare.grp_art_id  
            ,vol_trend_prepare.brand
            ,vol_trend_prepare.gs_fo_id       
            ,vol_trend_prepare.frmt_id      
            ,vol_trend_prepare.algorithm_type  
            ,vol_trend_prepare.aum_type_id    
            ,vol_trend_prepare.scnd_holiday_id 
            ,vol_trend_prepare.direction_type
            )\
        .agg(
            f.avg(f.col("prir_sale_discr")).alias("av_disc_prir_sale"),
            f.stddev_pop(f.col("prir_sale_")).alias("std_prir_sale"),
            f.sum(f.col("prir_sale_cnt")).alias("cnt_prir_sale"),
            f.avg(f.col("prir_sale_")).alias("avg_prir_sale"),
            f.avg(f.col("prir_client_discr")).alias("av_disc_prir_client"),
            f.stddev_pop(f.col("prir_client_")).alias("std_prir_client"),
            f.sum(f.col("prir_client_cnt")).alias("cnt_prir_client"),
            f.avg(f.col("prir_client_")).alias("avg_prir_client"),    
            f.avg(f.col("prir_txn_discr")).alias("av_disc_prir_txn"),
            f.stddev_pop(f.col("prir_txn_")).alias("std_prir_txn"),
            f.sum(f.col("prir_txn_cnt")).alias("cnt_prir_txn"),
            f.avg(f.col("prir_txn_")).alias("avg_prir_txn"),  
            f.avg(f.col("prir_ssn_discr")).alias("av_disc_prir_ssn"),
            f.stddev_pop(f.col("prir_ssn_")).alias("std_prir_ssn"),
            f.sum(f.col("prir_ssn_cnt")).alias("cnt_prir_ssn1"),
            f.avg(f.col("prir_ssn_")).alias("avg_prir_ssn")    
            )   
        
        vol_trend.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend")
        vol_trend = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend').repartition("grp_art_id")
        
        #print("vol_trend = " , vol_trend.count())
            
        i = 1
    
        while i>0:
            
            t_elasticity_raw_fltr_step1 = vol_trend \
            .withColumn("av_disc", f.expr(f"case when ({i} = 1) then av_disc_prir_sale when ({i} = 2) then av_disc_prir_client when ({i} = 3) then av_disc_prir_txn else av_disc_prir_ssn end"))\
            .withColumn("prir1", f.expr(f"case when ({i} = 1) then avg_prir_sale when ({i} = 2) then avg_prir_client when ({i} = 3) then avg_prir_txn else avg_prir_ssn end"))\
            .withColumn("stddev_koeff", f.expr(f"case when ({i} = 1) then std_prir_sale when ({i} = 2) then std_prir_client when ({i} = 3) then std_prir_txn else std_prir_ssn end"))\
            .withColumn("cnt_int_rows", f.expr(f"case when ({i} = 1) then cnt_prir_sale when ({i} = 2) then cnt_prir_client when ({i} = 3) then cnt_prir_txn else cnt_prir_ssn1 end"))\
            .where(f"(round(stddev_koeff / (prir1 + 1.0) ,6) < 0.50 or cnt_int_rows >= 200) and cnt_int_rows >= 10")
            
            window_row_num = Window.partitionBy(t_elasticity_raw_fltr_step1.grp_art_id,t_elasticity_raw_fltr_step1.brand,t_elasticity_raw_fltr_step1.gs_fo_id,t_elasticity_raw_fltr_step1.frmt_id,t_elasticity_raw_fltr_step1.algorithm_type,t_elasticity_raw_fltr_step1.aum_type_id,t_elasticity_raw_fltr_step1.scnd_holiday_id,t_elasticity_raw_fltr_step1.direction_type)
            
            df_elasticity_raw_fltr = t_elasticity_raw_fltr_step1 \
            .withColumn("subfrmt_id", f.lit(-1))\
            .withColumn("type_prirost", f.lit(i))\
            .withColumn("skidgr_", t_elasticity_raw_fltr_step1.skidgr.cast("decimal(18,6)") / 10)\
            .withColumn("wo_dmp", f.lit(p_wo_dmp))\
            .withColumn("where_count_partition", f.count(f.lit(1)).over(window_row_num))\
            .withColumn("where_max_partition", f.max(t_elasticity_raw_fltr_step1.skidgr).over(window_row_num))\
            .withColumn("where_min_partition", f.min(t_elasticity_raw_fltr_step1.skidgr).over(window_row_num))\
            .where(f"(where_count_partition > 3) and (where_max_partition-where_min_partition >200)")\
            .select(
                t_elasticity_raw_fltr_step1.grp_art_id,
                t_elasticity_raw_fltr_step1.brand,
                t_elasticity_raw_fltr_step1.gs_fo_id,
                t_elasticity_raw_fltr_step1.frmt_id,
                t_elasticity_raw_fltr_step1.algorithm_type,
                f.col("subfrmt_id"),
                t_elasticity_raw_fltr_step1.aum_type_id,
                t_elasticity_raw_fltr_step1.scnd_holiday_id,
                t_elasticity_raw_fltr_step1.direction_type,
                f.col("type_prirost"),
                f.col("skidgr_").alias("skidgr"),
                t_elasticity_raw_fltr_step1.av_disc,
                t_elasticity_raw_fltr_step1.prir1.alias("prir"),
                t_elasticity_raw_fltr_step1.stddev_koeff,
                t_elasticity_raw_fltr_step1.cnt_int_rows,
                f.col("wo_dmp")    
                )
        
            df_elasticity_raw_fltr.repartition(cnt_part_imp).write.format("parquet").mode("append").save("/data/inc/olifir_pg/elasticity/df_elasticity_raw_fltr") 
    
            print("i = " , i)
        #print("df_elasticity_raw_fltr = " , df_elasticity_raw_fltr.count())
        
            i -= 1
        
        p_bitmap-=1
     
    p_brand-=1    
    print("p_brand = " , p_brand)
    
    

#пересчитываем связки
df_gr_complete = spark.read.format('parquet').load("/data/inc/olifir_pg/elasticity/df_elasticity_raw_fltr").where(f"wo_dmp = 1 and brand = '-1' ").select(f.col("grp_art_id").alias("art_grp_lvl_2_id") ,f.col("wo_dmp").alias("is_delete") ).distinct()

vol_grp_art = df_art_ext \
.join(df_gr_complete, ["art_grp_lvl_2_id"], how = "left") \
.withColumn("direction_type", f.when(df_art_ext.direction.isin(3,4), f.lit(34)).otherwise(f.lit(12)))\
.withColumn("brand", f.lit("-1"))\
.where(f"is_delete is null")\
.select(
        df_art_ext.art_id,
        df_art_ext.art_grp_lvl_2_id.alias("grp_art_id"),
        f.col("direction_type"),
        f.col("brand")
    ).distinct()
    
df_rep_elasticity = spark.read.format('delta').load('/data/base/d_rep_elasticity').where(f"scnd_sale_qnty > 0 and scnd_client_cnt > 0 and scnd_txn_cnt > 0 and scnd_price > 0 and frst_sale_qnty > 0 and frst_client_cnt > 0 and frst_txn_cnt > 0 and frst_price > 0 and scnd_day_cnt_rest_sales > 0 and (cast(scnd_price as float) / frst_price - 1 <= 0) and frst_begin_dt not between '2020-03-09' and '2020-05-03' and frst_end_dt not between '2020-03-09' and '2020-05-03' and scnd_begin_dt not between '2020-03-09' and '2020-05-03' and scnd_end_dt not between '2020-03-09' and '2020-05-03' and frst_begin_dt not between '2022-02-24' and '2022-03-22' and frst_end_dt not between '2022-02-24' and '2022-03-22' and scnd_begin_dt not between '2022-02-24' and '2022-03-22' and scnd_end_dt not between '2022-02-24' and '2022-03-22'")

p_date_calc = df_rep_elasticity \
 .join(f.broadcast(vol_grp_art), on = ["art_id"] , how = "inner")\
 .agg(f.max(df_rep_elasticity.scnd_begin_dt))

p_date_calc = p_date_calc.take(1)[0][0]
p_date_calc_begin = (p_date_calc - datetime.timedelta(days = (365*3) + 1)).strftime("%Y-%m-%d")

#COMPLETE VOL_TREND_PRIMARY
vol_trend_primary = df_rep_elasticity\
.join(f.broadcast(vol_grp_art), on = ["art_id"], how = "inner" )\
.where(f"scnd_begin_dt >= '{p_date_calc_begin}' ")\
.select (
           df_rep_elasticity.whs_id,
           df_rep_elasticity.art_id,
           df_rep_elasticity.algorithm_type,
           df_rep_elasticity.scnd_reason_type_id,
           df_rep_elasticity.scnd_holiday_id,
           df_rep_elasticity.frst_price,
           df_rep_elasticity.scnd_price,
           df_rep_elasticity.frst_sale_qnty,
           df_rep_elasticity.scnd_sale_qnty,
           df_rep_elasticity.frst_day_cnt_rest_sales,
           df_rep_elasticity.scnd_day_cnt_rest_sales,
           df_rep_elasticity.frst_client_cnt,
           df_rep_elasticity.scnd_client_cnt,
           df_rep_elasticity.frst_txn_cnt,
           df_rep_elasticity.scnd_txn_cnt,
           df_rep_elasticity.ssn_coef_sale_txn,
           df_rep_elasticity.frst_begin_dt,
           df_rep_elasticity.frst_end_dt,
           df_rep_elasticity.scnd_begin_dt,
           df_rep_elasticity.scnd_end_dt
    )

vol_trend_primary.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_primary")
vol_trend_primary = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_primary').repartition("whs_id") 
 
#обновляем тренд
vol_trend_temp = vol_trend_primary \
.join(vol_grp_art, on = ["art_id"], how = "inner")\
.join(vol_gs_fo_whs, on = ["whs_id"], how = "inner")\
.join(vol_dmp, on = ["whs_id","art_id","scnd_begin_dt","algorithm_type"], how = "left")\
.join(vol_dmp_prev, on = ["whs_id","art_id","frst_begin_dt","algorithm_type"], how = "left")\
.join(vol_rc, on = ["whs_id","art_id","scnd_begin_dt"], how = "left")\
.join(vol_rc_prev, on = ["whs_id","art_id","frst_begin_dt"], how = "left")\
.withColumn("aum_type_id" , f.when(vol_trend_primary.scnd_reason_type_id.isin(21001,24062) ,f.lit(1)).otherwise(f.when(vol_trend_primary.scnd_reason_type_id == f.lit(21007),f.lit(2)).otherwise(f.when(vol_trend_primary.scnd_reason_type_id == f.lit(21004),f.lit(3)).otherwise(f.lit(4)))))\
.withColumn("scnd_holiday_id",f.coalesce(vol_trend_primary.scnd_holiday_id,f.lit(0)))\
.withColumn("disc",  (vol_trend_primary.scnd_price.cast("decimal(18,6)")  /  vol_trend_primary.frst_price.cast("decimal(18,6)") - f.lit(1).cast("decimal(18,6)")).cast("decimal(18,6)"))\
.withColumn("skidgr",f.when(f.abs(f.round(f.col("disc"),5)) > f.lit(0.8), f.floor(f.round(f.col("disc") * 10,5)) * 100).otherwise(f.when(f.abs(f.round(f.col("disc"),5)) > f.lit(0.5), f.floor(f.round(f.col("disc") * 20,5)) * 50).otherwise(f.floor(f.round(f.col("disc") * 40,5)) * 25)) )\
.withColumn("prir_sale",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_day_cnt_rest_sales / vol_trend_primary.scnd_day_cnt_rest_sales) - 1.0)\
.withColumn("prir_client",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_client_cnt / vol_trend_primary.scnd_client_cnt) - 1.0 )\
.withColumn("prir_txn",(vol_trend_primary.scnd_sale_qnty / vol_trend_primary.frst_sale_qnty) * (vol_trend_primary.frst_txn_cnt / vol_trend_primary.scnd_txn_cnt)  - 1.0 )\
.withColumn("prir_ssn",(vol_trend_primary.scnd_sale_qnty / (vol_trend_primary.frst_sale_qnty * f.when(vol_trend_primary.ssn_coef_sale_txn == f.lit(0), None).otherwise(vol_trend_primary.ssn_coef_sale_txn))) - 1.0 )\
.where(f"prir_sale >=0 and is_vol_dmp is null and is_vol_dmp_prev is null and is_vol_rc is null and is_vol_rc_prev is null")\
.select(
    vol_trend_primary.whs_id,
    vol_trend_primary.art_id,
    vol_grp_art.grp_art_id,
    vol_grp_art.brand,
    vol_gs_fo_whs.gs_fo_id,
    vol_gs_fo_whs.frmt_id,
    vol_trend_primary.algorithm_type,
    f.col("aum_type_id"),
    f.col("scnd_holiday_id"),
    vol_grp_art.direction_type,
    f.col("disc"),
    f.col("skidgr"),
    f.col("prir_sale"),
    f.col("prir_client"),
    f.col("prir_txn"),
    f.col("prir_ssn")
    )
   
vol_trend_temp.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_temp")
vol_trend_temp = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_temp').repartition("whs_id")

#перезапускаем цикл
p_brand = 0

while p_brand >=0:
    print("p_brand = " , p_brand)

    p_bitmap = 3
    p_max_s = 4
    p_min_s = 1

    #добавить очистку
    subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare/".strip().split())
    subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl/".strip().split())


    while p_bitmap >=0:
        
        subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare/".strip().split())
        
        vol_trend_prepare = vol_trend_temp \
        .withColumn("gs_fo_id_", f.expr("case when " + getBit(p_bitmap,0)+" = 0 then -1 else gs_fo_id end"))\
        .withColumn("frmt_id_", f.expr("case when " + getBit(p_bitmap,1)+" = 0 then -1 else frmt_id end"))\
        .withColumn("aum_type_id", f.lit(-1))\
        .withColumn("scnd_holiday_id", f.lit(-1))\
        .withColumn("direction_type", f.lit(-1))\
        .withColumn("flag_sale", f.lit(1))\
        .withColumn("flag_client", f.lit(1))\
        .withColumn("flag_txn", f.lit(1))\
        .withColumn("flag_ssn", f.expr("case when prir_ssn is null then 0 else 1 end"))\
        .withColumn("flag_step", f.lit(0))\
        .withColumn("brand_", f.lit("-1"))\
        .select(
            vol_trend_temp.whs_id,
            vol_trend_temp.art_id,
            vol_trend_temp.grp_art_id, 
            f.col("brand_").alias("brand"), 
            f.col("gs_fo_id_").alias("gs_fo_id"), 
            f.col("frmt_id_").alias("frmt_id"),  
            vol_trend_temp.algorithm_type,
            f.col("aum_type_id"),
            f.col("scnd_holiday_id"),
            f.col("direction_type"),
            vol_trend_temp.disc,
            vol_trend_temp.skidgr, 
            vol_trend_temp.prir_sale,
            vol_trend_temp.prir_client,
            vol_trend_temp.prir_txn,
            vol_trend_temp.prir_ssn,
            f.col("flag_sale"),
            f.col("flag_client"),
            f.col("flag_txn"),
            f.col("flag_ssn"),
            f.col("flag_step")
            )
        
        vol_trend_prepare.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare")
        vol_trend_prepare = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare').repartition("whs_id")
    
        print("p_bitmap = " , p_bitmap)
        #print("vol_trend_prepare = " , vol_trend_prepare.count())
    
        s = p_min_s
    
        while s<= p_max_s:
            print("s = " , s)
        
            if (s == 1):
                tr = 3.0
            elif (s == 2):
                tr = 2.5
            elif (s == 3):
                tr = 2.0
            elif (s == 4):
                tr = 2.0
            
            subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl/".strip().split())
            
            vol_trend_lvl = vol_trend_prepare \
            .where( vol_trend_prepare.flag_step == f.lit(s - 1))\
            .withColumn("prir_sale_", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.prir_sale).otherwise(None))\
            .withColumn("prir_client_", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.prir_client).otherwise(None))\
            .withColumn("prir_txn_", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.prir_txn).otherwise(None))\
            .withColumn("prir_ssn_", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.prir_ssn).otherwise(None))\
            .groupBy(
                 vol_trend_prepare.skidgr        
                ,vol_trend_prepare.grp_art_id  
                ,vol_trend_prepare.brand
                ,vol_trend_prepare.gs_fo_id       
                ,vol_trend_prepare.frmt_id      
                ,vol_trend_prepare.algorithm_type  
                ,vol_trend_prepare.aum_type_id    
                ,vol_trend_prepare.scnd_holiday_id 
                ,vol_trend_prepare.direction_type
                )\
            .agg(
                f.avg(f.col("prir_sale_")).alias("avg_prir_sale"),
                f.stddev_pop(f.col("prir_sale_")).alias("std_prir_sale"),
                f.avg(f.col("prir_client_")).alias("avg_prir_client"),
                f.stddev_pop(f.col("prir_client_")).alias("std_prir_client"),    
                f.avg(f.col("prir_txn_")).alias("avg_prir_txn"),
                f.stddev_pop(f.col("prir_txn_")).alias("std_prir_txn"),
                f.avg(f.col("prir_ssn_")).alias("avg_prir_ssn"),
                f.stddev_pop(f.col("prir_ssn_")).alias("std_prir_ssn")
                )
    
            vol_trend_lvl.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl")
            vol_trend_lvl = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_lvl').repartition("grp_art_id")
            
            #print("vol_trend_lvl = " , vol_trend_lvl.count())
            
            df_temp_vol_trend_prepare = vol_trend_prepare \
            .join(vol_trend_lvl, on = ["skidgr","grp_art_id","brand","gs_fo_id","frmt_id","algorithm_type","aum_type_id","scnd_holiday_id","direction_type"], how = "inner")\
            .where(vol_trend_prepare.flag_step == f.lit(s - 1))\
            .withColumn("flag_sale", f.when(vol_trend_prepare.prir_sale.between(f.round(vol_trend_lvl.avg_prir_sale - tr * vol_trend_lvl.std_prir_sale,6), f.round(vol_trend_lvl.avg_prir_sale + tr * vol_trend_lvl.std_prir_sale,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_client", f.when(vol_trend_prepare.prir_client.between(f.round(vol_trend_lvl.avg_prir_client - tr * vol_trend_lvl.std_prir_client,6), f.round(vol_trend_lvl.avg_prir_client + tr * vol_trend_lvl.std_prir_client,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_txn", f.when(vol_trend_prepare.prir_txn.between(f.round(vol_trend_lvl.avg_prir_txn - tr * vol_trend_lvl.std_prir_txn,6), f.round(vol_trend_lvl.avg_prir_txn + tr * vol_trend_lvl.std_prir_txn,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_ssn", f.when(vol_trend_prepare.prir_ssn.between(f.round(vol_trend_lvl.avg_prir_ssn - tr * vol_trend_lvl.std_prir_ssn,6), f.round(vol_trend_lvl.avg_prir_ssn + tr * vol_trend_lvl.std_prir_ssn,6)), f.lit(1)).otherwise(f.lit(0)))\
            .withColumn("flag_step_", f.lit(s))\
            .select(
                vol_trend_prepare.whs_id,
                vol_trend_prepare.art_id,
                vol_trend_prepare.grp_art_id, 
                vol_trend_prepare.brand,
                vol_trend_prepare.gs_fo_id, 
                vol_trend_prepare.frmt_id, 
                vol_trend_prepare.algorithm_type,
                vol_trend_prepare.aum_type_id, 
                vol_trend_prepare.scnd_holiday_id, 
                vol_trend_prepare.direction_type, 
                vol_trend_prepare.disc,
                vol_trend_prepare.skidgr,
                vol_trend_prepare.prir_sale,
                vol_trend_prepare.prir_client,
                vol_trend_prepare.prir_txn,
                vol_trend_prepare.prir_ssn,
                f.col("flag_sale"),
                f.col("flag_client"),
                f.col("flag_txn"),
                f.col("flag_ssn"),
                f.col("flag_step_").alias("flag_step")
            )
        
            df_temp_vol_trend_prepare.repartition(cnt_part_imp).write.format("parquet").mode("append").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare")
            vol_trend_prepare = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend_prepare').repartition("whs_id")
            
            #print("vol_trend_prepare_temp = " , df_temp_vol_trend_prepare.count())
            
            s+=1
            
            #-------------end while s<= p_max_s
        
        vol_trend = vol_trend_prepare \
        .where( vol_trend_prepare.flag_step == f.lit(p_max_s))\
        .withColumn("prir_sale_discr", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_sale_", f.when( vol_trend_prepare.flag_sale == f.lit(1), vol_trend_prepare.prir_sale).otherwise(None))\
        .withColumn("prir_sale_cnt", f.when( vol_trend_prepare.flag_sale == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_client_discr", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_client_", f.when( vol_trend_prepare.flag_client == f.lit(1), vol_trend_prepare.prir_client).otherwise(None))\
        .withColumn("prir_client_cnt", f.when( vol_trend_prepare.flag_client == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_txn_discr", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_txn_", f.when( vol_trend_prepare.flag_txn == f.lit(1), vol_trend_prepare.prir_txn).otherwise(None))\
        .withColumn("prir_txn_cnt", f.when( vol_trend_prepare.flag_txn == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .withColumn("prir_ssn_discr", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.disc).otherwise(None))\
        .withColumn("prir_ssn_", f.when( vol_trend_prepare.flag_ssn == f.lit(1), vol_trend_prepare.prir_ssn).otherwise(None))\
        .withColumn("prir_ssn_cnt", f.when( vol_trend_prepare.flag_ssn == f.lit(1), f.lit(1)).otherwise(f.lit(0)))\
        .groupBy(
             vol_trend_prepare.skidgr        
            ,vol_trend_prepare.grp_art_id  
            ,vol_trend_prepare.brand
            ,vol_trend_prepare.gs_fo_id       
            ,vol_trend_prepare.frmt_id      
            ,vol_trend_prepare.algorithm_type  
            ,vol_trend_prepare.aum_type_id    
            ,vol_trend_prepare.scnd_holiday_id 
            ,vol_trend_prepare.direction_type
            )\
        .agg(
            f.avg(f.col("prir_sale_discr")).alias("av_disc_prir_sale"),
            f.stddev_pop(f.col("prir_sale_")).alias("std_prir_sale"),
            f.sum(f.col("prir_sale_cnt")).alias("cnt_prir_sale"),
            f.avg(f.col("prir_sale_")).alias("avg_prir_sale"),
            f.avg(f.col("prir_client_discr")).alias("av_disc_prir_client"),
            f.stddev_pop(f.col("prir_client_")).alias("std_prir_client"),
            f.sum(f.col("prir_client_cnt")).alias("cnt_prir_client"),
            f.avg(f.col("prir_client_")).alias("avg_prir_client"),    
            f.avg(f.col("prir_txn_discr")).alias("av_disc_prir_txn"),
            f.stddev_pop(f.col("prir_txn_")).alias("std_prir_txn"),
            f.sum(f.col("prir_txn_cnt")).alias("cnt_prir_txn"),
            f.avg(f.col("prir_txn_")).alias("avg_prir_txn"),  
            f.avg(f.col("prir_ssn_discr")).alias("av_disc_prir_ssn"),
            f.stddev_pop(f.col("prir_ssn_")).alias("std_prir_ssn"),
            f.sum(f.col("prir_ssn_cnt")).alias("cnt_prir_ssn1"),
            f.avg(f.col("prir_ssn_")).alias("avg_prir_ssn")    
            )   
        
        vol_trend.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/p_df_vol_trend")
        vol_trend = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/p_df_vol_trend').repartition("grp_art_id")
        
        #print("vol_trend = " , vol_trend.count())
            
        i = 1
    
        while i>0:
            
            t_elasticity_raw_fltr_step1 = vol_trend \
            .withColumn("av_disc", f.expr(f"case when ({i} = 1) then av_disc_prir_sale when ({i} = 2) then av_disc_prir_client when ({i} = 3) then av_disc_prir_txn else av_disc_prir_ssn end"))\
            .withColumn("prir1", f.expr(f"case when ({i} = 1) then avg_prir_sale when ({i} = 2) then avg_prir_client when ({i} = 3) then avg_prir_txn else avg_prir_ssn end"))\
            .withColumn("stddev_koeff", f.expr(f"case when ({i} = 1) then std_prir_sale when ({i} = 2) then std_prir_client when ({i} = 3) then std_prir_txn else std_prir_ssn end"))\
            .withColumn("cnt_int_rows", f.expr(f"case when ({i} = 1) then cnt_prir_sale when ({i} = 2) then cnt_prir_client when ({i} = 3) then cnt_prir_txn else cnt_prir_ssn1 end"))\
            .where(f"(round(stddev_koeff / (prir1 + 1.0) ,3) < 0.50 or cnt_int_rows >= 200) and cnt_int_rows >= 10")
            
            window_row_num = Window.partitionBy(t_elasticity_raw_fltr_step1.grp_art_id,t_elasticity_raw_fltr_step1.brand,t_elasticity_raw_fltr_step1.gs_fo_id,t_elasticity_raw_fltr_step1.frmt_id,t_elasticity_raw_fltr_step1.algorithm_type,t_elasticity_raw_fltr_step1.aum_type_id,t_elasticity_raw_fltr_step1.scnd_holiday_id,t_elasticity_raw_fltr_step1.direction_type)
            
            df_elasticity_raw_fltr = t_elasticity_raw_fltr_step1 \
            .withColumn("subfrmt_id", f.lit(-1))\
            .withColumn("type_prirost", f.lit(i))\
            .withColumn("skidgr_", t_elasticity_raw_fltr_step1.skidgr.cast("decimal(18,6)") / 10)\
            .withColumn("wo_dmp", f.lit(p_wo_dmp))\
            .withColumn("where_count_partition", f.count(f.lit(1)).over(window_row_num))\
            .withColumn("where_max_partition", f.max(t_elasticity_raw_fltr_step1.skidgr).over(window_row_num))\
            .withColumn("where_min_partition", f.min(t_elasticity_raw_fltr_step1.skidgr).over(window_row_num))\
            .where(f"(where_count_partition > 3) and (where_max_partition-where_min_partition >200)")\
            .select(
                t_elasticity_raw_fltr_step1.grp_art_id,
                t_elasticity_raw_fltr_step1.brand,
                t_elasticity_raw_fltr_step1.gs_fo_id,
                t_elasticity_raw_fltr_step1.frmt_id,
                t_elasticity_raw_fltr_step1.algorithm_type,
                f.col("subfrmt_id"),
                t_elasticity_raw_fltr_step1.aum_type_id,
                t_elasticity_raw_fltr_step1.scnd_holiday_id,
                t_elasticity_raw_fltr_step1.direction_type,
                f.col("type_prirost"),
                f.col("skidgr_").alias("skidgr"),
                t_elasticity_raw_fltr_step1.av_disc,
                t_elasticity_raw_fltr_step1.prir1.alias("prir"),
                t_elasticity_raw_fltr_step1.stddev_koeff,
                t_elasticity_raw_fltr_step1.cnt_int_rows,
                f.col("wo_dmp")    
                )
        
            df_elasticity_raw_fltr.repartition(cnt_part_imp).write.format("parquet").mode("append").save("/data/inc/olifir_pg/elasticity/df_elasticity_raw_fltr") 
    
            print("i = " , i)
        #print("df_elasticity_raw_fltr = " , df_elasticity_raw_fltr.count())
        
            i -= 1
        
        p_bitmap-=1
     
    p_brand-=1    
    print("p_brand = " , p_brand)


df_elasticity_raw_fltr = spark.read.format('parquet').load('/data/inc/olifir_pg/elasticity/df_elasticity_raw_fltr')
 
df_grp_art_hier = spark.read.format('delta').load('/data/base/d_grp_art_hier').persist()

p_max_points = 15000.0
#p_wo_dmp = 1

window_row_num = Window.partitionBy(df_elasticity_raw_fltr.grp_art_id,df_elasticity_raw_fltr.brand,df_elasticity_raw_fltr.gs_fo_id,df_elasticity_raw_fltr.frmt_id,df_elasticity_raw_fltr.algorithm_type,df_elasticity_raw_fltr.aum_type_id,df_elasticity_raw_fltr.scnd_holiday_id,df_elasticity_raw_fltr.direction_type, df_elasticity_raw_fltr.type_prirost, df_elasticity_raw_fltr.wo_dmp)
        
df_t_elasticity_trend_final = df_elasticity_raw_fltr \
.join(df_grp_art_hier, ["grp_art_id"], how = "inner") \
.where(df_elasticity_raw_fltr.wo_dmp == f.lit(p_wo_dmp))\
.withColumn("where_row_number", f.row_number().over(window_row_num.orderBy(df_elasticity_raw_fltr.grp_art_id,df_elasticity_raw_fltr.brand))) \
.withColumn("ln_pr", f.log(math.e,df_elasticity_raw_fltr.prir + f.lit(1))) \
.withColumn("skidgr1", df_elasticity_raw_fltr.skidgr.cast("decimal(18,6)") / f.lit(100.0)) \
.withColumn("max_cnt_rows", f.max(df_elasticity_raw_fltr.cnt_int_rows).over(window_row_num)) \
.withColumn("weight", f.when(f.col("max_cnt_rows") >= f.lit(15), f.least(f.lit(1),df_elasticity_raw_fltr.cnt_int_rows.cast("decimal(18,6)") / f.lit(p_max_points) )).otherwise(df_elasticity_raw_fltr.cnt_int_rows.cast("decimal(18,6)") / f.least(f.col("max_cnt_rows"), f.lit(p_max_points)))) \
.withColumn("lny_wx", f.log(math.e,df_elasticity_raw_fltr.prir + f.lit(1.0) ) * f.col("weight") * f.col("skidgr1")) \
.withColumn("lny_w", f.log(math.e,df_elasticity_raw_fltr.prir + f.lit(1.0) ) * f.col("weight")) \
.withColumn("w_x", f.col("weight") * f.col("skidgr1")) \
.withColumn("w_x2", f.col("weight") * (f.col("skidgr1") * f.col("skidgr1"))) \
.withColumn("disc_group_cnt", f.count(f.lit(1)).over(window_row_num))\
.withColumn("min_discount", f.min(df_elasticity_raw_fltr.av_disc).over(window_row_num))\
.withColumn("max_discount", f.max(df_elasticity_raw_fltr.av_disc).over(window_row_num))\
.withColumn("avg_stddev", f.avg(df_elasticity_raw_fltr.stddev_koeff).over(window_row_num))\
.withColumn("max_stddev", f.max(df_elasticity_raw_fltr.stddev_koeff).over(window_row_num))\
.withColumn("sum_cnt_int_rows", f.sum(df_elasticity_raw_fltr.cnt_int_rows).over(window_row_num))\
.withColumn("min_cnt_int_rows", f.min(df_elasticity_raw_fltr.cnt_int_rows).over(window_row_num))\
.withColumn("sum_lny_wx", f.sum(f.col("lny_wx")).over(window_row_num))\
.withColumn("sum_s_w", f.sum(f.col("weight")).over(window_row_num))\
.withColumn("sum_lny_w", f.sum(f.col("lny_w")).over(window_row_num))\
.withColumn("sum_w_x", f.sum(f.col("w_x")).over(window_row_num))\
.withColumn("sum_w_x2", f.sum(f.col("w_x2")).over(window_row_num))\
.withColumn("exp_trend_b", -((f.col("sum_lny_wx") * f.col("sum_s_w")) - (f.col("sum_lny_w") * f.col("sum_w_x"))) / ((f.col("sum_w_x2") * f.col("sum_s_w")) - (f.col("sum_w_x") * f.col("sum_w_x") ))) \
.withColumn("exp_trend_a",((f.col("exp_trend_b") * f.col("sum_w_x")) + f.col("sum_lny_w")  ) / f.col("sum_s_w") ) \
.withColumn("subfrmt_id", f.lit(-1))\
.withColumn("art_grp_lvl", df_grp_art_hier.lvl)\
.select(
        f.col("art_grp_lvl"),
        df_elasticity_raw_fltr.grp_art_id, 
        df_elasticity_raw_fltr.brand,
        df_elasticity_raw_fltr.gs_fo_id, 
        df_elasticity_raw_fltr.frmt_id, 
        df_elasticity_raw_fltr.algorithm_type,
        f.col("subfrmt_id"),
        df_elasticity_raw_fltr.aum_type_id, 
        df_elasticity_raw_fltr.scnd_holiday_id, 
        df_elasticity_raw_fltr.direction_type , 
        df_elasticity_raw_fltr.type_prirost,
        f.col("disc_group_cnt"),
        f.col("min_discount"),
        f.col("max_discount"),
        f.col("avg_stddev"),
        f.col("max_stddev"),
        f.col("sum_cnt_int_rows"),
        f.col("min_cnt_int_rows"),
        f.col("exp_trend_a"),
        f.col("exp_trend_b"),
        df_elasticity_raw_fltr.wo_dmp        
    ).where(f.col("where_row_number") == f.lit(1))
 

df_t_elasticity_trend_final.repartition(cnt_part_imp).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/elasticity/df_t_elasticity_trend_final")   

print("Собрали df_t_elasticity_trend_final")

df_art_ext = spark.read.format('delta').load('/data/base/d_art_ext').persist()
 
#subprocess.call("hdfs dfs -rm -r /data/inc/olifir_pg/elasticity/df_elasticity_art_cluster".strip().split())

df_art_ext = df_art_ext\
.withColumn("check_brand", f.expr("replace(replace(replace(replace(replace(lower(art_grp_2_lvl_1_name), 'c', 'с'), ' ооо', ''), ' оао', ''), ' кк', ''), ' мк', '')"))\

df_t_elasticity_trend_final = spark.read.format('parquet').load("/data/inc/olifir_pg/elasticity/df_t_elasticity_trend_final").where(f"gs_fo_id = -1 and algorithm_type = 1 and round(exp_trend_b, 3) >= 0 and wo_dmp = 1")

df_t_elasticity_trend_final = df_t_elasticity_trend_final \
.withColumn("sort_id", f.expr(f"case when brand = '-1' then 1 else 0 end"))

window_row_num = Window.partitionBy(df_t_elasticity_trend_final.wo_dmp, df_art_ext.art_id, df_t_elasticity_trend_final.frmt_id).orderBy(df_t_elasticity_trend_final.art_grp_lvl.desc(), df_t_elasticity_trend_final.sort_id)

df_elasticity_art_cluster = df_art_ext \
.join(df_t_elasticity_trend_final, on = [(((df_t_elasticity_trend_final.grp_art_id == df_art_ext.art_grp_lvl_3_id) & (df_t_elasticity_trend_final.art_grp_lvl == 3)) | ((df_t_elasticity_trend_final.grp_art_id == df_art_ext.art_grp_lvl_2_id) & (df_t_elasticity_trend_final.art_grp_lvl == 2)) | ((df_t_elasticity_trend_final.grp_art_id == df_art_ext.art_grp_lvl_1_id) & (df_t_elasticity_trend_final.art_grp_lvl == 1)) | ((df_t_elasticity_trend_final.grp_art_id == df_art_ext.art_grp_lvl_0_id) & (df_t_elasticity_trend_final.art_grp_lvl == 0))) & (((f.col("check_brand") == df_t_elasticity_trend_final.brand) | (df_t_elasticity_trend_final.brand == f.lit("-1"))))], how = "inner")\
.withColumn("coef", (f.exp(df_t_elasticity_trend_final.exp_trend_a + df_t_elasticity_trend_final.exp_trend_b * f.lit(0.2))).cast("float"))\
.withColumn("coef_dmp", (f.when(f.lit(1) == f.lit(0), f.lit(0.1)).otherwise(None)).cast("float"))\
.withColumn("num_order", f.rank().over(window_row_num))\
.where((f.col("num_order") == f.lit(1)))\
.select (
		df_t_elasticity_trend_final.wo_dmp,
		df_art_ext.art_id,
		df_t_elasticity_trend_final.frmt_id,
		df_t_elasticity_trend_final.art_grp_lvl,
		df_t_elasticity_trend_final.grp_art_id,
		df_t_elasticity_trend_final.brand,
		df_t_elasticity_trend_final.exp_trend_a.cast("float"),
		df_t_elasticity_trend_final.exp_trend_b.cast("float"),   
		f.col("coef"),
		f.col("coef_dmp"),
		f.col("num_order")
   )

df_elasticity_art_cluster.repartition(cnt_part_imp).write.format("delta").mode("overwrite").save("/data/base/d_elasticity_art_cluster")
delta_table3 = DeltaTable.forPath(spark,"/data/base/d_elasticity_art_cluster")
delta_table3.vacuum(0)   

print("Собрали df_elasticity_art_cluster")    