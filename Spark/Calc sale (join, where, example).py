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

#Параметры расчета

#Даты начала и конца расчета
p_begdt = (datetime.datetime.today() - datetime.timedelta(days = 84)).strftime("%Y-%m-%d")
p_enddt = (datetime.datetime.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")
p_snapshot_type = 2

#Форматы и группы (ограничения задавать тут)
p_frmt = 'frmt_id in (2)'
p_gr20 = 'art_grp_lvl_0_id not in (1876,535,1916,17415,115,637,1266,1561,14257,1009,17191,16864,17241,417,367,1298,1594,1268)'

#формируем справочники для расчета
df_whs = spark.read.format('delta').load('/data/base/d_whs').persist()
df_whs = df_whs.where(f"{p_frmt}")

df_art_ext = spark.read.format('delta').load('/data/base/d_art_ext').persist()
df_art_ext = df_art_ext.where(f"{p_gr20}" )

#Формируем vt_whs_art_day_rc
df_rc_calendar = spark.read.format('delta').load('/data/base/d_rc_calendar')
df_day = spark.read.format('delta').load('/data/base/d_days').persist()

df_rc_calendar = df_rc_calendar.where(f"beg_dt <= '{p_enddt}' and end_dt >= '{p_begdt}' and market_company_id = 1").select("whs_id","art_id","beg_dt", "end_dt").distinct()
df_day = df_day.where(f"day_id between '{p_begdt}' and '{p_enddt}' ")

df_vt_whs_art_day_rc = df_rc_calendar.join(df_day, ((df_day.day_id>=df_rc_calendar.beg_dt) & (df_day.day_id<=df_rc_calendar.end_dt)), how="inner").select("whs_id","art_id","day_id").distinct()

#Формируем входные данные T_V_INPUT_DATA

df_sale_rest = spark.read.format('delta').load('/data/base/d_sale_rest_day')
df_sale_rest = df_sale_rest.where(f" (day_id between '{p_begdt}' and '{p_enddt}')  and (coalesce(rest_qnty,0) >0 or coalesce(sales_qnty,0) >0 )")

df_sale_rest_84 = df_sale_rest\
.join(df_whs, on = ["whs_id"], how = "inner")\
.join(df_art_ext, on = ["art_id"], how = "inner")\
.join(df_vt_whs_art_day_rc, on = ["whs_id", "art_id", "day_id"], how = "left").filter(df_vt_whs_art_day_rc.whs_id.isNotNull())\
.groupBy(df_sale_rest.whs_id, df_sale_rest.art_id)\
.agg(f.countDistinct("day_id").alias("days_wsr_84"))

df_whs_cluster = spark.read.format('delta').load('/data/base/d_machine_forecast_whs_clusters')

df_t_v_input_data = df_sale_rest_84\
.filter(f.col("days_wsr_84") >= f.lit(14))\
.join(df_whs, on = ["whs_id"], how = "inner")\
.join(df_art_ext, on = ["art_id"], how = "inner")\
.join(df_whs_cluster, on = ["whs_id"], how = "left")\
.select(
            df_sale_rest_84.whs_id,
            df_sale_rest_84.art_id,
            df_whs.frmt_id,            
            df_whs.branch_id,
            f.coalesce(df_whs_cluster.cluster_id,f.lit(1)).alias("cluster_id"),
            df_art_ext.art_grp_lvl_3_id
        )

# Формируем T_V_ELAST  

df_elast_art_cluster = spark.read.format('delta').load('/data/base/d_elasticity_art_cluster').repartition("art_id")
df_elast_art_cluster_wo_frmt = spark.read.format("delta").load("/data/base/d_elasticity_art_cluster").where("frmt_id = -1").repartition("art_id")

window_row_num = Window.partitionBy(df_t_v_input_data.art_id, df_t_v_input_data.frmt_id).orderBy("exp_trend_a_new","exp_trend_b_new")

df_t_v_elast = df_t_v_input_data\
.join(df_elast_art_cluster, on = ["art_id","frmt_id"], how = "inner")\
.join(df_elast_art_cluster_wo_frmt, on = ["art_id","frmt_id"], how = "inner")\
.withColumn("exp_trend_a_new", f.coalesce(df_elast_art_cluster.exp_trend_a, df_elast_art_cluster_wo_frmt.exp_trend_a))\
.withColumn("exp_trend_b_new", f.coalesce(df_elast_art_cluster.exp_trend_b, df_elast_art_cluster_wo_frmt.exp_trend_b))\
.withColumn("row_number",f.row_number().over(window_row_num))\
.select(
        df_t_v_input_data.art_id,
        df_t_v_input_data.frmt_id,
        f.col("exp_trend_a_new").alias("exp_trend_a"),
        f.col("exp_trend_b_new").alias("exp_trend_b")
       ).where("row_number=1")

# Обновляем эластичность в T_V_INPUT_DATA
        
df_t_v_input_data = df_t_v_input_data.join(df_t_v_elast, on = ["art_id", "frmt_id"], how = "left")\
.select(
    df_t_v_input_data.columns + 
    [
        df_t_v_elast.exp_trend_a,
        df_t_v_elast.exp_trend_b
    ]
    )

df_t_v_input_data = df_t_v_input_data.where(f"exp_trend_a is null or exp_trend_b is null")

#Обновляем праздничные коэффициенты T_V_HOLIDAY_DATA

df_machine_forecast_holidays_coef_clusters = spark.read.format("delta").load("/data/base/d_machine_forecast_holidays_coeff_clusters")

df_t_v_holaday_data = df_machine_forecast_holidays_coef_clusters\
.where(f"day_id between '{p_begdt}' and '{p_enddt}' ")\
.groupBy(
      df_machine_forecast_holidays_coef_clusters.frmt_id
    , df_machine_forecast_holidays_coef_clusters.branch_id
    , df_machine_forecast_holidays_coef_clusters.cluster_id
    , df_machine_forecast_holidays_coef_clusters.art_grp_lvl_3_id
    , df_machine_forecast_holidays_coef_clusters.day_id
    )\
.agg(f.exp(f.sum(df_machine_forecast_holidays_coef_clusters.koeff_holiday)).alias("coef_holiday"))  

#Сезонные коэффициенты T_V_SEASONALITY_DATA

df_machine_forecast_seasonality_coeff_clusters = spark.read.format("delta").load("/data/base/d_machine_forecast_seasonality_coeff_clusters")

df_day = df_day\
.withColumn("yearday_id", (df_day.month_number * 100) + df_day.day_month_number )

df_t_v_seasonality_data= df_machine_forecast_seasonality_coeff_clusters\
.join(df_day, on = ["yearday_id"], how = "inner")\
.groupBy(
      df_machine_forecast_seasonality_coeff_clusters.frmt_id
    , df_machine_forecast_seasonality_coeff_clusters.branch_id
    , df_machine_forecast_seasonality_coeff_clusters.cluster_id
    , df_machine_forecast_seasonality_coeff_clusters.art_grp_lvl_3_id
    , df_day.day_id
    )\
.agg(f.avg(f.when(f.exp(df_machine_forecast_seasonality_coeff_clusters.koef_yearly) < 0.5 , 0.5).otherwise(f.exp(df_machine_forecast_seasonality_coeff_clusters.koef_yearly))).alias("coef_season"))

#Формируем T_V_WHS_ART_ITER

df_whs_art_iter = df_t_v_input_data\
.groupBy(
      df_t_v_input_data.whs_id
    , df_t_v_input_data.art_id
    )\
.agg(f.max(df_t_v_input_data.frmt_id).alias("frmt_id")  
,f.max(df_t_v_input_data.branch_id).alias("branch_id")  
,f.max(df_t_v_input_data.cluster_id).alias("cluster_id")  
,f.max(df_t_v_input_data.art_grp_lvl_3_id).alias("art_grp_lvl_3_id"))  

#Собираем продажи и цена за 84 дня T_V_SALES_PRICE_TEMP

df_art_moreinfo = spark.read.format('delta').load('/data/base/d_art_moreinfo')

df_price_indicator_day = spark.read.format('delta').load('/data/base/d_price_indicator_day')
df_price_indicator_day = df_price_indicator_day.where(f" (day_id between '{p_begdt}' and '{p_enddt}') ") 

df_spa_priceonreason = spark.read.format('delta').load('/data/base/d_priceon_reason')

df_t_v_sales_price_temp = df_sale_rest\
.join(df_whs_art_iter, on = ["art_id", "whs_id"], how = "inner")\
.join(df_art_moreinfo, on = ["art_id"], how = "left")\
.join(df_price_indicator_day, on = ["art_id","whs_id","day_id"], how = "left")\
.join(df_spa_priceonreason, on = ["action_type_id"], how = "left")\
.join(df_vt_whs_art_day_rc, on = ["art_id","whs_id","day_id"], how = "left").filter(df_vt_whs_art_day_rc.whs_id.isNotNull())\
.withColumn("opsum", df_sale_rest.sale + f.coalesce(df_sale_rest.disc_loyal, f.lit(0)))\
.withColumn("is_monitor", f.when(df_spa_priceonreason.reason_type_id == f.lit(21004), f.lit(1)).otherwise(f.lit(0)))\
.withColumn("is_regular",f.when(f.coalesce(df_spa_priceonreason.reason_type_id, f.lit(24185)) == f.lit(24185), f.lit(1)).otherwise(f.lit(0)))\
.withColumn("is_txn_disc",f.lit(0))\
.withColumn("sale_reason_type",f.when(((f.coalesce(df_spa_priceonreason.reason_type_id, f.lit(24185)) == f.lit(24062)) | (f.coalesce(df_spa_priceonreason.reason_type_id, f.lit(24185)) == f.lit(21001)) ) & (f.lit(1) - df_price_indicator_day.factual_price / f.coalesce(df_price_indicator_day.regular_price, f.lit(0)) < f.lit(0.05)), f.lit(1)).otherwise(
 f.when((f.coalesce(df_spa_priceonreason.reason_type_id, f.lit(24185)) == f.lit(24185)) & (f.lit(1) - df_price_indicator_day.factual_price / f.coalesce(df_price_indicator_day.regular_price, f.lit(0)) >= f.lit(0.05)), f.lit(0)).otherwise(f.lit(2))))\
.select(
        df_sale_rest.day_id,
        df_sale_rest.whs_id,
        df_sale_rest.art_id,
        df_sale_rest.sales_qnty,
        f.col("opsum"),
        df_sale_rest.disc_ma_qnty.alias("discount_sum"),
        df_price_indicator_day.factual_price,
        df_price_indicator_day.regular_price,
        f.col("is_monitor"),
        f.col("is_regular"),
        f.col("is_txn_disc"),
        df_whs_art_iter.frmt_id,
        df_whs_art_iter.branch_id,
        df_whs_art_iter.cluster_id,
        df_whs_art_iter.art_grp_lvl_3_id,
        f.col("sale_reason_type")
   ).where(((df_sale_rest.rest_qnty > f.lit(0)) & (f.coalesce(df_art_moreinfo.is_sp_gm,f.lit(0)) == f.lit(0))) | (f.coalesce(df_sale_rest.sales_qnty,f.lit(0)) > f.lit(0)))

#Добавляем сезонность

df_t_v_sales_price_temp =  df_t_v_sales_price_temp.join(df_t_v_seasonality_data, on=["frmt_id", "branch_id", "cluster_id", "art_grp_lvl_3_id", "day_id"], how="left")\
.select(df_t_v_sales_price_temp.columns + [f.col("coef_season")])

#Добавляем праздники в жизнь нашу грешную

df_t_v_sales_price_temp =  df_t_v_sales_price_temp.join(df_t_v_holaday_data, on=["frmt_id", "branch_id", "cluster_id", "art_grp_lvl_3_id", "day_id"], how="left")\
.select(df_t_v_sales_price_temp.columns + [f.col("coef_holiday")])

#Определяем кол-во дней за период обучения

df_days_cnt = df_t_v_sales_price_temp\
.groupBy(
      df_t_v_sales_price_temp.whs_id
    , df_t_v_sales_price_temp.art_id
    )\
.agg(
 f.count(f.lit(1)).alias("days_cnt_all")  
,f.count(f.when(df_t_v_sales_price_temp.sale_reason_type == f.lit(0), f.lit(1))).alias("days_cnt_reg")  
,f.avg(f.when(df_t_v_sales_price_temp.sale_reason_type == f.lit(0), df_t_v_sales_price_temp.sales_qnty)).alias("sale_qnty_avg_reg") 
,f.count(f.when(df_t_v_sales_price_temp.sale_reason_type == f.lit(1), f.lit(1))).alias("days_cnt_promo")  
,f.avg(f.when(df_t_v_sales_price_temp.sale_reason_type == f.lit(1), df_t_v_sales_price_temp.sales_qnty)).alias("sale_qnty_avg_promo")
)  

df_days_cnt = df_days_cnt\
.withColumn("days_cnt", f.when((f.col("days_cnt_promo") >= 7) & (f.col("days_cnt_reg") >= 7) & (f.col("sale_qnty_avg_promo") >= f.col("sale_qnty_avg_reg")), f.col("days_cnt_promo")).otherwise(f.col("days_cnt_all")))\
.withColumn("to_use_only_promo", f.when((f.col("days_cnt_promo") >= 7) & (f.col("days_cnt_reg") >= 7) & (f.col("sale_qnty_avg_promo") >= f.col("sale_qnty_avg_reg")), f.lit(1)).otherwise(f.lit(0)))

df_t_v_sales_price_temp =  df_t_v_sales_price_temp.join(df_days_cnt, on=["whs_id","art_id"], how="left")\
.select(
         df_t_v_sales_price_temp.columns + [f.col("days_cnt")] + [f.col("to_use_only_promo")]
      )

# Удаление связок Магазин - Товар с DAYS_CNT < 14  
# 
df_t_v_sales_price_temp = df_t_v_sales_price_temp\
.where((df_t_v_sales_price_temp.days_cnt < f.when(f.col("to_use_only_promo") == f.lit(0), f.lit(14)).otherwise(f.lit(7)) ) | (f.lit(1) < f.when((f.col("to_use_only_promo") == f.lit(1)) & (f.col("sale_reason_type") != f.lit(1)), f.lit(1)).otherwise(f.lit(0))) )

#Чистые продажи и фактическая цена

df_t_v_sales_price_temp =  df_t_v_sales_price_temp\
.withColumn("sale_qnty_clean", f.col("sales_qnty") / f.coalesce(f.col("coef_holiday") , f.lit(1.0)) / f.coalesce(f.col("coef_season"), f.lit(1.0)))\
.withColumn("target_price", f.when(f.col("is_txn_disc") == f.lit(1), f.col("regular_price")).otherwise(f.when(f.col("sales_qnty") != f.lit(0), f.col("opsum") / f.col("sales_qnty")).otherwise(f.col("factual_price"))))

#Формируем финальную таблицу (предитог)

df_output_temp =  df_t_v_sales_price_temp\
.join(df_t_v_input_data, on = ["art_id", "whs_id"], how = "inner")\
.withColumn("regular_price_alg", f.lit(0))\
.withColumn("exp_trend_a_", f.when(f.col("exp_trend_a") > f.lit(35) , f.lit(35)).otherwise(f.when(f.col("exp_trend_a") < f.lit(-35) , f.lit(-35)).otherwise(f.col("exp_trend_a"))))\
.withColumn("exp_trend_b_", f.when(f.col("exp_trend_b") > f.lit(35) , f.lit(35)).otherwise(f.when(f.col("exp_trend_b") < f.lit(-35) , f.lit(-35)).otherwise(f.col("exp_trend_b"))))\
.withColumn("disc_calc_",f.when(f.col("regular_price") != f.lit(0), f.lit(1.0) / f.col("target_price") / f.col("regular_price")))\
.withColumn("disc_calc", f.when(f.col("disc_calc_") > f.lit(0), f.col("disc_calc_")).otherwise(f.lit(0)))\
.withColumn("exp_1_tmp", f.col("exp_trend_a_") + f.col("exp_trend_b_") * f.col("disc_calc"))\
.withColumn("exp_1", f.when(f.col("exp_1_tmp") > f.lit(35), f.lit(35)).otherwise(f.when(f.col("exp_1_tmp") < f.lit(-35), f.lit(-35)).otherwise(f.col("exp_1_tmp"))))\
.withColumn("exp_2_tmp", f.when((f.lit(1) - f.col("disc_calc")) != 0 , f.col("exp_trend_a_") - f.col("exp_trend_b_") * f.col("disc_calc") / (f.lit(1) - f.col("disc_calc"))))\
.withColumn("exp_2", f.when(f.col("exp_2_tmp") > f.lit(35), f.lit(35)).otherwise(f.when(f.col("exp_2_tmp") < f.lit(-35), f.lit(-35)).otherwise(f.col("exp_2_tmp"))))\
.withColumn("coef_growth_temp", f.when(f.col("disc_calc") >= f.lit(0), f.exp(f.col("exp_1")) - f.exp(f.col("exp_trend_a_")) + f.lit(1) ).otherwise(f.when((f.lit(1) + f.exp(f.col("exp_2")) - f.exp(f.col("exp_trend_a_"))) != 0 , f.lit(1) / (f.lit(1) + f.exp(f.col("exp_2")) - f.exp(f.col("exp_trend_a_"))))))\
.withColumn("coef_growth", f.when(f.col("coef_growth_temp") > f.lit(200), f.lit(200)).otherwise(f.col("coef_growth_temp")))\
.withColumn("qnty_reg", f.when(f.col("sale_qnty_clean") > f.lit(0), f.col("sale_qnty_clean")).otherwise(f.lit(0)) / f.when(f.col("coef_growth") != 0, f.col("coef_growth")))


df_output_temp = df_output_temp\
.where("qnty_reg >= 0")\
.groupBy(
      df_output_temp.whs_id
    , df_output_temp.art_id
    )\
.agg(f.avg(df_output_temp.regular_price).alias("regular_price")  
, f.min(df_output_temp.regular_price_alg).alias("regular_price_alg") 
, f.sum(f.when(f.col("sales_qnty") > f.lit(0) , f.col("sales_qnty")).otherwise(f.lit(0))).alias("sales_qnty") 
, f.sum(f.when(f.col("sale_qnty_clean") > f.lit(0) , f.col("sale_qnty_clean")).otherwise(f.lit(0))).alias("sale_qnty_clean")
, f.sum(f.col("qnty_reg")).alias("qnty_reg_")
, f.count(f.lit(1)).alias("days_cnt_")
)

df_output_temp = df_output_temp\
.withColumn("sale_qnty_reg_avg", f.col("qnty_reg_") / f.when(f.col("days_cnt_") != 0 , f.col("days_cnt_")))

#Финальная таблица

df_output_data = df_output_temp\
.withColumn("day_id_update",f.lit(datetime.datetime.today().strftime("%Y-%m-%d")))\
.withColumn("snapshot_type", f.lit(p_snapshot_type))\
.where("coalesce(sale_qnty_reg_avg,0) >0 and coalesce(regular_price,0) >0")\
.select(
    df_output_temp.whs_id
   , df_output_temp.art_id
   , df_output_temp.sale_qnty_reg_avg
   , df_output_temp.regular_price
   , df_output_temp.regular_price_alg
   , df_output_temp.sales_qnty
   , df_output_temp.sale_qnty_clean
   , df_output_temp.qnty_reg_.alias("qnty_reg")
   , df_output_temp.days_cnt_.alias("days_cnt")
    )

#Сохраняем результат
df_output_data.repartition(1).write.format("parquet").mode("overwrite").save("/data/inc/olifir_pg/p_df_machine_forecast_base_sale")   