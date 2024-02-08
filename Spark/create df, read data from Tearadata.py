from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import pyspark
import numpy as np
import subprocess
spark = SparkSession.builder.appName("d_assort_prim").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
from pyspark.sql import functions as f
from pyspark.sql import types as t
import sys
import datetime
from delta.tables import *
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


sql = """SELECT a.* , a.whs_id mod 10 as MDN from PRD_VD_DM.V_ASSORT_PRIM AS a WHERE DAY_ID = '2021-07-10'"""


#print(sql)	

df = spark.read.format("jdbc")\
    .option("url","jdbc:teradata://teradata/CHARSET=UTF16,TMODE=ANSI,LOGMECH=TD2")\
    .option("user","anp_infa")\
    .option("password", "anp_infa")\
    .option("dbtable", "(SELECT A.* FROM ({sql})A) emp".format(sql=sql)) \
    .option("partitionColumn", "MDN")\
    .option("lowerBound", 0)\
    .option("upperBound", 9)\
    .option("numPartitions", 10)\
    .option("driver", "com.teradata.jdbc.TeraDriver")\
.load()


df=df.select(
 'src_system_id'
,'day_id'
,'art_id'
,'cntr_id'
,'whs_id'
,'artset_id'
,'dt1'
,'perc'
,'isdc'
)

df.repartition('day_id').write.format("delta").partitionBy('day_id').mode("append").option("mergeSchema", "true").save("/data/base/d_assort_prim/")
delta_table3 = DeltaTable.forPath(spark,"/data/base/d_assort_prim/")
delta_table3.vacuum(0)
print(1)

