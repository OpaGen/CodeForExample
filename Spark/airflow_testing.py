from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import pyspark

spark = SparkSession.builder.appName('airflow_example').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set('spark.sql.shuffle.partitions', '1750')

print(sys.argv[1])

spark.read.format('delta').load('/data/base/d_whs').show()