import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--conf spark.driver.extraJavaOptions='--add-opens=java.base/javax.security.auth=ALL-UNNAMED' "
    "--conf spark.executor.extraJavaOptions='--add-opens=java.base/javax.security.auth=ALL-UNNAMED' "
    "pyspark-shell"
)

from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,stddev,lag, abs
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("FinancialDataTransform") \
    .getOrCreate()

file_path ="C:\\Users\\Asus\\Contacts\\Desktop\\data eng\\ETL_PIPELINE\\AAPL_data_20251022_132303.csv"
df = spark.read.option("header", True).csv(file_path)



df = df.withColumn("Open", expr("try_cast(Open as float)")) \
       .withColumn("High", expr("try_cast(High as float)")) \
       .withColumn("Low", expr("try_cast(Low as float)")) \
       .withColumn("Close", expr("try_cast(Close as float)")) \
       .withColumn("Volume", expr("try_cast(Volume as int)"))


window_spec_ma = Window.partitionBy("symbol").orderBy("Datetime").rowsBetween(-2, 0)
window_spec_lag = Window.partitionBy("symbol").orderBy("Datetime")

df = df.withColumn("MA_3", avg("Close").over(window_spec_ma))
df = df.withColumn("Volatility_3", stddev("Close").over(window_spec_ma))
df = df.withColumn("Prev_Close", lag("Close", 1).over(window_spec_lag))
df = df.withColumn("Daily_Return", ((col("Close") - col("Prev_Close")) / col("Prev_Close")) * 100)
df = df.withColumn("Volume_MA_3", avg("Volume").over(window_spec_ma))
df = df.withColumn("Large_Move", (abs(col("Daily_Return")) > 2).cast("int"))



output_path = "C:\\Users\\Asus\\Contacts\\Desktop\\data eng\\ETL_PIPELINE\\data\\transformed\\"
df.write.mode("overwrite").parquet(output_path)  

print(f"Transformed data saved to {output_path}")