dfrom pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import shutil

spark = SparkSession.builder.appName("Netflix_Bronze_To_Silver").getOrCreate()

df = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .load("/Volumes/workspace/default/batch21/netflixoutput/")

df = df.withColumn(
    "runtime",
    when(
        col("duration").rlike("season|Season"),
        regexp_extract(col("duration"), r"(\d+)", 1).cast("int") * 50
    ).when(
        col("duration").rlike("min"),
        regexp_extract(col("duration"), r"(\d+)", 1).cast("int")
    ).otherwise(None)
)

df = df.withColumn(
    "is_indian_movie",
    when(col("country").contains("india"),True).otherwise(False)
)

df = df.withColumn(
    "establish_code",
    regexp_extract(col("rating"), r"^([^-]+)", 1)
)

df = df.withColumn(
    "runtime_int",
    col("runtime").cast("int")
)
df = df.withColumn(
    "modified_date_value",
    to_date(trim(col("date_added")), "MMMM d, yyyy")
)
df = df.withColumn(
    "load_tmst",
    current_timestamp()
)
from pyspark.sql.functions import current_date

df = df.withColumn(
    "load_date",
    current_date()
)
df.show()