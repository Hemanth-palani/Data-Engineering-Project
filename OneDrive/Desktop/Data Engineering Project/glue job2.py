import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------
# 1. Initialize Glue Job
# -------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------------
# 2. Read Data (Parquet)
# -------------------------------
df = spark.read.format("parquet") \
    .load("s3://proj-bucket/bronze_to_silver_output/")

# -------------------------------
# 3. Handle Missing Columns Safely
# -------------------------------

cols = df.columns

# -------------------------------
# 4. Transformations
# -------------------------------

# Runtime conversion (ONLY if duration exists)
if "duration" in cols:
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
else:
    df = df.withColumn("runtime", lit(None).cast("int"))

# Indian movie flag (safe)
if "country" in cols:
    df = df.withColumn(
        "is_indian_movie",
        when(lower(col("country")).contains("india"), True).otherwise(False)
    )
else:
    df = df.withColumn("is_indian_movie", lit(False))

# Extract rating code (safe)
if "rating" in cols:
    df = df.withColumn(
        "establish_code",
        regexp_extract(col("rating"), r"^([^-]+)", 1)
    )
else:
    df = df.withColumn("establish_code", lit(None))

# Runtime int (safe)
df = df.withColumn(
    "runtime_int",
    col("runtime").cast("int")
)

# Convert date (safe)
if "date_added" in cols:
    df = df.withColumn(
        "modified_date_value",
        to_date(trim(col("date_added")), "MMMM d, yyyy")
    )
else:
    df = df.withColumn("modified_date_value", lit(None).cast("date"))

# Add timestamps
df = df.withColumn("load_tmst", current_timestamp())
df = df.withColumn("load_date", current_date())

# -------------------------------
# 5. Write Output
# -------------------------------
df.write.mode("overwrite") \
    .parquet("s3://proj-bucket/netflix-silver/")

# -------------------------------
# 6. Commit Job
# -------------------------------
job.commit()