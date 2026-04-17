import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------
# 1. Initialize Glue Context
# -------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------------
# 2. Read Data from S3
# -------------------------------
input_path = "s3://proj-bucket/bronze/netflix_titles_csv.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .load(input_path)

# -------------------------------
# 3. Remove Duplicates
# -------------------------------
df = df.dropDuplicates(["show_id"])

# -------------------------------
# 4. Standardize Column Names
# -------------------------------
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# -------------------------------
# 5. Trim Whitespaces
# -------------------------------
for c in df.columns:
    df = df.withColumn(c, trim(col(c)))

# -------------------------------
# 6. Handle Null Values
# -------------------------------
df = df.fillna({
    "director": "unknown",
    "cast": "unknown",
    "country": "unknown",
    "rating": "not_rated"
})

# -------------------------------
# 7. Fix Data Types
# -------------------------------
df = df.withColumn("release_year", col("release_year").cast(IntegerType()))

# -------------------------------
# 8. Remove Special Characters
# -------------------------------
df = df.withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", ""))

# -------------------------------
# 9. Validation Rules
# -------------------------------
df = df.filter(col("show_id").isNotNull())
df = df.filter(col("title").isNotNull())

# -------------------------------
# 10. Handle Outliers
# -------------------------------
df = df.filter(col("release_year") >= 1900)

# -------------------------------
# 11. Select Final Columns
# -------------------------------
df_final = df.select(
    "show_id",
    "type",
    "title",
    "director",
    "cast",
    "country",
    "date_added",
    "release_year",
    "rating",
    "listed_in",
    "description"
)

# -------------------------------
# 12. Write to S3 (Parquet)
# -------------------------------
output_path = "s3://proj-bucket/bronze_to_silver_output/"

df_final.write \
    .mode("overwrite") \
    .parquet(output_path)

# -------------------------------
# 13. Commit Job
# -------------------------------
job.commit()