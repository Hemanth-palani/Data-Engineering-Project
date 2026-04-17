from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import shutil

spark = SparkSession.builder.appName("Netflix_Bronze_To_Silver").getOrCreate()

# -------------------------------
# 1. Read Data
# -------------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .load("/Volumes/workspace/default/batch21/netflix_titles_csv.csv")
# -------------------------------
# 2. Remove Duplicates
# -------------------------------
df = df.dropDuplicates(["show_id"])

# -------------------------------
# 3. Standardize Column Names
# -------------------------------
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# -------------------------------
# 4. Trim Whitespaces
# -------------------------------
for c in df.columns:
    df = df.withColumn(c, trim(col(c)))

# -------------------------------
# 5. Handle Null Values
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
# 10. Remove Special Characters
# -------------------------------
df = df.withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", ""))

# -------------------------------
# 12. Validation Rules
# -------------------------------
df = df.filter(col("show_id").isNotNull())
df = df.filter(col("title").isNotNull())

# -------------------------------
# 13. Handle Outliers
# -------------------------------
df = df.filter(col("release_year") >= 1900)

# -------------------------------
# 14. Select Final Columns
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
    "description",
)
df_final.show()
df.write \
  .mode("overwrite") \
  .parquet("/Volumes/workspace/default/batch21/netflixoutput/")