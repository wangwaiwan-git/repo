# Databricks notebook source
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
#file location & type
file_type = "csv"

#csv options
infer_schema        = "false"
first_row_is_header = "true"
delimiter           = ","

#create dataframe using options
file_location = "/FileStore/tables/BabyNames.csv"
BabyNames_df = spark.read.format(file_type) \
          .option("header", first_row_is_header) \
          .option("inferSchema", infer_schema) \
          .option("sep",delimiter) \
          .option("escape", '"')\
          .load(file_location)

#display(BabyNames_df)

# COMMAND ----------

BabyNames_df.createOrReplaceTempView("tbl_BabyNames")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl_BabyNames

# COMMAND ----------

