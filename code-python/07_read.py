# # Reading and Writing Data

# Copyright © 2010–2018 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we introduce the DataFrameReader and DataFrameWriter classes
# and demonstrate how to read from and write to a number of data sources.


# ## Reading and Writing Data

# * Spark can read from and write to a variety of data sources.

# * The Spark SQL `DataFrameReader` and `DataFrameWriter` classes support the
# following data sources:
#   * text
#   * delimited text
#   * JSON (JavaScript Object Notation)
#   * Apache Parquet
#   * Apache ORC
#   * Apache Hive
#   * JDBC connection

# * Spark SQL also integrates with the pandas Python package.

# * Additional data sources are supported by [third-party
# packages](https://spark-packages.org/).


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("read").enableHiveSupport().getOrCreate()

# **Note:** We have added the `enableHiveSupport` method.


# Create an HDFS directory for saved data:
!hdfs dfs -rm -r -skipTrash data  # Remove any existing directory
!hdfs dfs -mkdir data


# ## Working with delimited text files

# Use the
# [csv](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv)
# method of the
# [DataFrameReader](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# class to read a delimited text file:
riders = spark \
  .read \
  .csv("/duocar/raw/riders/", sep=",", header=True, inferSchema=True) \

# The `csv` method is a convenience method for the following more general
# syntax:
riders = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .option("inferSchema", True) \
  .load("/duocar/raw/riders/")

# **Note:** If you use either syntax with `header` set to `True`, then Spark
# assumes that *every* file in the directory has a header row.

# Spark does its best to infer the schema from the header row and column
# values:
riders.printSchema()

# Alternatively, you can manually specify the schema.  First, import the Spark
# SQL
# [types](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
# module:
from pyspark.sql.types import *

# Then specify the schema as a `StructType` instance: 
schema = StructType([
    StructField("id", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("join_date", DateType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("student", IntegerType(), True),
    StructField("home_block", StringType(), True),
    StructField("home_lat", DoubleType(), True),
    StructField("home_lon", DoubleType(), True),
    StructField("work_lat", DoubleType(), True),
    StructField("work_lon", DoubleType(), True)
])

# Finally, pass the schema to the `DataFrameReader`:
riders2 = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .schema(schema) \
  .load("/duocar/raw/riders/")

# **Note:** We must include the header option otherwise Spark will read the
# header row as a valid record.

# Confirm the explicit schema:
riders2.printSchema()

# Use the `csv` method of the
# [DataFrameWriter](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)
# class to write the DataFrame to a tab-delimited file:
riders2.write.csv("data/riders_tsv/", sep="\t")
!hdfs dfs -ls data/riders_tsv

# **Note:** The file has a `csv` extension even though it includes
# tab-separated values.  Never trust a file extension!

# Use the `mode` argument to overwrite existing files and the `compression`
# argument to specify a compression codec:
riders2.write.csv("data/riders_tsv_compressed/", sep="\t", mode="overwrite", compression="bzip2")
!hdfs dfs -ls data/riders_tsv_compressed

# See the Cloudera documentation on [Data
# Compression](https://www.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html)
# for more details.


# ## Working with text files

# Use the
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.text)
# method of the `DataFrameReader` class to read an unstructured text file:
weblogs = spark.read.text("/duocar/earcloud/apache_logs/")
weblogs.printSchema()
weblogs.head(5)

# **Note:** The default filesystem in CDH (and by extension CDSW) is HDFS.
# The read statement above is a shortcut for
# ``` python
# weblogs = spark.read.text("hdfs:///duocar/earcloud/apache_logs/")
# ```
# which in turn is a shortcut for
# ``` python
# weblogs = spark.read.text("hdfs:/<host:port>//duocar/earcloud/apache_logs")
# ```
# where `<host:port>` is the host and port of the HDFS namenode.

# Parse the unstructured data:
from pyspark.sql.functions import regexp_extract
requests = weblogs.select(regexp_extract("value", "^.*\"(GET.*?)\".*$", 1).alias("request")) 
requests.head(5)

# Use the
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.text)
# method of the `DataFrameWriter` class to write an unstructured text file:
requests.write.text("data/requests_txt/")
!hdfs dfs -ls data/requests_txt


# ## Working with Parquet files

# [Parquet](https://parquet.apache.org/) is a very popular columnar storage
# format for Hadoop.  Parquet is the default file format in Spark SQL.  Use
# the `parquet` method of the `DataFrameWriter` class to write to a Parquet
# file:
riders2.write.parquet("data/riders_parquet/")
!hdfs dfs -ls data/riders_parquet

# **Note:** The SLF4J messages are a known issue with CDH.

# Use the `parquet` method of the `DataFrameReader` class to the read from a
# Parquet file:
spark.read.parquet("data/riders_parquet/").printSchema()

# **Note:** Spark uses the schema stored with the data.


# ## Working with Hive Tables

# Use the `sql` method of the `SparkSession` class to run Hive queries:
spark.sql("SHOW DATABASES").show()
spark.sql("USE duocar")
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE riders").show()
spark.sql("SELECT * FROM riders LIMIT 10").show()

# Use the `table` method of the `DataFrameReader` class to read a Hive table:
riders_table = spark.read.table("riders")
riders_table.printSchema()
riders_table.show(5)

# Use the `saveAsTable` method of the `DataFrameWriter` class to write a Hive
# table:
import uuid
table_name = "riders_" + str(uuid.uuid4().hex)  # Create unique table name.
riders.write.saveAsTable(table_name)


# You can now manipulate this table with Hive or Impala or via Spark SQL:
spark.sql("DESCRIBE %s" % table_name).show()


# ## Working with object stores

# Pass the appropriate prefix and path to the DataFrameReader and
# DataFrameWriter methods to read from and write to an object store.  For
# example, use the prefix `s3a` and pass the S3 bucket to read from Amazon S3:
demographics = spark.read.csv("s3a://duocar/raw/demographics/", sep="\t", header=True, inferSchema=True)
demographics.printSchema()
demographics.show(5)

# If we have write permissions, then we can also write files to Amazon S3 using
# the `s3a` prefix.


# ## Working with pandas DataFrames

# Import the pandas package:
import pandas as pd

# Use the pandas `read_csv` method to read a local tab-delimited file:
demographics_pdf = pd.read_csv("data/demographics.txt", sep="\t")

# Access the pandas `dtypes` attribute to the view the data types:
demographics_pdf.dtypes

# Use the pandas `head` method to view the data:
demographics_pdf.head()

# Use the `createDataFrame` method of the `DataFrame` class to create a Spark
# DataFrame from a pandas DataFrame:
demographics = spark.createDataFrame(demographics_pdf)
demographics.printSchema()
demographics.show(5)

# Use the `toPandas` method to read a Spark DataFrame into a pandas DataFrame:
riders_pdf = riders.toPandas()
riders_pdf.dtypes
riders_pdf.head()

# **WARNING:** Use this with caution as you may use all your available memory!

# **Note:** Column types may not convert as expected when reading a Spark
# DataFrame into a pandas DataFrame and vice versa.


# ## Exercises

# (1) Use the `json` method of the `DataFrameWriter` class to write the
# `riders` DataFrame to the `data/riders_json/` (HDFS) directory.

# (2) Use the `hdfs dfs -ls` command to list the contents of the
# `data/riders_json/` directory.

# (3) Use the `hdfs dfs -cat` and `head` commands to display a JSON file in
# the `data/riders_json` directory.

# (4) Use Hue to browse the `data/riders_json/` directory.

# (5) Use the `json` method of the `DataFrameReader` class to read the JSON
# file into a DataFrame.

# (6) Examine the schema of the DataFrame.  Do you notice anything different?


# ## References

# [Spark Python API - pyspark.sql.DataFrameReader
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)

# [Spark Python API - pyspark.sql.DataFrameWriter
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)


# ## Cleanup

# Drop the Hive table:
spark.sql("DROP TABLE IF EXISTS %s" % table_name)

# Stop the SparkSession:
spark.stop()
