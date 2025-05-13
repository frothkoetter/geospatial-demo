
import laspy
import os
import numpy as np
import pyarrow as pa
import pyproj
import pandas as pd
import cml.data_v1 as cmldata
import geohash2

from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import lit
from pyspark.sql.functions import sha1, concat
from pyspark.sql.types import StringType
  

# LAS file processing function
def process_las_file(file_path):
    with laspy.open(file_path) as las:
        # Read all points
        las = las.read()
        
        # Convert to numpy structured array
        points = np.array([
            (x, y, z, intensity, return_num, classification)
            for x, y, z, intensity, return_num, classification in zip(
                las.x, las.y, las.z,
                las.intensity,
                las.return_number,
                las.classification
            )
        ], dtype=[
            ('x', 'f8'), ('y', 'f8'), ('z', 'f8'),
            ('intensity', 'i4'), ('return_num', 'i4'), 
            ('classification', 'i4')
        ])
        
        return points

# open Spark
# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '2')
SparkContext.setSystemProperty('spark.executor.memory', '8g')
SparkContext.setSystemProperty('spark.rpc.message.maxSize', '512')

CONNECTION_NAME = "se-aws-edl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# Create Hive database (if not exists)
spark.sql("CREATE DATABASE IF NOT EXISTS geospatial")

# Create Hive table with your schema
spark.sql("""
CREATE TABLE IF NOT EXISTS geospatial.punktwolke (
    x DOUBLE COMMENT 'X coordinate',
    y DOUBLE COMMENT 'Y coordinate',
    z DOUBLE COMMENT 'Z coordinate/elevation',
    intensity INT COMMENT 'Return intensity value',
    return_num INT COMMENT 'Return number',
    classification INT COMMENT 'Point classification'
)
USING ICEBERG
PARTITIONED BY (source_file STRING)

TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'spatial'='true'
)
""")

# use local laz file 
# use !hdfs dfs -copyToLocal from S3 or HDFS
laz_file = "/home/cdsw/cml-pipeline/data/3dm_32_292_5629_1_nw.laz"

# Process the file
points = process_las_file(laz_file)

# Create Spark DataFrame
schema = StructType([
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("z", DoubleType()),
    StructField("intensity", IntegerType()),
    StructField("return_num", IntegerType()),
    StructField("classification", IntegerType())
])

# Convert numpy array to Spark DataFrame
df = spark.createDataFrame(
    [(float(p[0]), float(p[1]), float(p[2]), 
     int(p[3]), int(p[4]), int(p[5])) for p in points],
    schema=schema
)

df.createOrReplaceTempView("temp_points")
spark.sql("INSERT INTO TABLE geospatial.punktwolke SELECT * FROM temp_points").show()

# EoF
spark.stop()
