import cml.data_v1 as cmldata
import laspy
import numpy as np
import os

#import pygeohash as pgh

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import sha1, concat
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyproj import Transformer

import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '2')
SparkContext.setSystemProperty('spark.executor.memory', '8g')
SparkContext.setSystemProperty('spark.rpc.message.maxSize', '512')

CONNECTION_NAME = "se-aws-edl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()

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


# Sample file path (replace with your LAS/LAZ file)
# https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/ 
# !hdfs dfs -copyToLocal to copy data from S3 or HDFS to local 

filename = "cml-pipeline/data/3dm_32_292_5629_1_nw.laz"

# Process the file
points = process_las_file(filename)

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


# Set up projection transformer
# EPSG 25832, Höhe: EPSG 7837
transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)

# UDF to convert and encode geohash
def projected_to_geohash(x, y):
    import geohash2
    try:
        lon, lat = transformer.transform(x, y)  # Note: always_xy=True makes it x, y -> lon, lat
        return geohash2.encode(lat, lon, precision=12)
    except:
        return None

# Register UDF
geohash_udf = udf(projected_to_geohash, StringType())

# Add file origin metadata
df = df.withColumn("source_file", lit(filename))

# Apply
# Use withColumn correctly
df = df.withColumn("geohash12", geohash_udf(df["x"], df["y"]))   

# Create Hive database (if not exists)
spark.sql("CREATE DATABASE IF NOT EXISTS geospatial")

# Create Hive table with your schema
spark.sql("""
CREATE TABLE IF NOT EXISTS geospatial.punktwolke_geohash12 (
    x DOUBLE COMMENT 'X coordinate',
    y DOUBLE COMMENT 'Y coordinate',
    z DOUBLE COMMENT 'Z coordinate/elevation',
    intensity INT COMMENT 'Return intensity value',
    return_num INT COMMENT 'Return number',
    classification INT COMMENT 'Point classification',
    source_file string,
    geohash12 string
)
USING ICEBERG
PARTITIONED BY(truncate(6,geohash12))
""")


#Alternative: Using SQL INSERT
df.createOrReplaceTempView("temp_points")
spark.sql("INSERT INTO TABLE geospatial.punktwolke_geohash12 SELECT * FROM temp_points")


# EoF
spark.stop()