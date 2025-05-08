# !pip install -r requirements.txt
# !pip install cudf-cu12 --extra-index-url=https://pypi.nvidia.com

import boto3
import os
import laspy
import io
# import cudf
import time
import sys
import re
import fnmatch
import pyproj
import cml.data_v1 as cmldata
import geohash2

from pyproj import Transformer
from tqdm import tqdm  # For progress bars
from fnmatch import fnmatch
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import sha1, concat
from pyspark.sql.types import StringType



# Coordinate transformers

# Initialize coordinate transformer: EPSG:25832 (UTM) → EPSG:4326 (lat/lon)
transformer = pyproj.Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
# Set batch size for efficient CPU processing
batch_size = 1_000_000

# Define schema Spark DF
schema = StructType([
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("z", DoubleType()),
    StructField("intensity", IntegerType()),
    StructField("return_num", IntegerType()),
    StructField("classification", IntegerType())
])

def add_geohash(gdf, precision=7):
    """Add geohash column to cuDF DataFrame"""
    # Convert coordinates to WGS84
    lon, lat = utm32n_to_wgs84.transform(
        gdf['x'].to_array(),
        gdf['y'].to_array()
    )

    # Calculate geohashes (CPU operation)
    geohashes = [geohash2.encode(lat[i], lon[i], precision) for i in range(len(lat))]

    # Add to DataFrame - cuDF style (not withColumn)
    gdf['geohash'] = geohashes
    return gdf

# UDF to convert and encode geohash
def projected_to_geohash(x, y):
    import geohash2
    try:
        lon, lat = transformer.transform(x, y)  # Note: always_xy=True makes it x, y -> lon, lat
        return geohash2.encode(lat, lon, precision=6)
    except:
        return None


# Register UDF
geohash_udf = udf(projected_to_geohash, StringType())

def encode_geohash(x, y, geohash_out):
    for i in range(len(x)):
        geohash_out[i] = geohash2.encode(y[i], x[i], precision=6)


# Initialize S3 client
session = boto3.Session()

# Create a session
session = boto3.Session(
    aws_access_key_id="AKIA6I6SNFMLMADZUHA3",
    aws_secret_access_key="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",
    region_name="eu-central-1"
)

# S3 resource (for object operations like download/upload)
s3_resource = session.resource("s3")

s3_client = session.client("s3")
bucket_name = "goes-se-sandbox"
prefix = "data/geospatial/nw/"

# Get all LAZ files

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

fnmatch_pattern = "*3dm_32_283*.laz"
laz_files = []
for obj in response['Contents']:
  key = obj['Key']
  if fnmatch(key, fnmatch_pattern):
    laz_files.append(key)

# With error handling if no files found
if not laz_files:
    raise FileNotFoundError(
        f"No LAZ files found matching pattern in s3://{bucket_name}/{prefix}\n"
        f"Expected pattern: {fnmatch_pattern}"
    )

print(f"Found {len(laz_files)} matching LAZ files:")

# Process each file
for file_key in tqdm(laz_files, desc="Download LAZ files"):
    try:
        # Download file
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        #binary_data = file_obj['Body'].read()
        #source_file = os.path.basename(file_key)
        # Just for confirmation
        print(f"Downloaded file: {file_key}, size: {len(binary_data)} bytes")
        # Define local path to save the file
        local_path = '/home/cdsw/data/'+ source_file  # just the filename

        # Write to local file
        # with open(local_path, 'wb') as f:
        #  f.write(binary_data)


    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        continue


# open Spark
# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '2')
SparkContext.setSystemProperty('spark.executor.memory', '8g')
SparkContext.setSystemProperty('spark.rpc.message.maxSize', '512')

CONNECTION_NAME = "itzg-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# Create Hive database (if not exists)
spark.sql("CREATE DATABASE IF NOT EXISTS geospatial")

# Create Hive table with your schema
spark.sql("""
CREATE TABLE IF NOT EXISTS geospatial.point_cloud (
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


# Process each file
for file_key in tqdm(laz_files, desc="Processing LAZ files"):
    try:
        # Download file
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        binary_data = file_obj['Body'].read()
        source_file = os.path.basename(file_key)
        # Process with laspy + cuDF
        start_time = time.time()
        with laspy.open(io.BytesIO(binary_data)) as las:
            las = las.read()

            # Extract fields
            x = las.x
            y = las.y
            z = las.z
            intensity = las.intensity
            return_num = las.return_number
            classification = las.classification

            point_data = list(zip(
                list(las.x),
                list(las.y),
                list(las.z),
                list(las.intensity),
                list(las.return_number),
                list(las.classification)
            ))

        df = spark.createDataFrame(
          [(float(p[0]), float(p[1]), float(p[2]),
            int(p[3]), int(p[4]), int(p[5])) for p in point_data],
          schema=schema
        )

        # Add file origin metadata
        df = df.withColumn("source_file", lit(source_file))

        # INSERT into Iceberg
        # Convert pandas → Spark DataFrame


        df.createOrReplaceTempView("temp_points")
        spark.sql("INSERT INTO TABLE geospatial.point_cloud SELECT * FROM temp_points").show()
        spark.sql("SELECT count(*) FROM temp_points group").show()


    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        continue

print("Processing complete!")
spark.stop()
