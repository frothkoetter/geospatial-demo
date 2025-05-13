# Geospatial Workshop

Ingest, transform and analyse geospatial data

## Introduction

This workshop gives an overview of how to use the Cloudera quickly ingest data from remote sources, transform and explore raw data, create enriched and curated versions of the data for analyze, and then scale up.

![](images/demo-flow.png)

## Compoments

Cloudera Machine Learning (CML) is Cloudera’s cloud-native machine learning platform built for CDP. Cloudera Machine Learning unifies self-service data science and data engineering in a single, portable service as part of an enterprise data cloud for multi-function analytics on data anywhere.

Cloudera Data Flow (CDF) data service powered by Apache NiFi that facilitates universal data distribution by streamlining the end-to-end process of data movement.

Cloudera Data Warehouse enables IT to deliver a cloud-native self-service analytic experience to BI analysts that goes from zero to query in minutes. It performs on all sizes and types of data, including structured and unstructured, while scaling cost-effectively past petabytes.

# Apache NiFi Flow Explanation: nifi2-demo-flow.json

![](images/nifi-image-01.png)

This NiFi flow is designed to ingest LAZ (LAS Zip) files from an open geodata source, process them, and store them in both Amazon S3 and an Iceberg table. Here's a breakdown of the flow:

## Overview
The flow downloads 3D laser scanning data from North Rhine-Westphalia's open geodata portal, processes the LAZ files, converts them to CSV format, and stores them in cloud storage and a database table.

## Main Components

Create upfront the Iceberg table punktwolke - see   - run cdw/hive-punktwolke.sql on CDW / HUE

![](images/punktwolke-SQL.png)

## NIFI Flow Processors

### 1. Data Retrieval
- **3D-Messdaten Laserscanning - Paketierung: Einzelkacheln** (InvokeHTTP processor)
  - Makes a GET request to `https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/`
  - Retrieves the initial data listing (likely an XML file)

### 2. Data Processing Pipeline
- **Full list of download** (SplitXml processor)
  - Splits the XML response into individual elements (depth=4)

- **Eval LAZ files** (EvaluateXPath processor)
  - Extracts LAZ filenames using XPath expression `file/@name`

- **construct File URI** (ReplaceText processor)
  - Constructs full download URLs by combining the base URL with filenames
  - Uses expression `${invokehttp.request.url}/$1`

- **Prepare Download** (ExtractText processor)
  - Extracts the LAZ filename (`laz_file`) and URL (`url`) using regex patterns

- **Download File** (InvokeHTTP processor)
  - Downloads the actual LAZ files using the constructed URLs

### 3. Storage Paths
#### A. S3 Storage Path
- **Store in Cloud Storage S3** (PutS3Object processor)
  - Stores files in S3 bucket `goes-se-sandbox`
  - Path: `data/geospatial/nw/${laz_file}`
  - Uses AWS credentials from parameter context
  - Auto-terminates on success, routes failures to funnel

#### B. Iceberg Table Path
- **LazToCsvProcessor** (Custom Python processor)
  - Converts LAZ files to CSV format
  - Processes in chunks (10,000 records) with max 100,000 records

- **Store in Iceberg Table PUNKTWOLKE** (PutIceberg processor)
  - Stores data in Iceberg table `punktwolke` in `geospatial` namespace
  - Uses HiveCatalogService for catalog management
  - File format: PARQUET
  - Uses Kerberos authentication (user: frothkoetter)
  - Auto-terminates on success, routes failures to funnel

### 4. Supporting Components
- **Controller Services**:
  - HiveCatalogService: Manages Iceberg catalog
  - CSVReader: Reads CSV data
  - KerberosPasswordUserService: Handles Kerberos auth
  - AWSCredentialsProviderControllerService: Manages AWS credentials

- **Parameter Context** (`lidar_nw`):
  - Contains sensitive credentials and configuration
  - AWS access keys
  - S3 bucket/path configuration
  - CDP environment files
  - Kerberos credentials

### 5. Error Handling
- Multiple connections route failures to funnels
- Backpressure settings (10,000 objects or 1GB) prevent overload
- Retry count set to 10 for processors

## Flow Execution Path
1. Retrieve XML listing of available files
2. Split XML into individual file entries
3. Extract LAZ filenames and construct full URLs
4. Download each LAZ file
5. Store original LAZ files in S3
6. Convert LAZ to CSV
7. Store CSV data in Iceberg table

This flow appears to be part of a geospatial data pipeline, processing 3D laser scanning data for storage and analysis in a data lake environment.


## Nifi 2 Python Transformer

This custom NiFi 2 Python processor (`LazToCsvProcessor`) is designed to **convert `.laz` point cloud files into CSV format**. It uses the `laspy`, `lazrs`, and `numpy` libraries to read, process, and transform LiDAR point data.

Here's a high-level explanation of how it works:

---

### 🔧 **Purpose**

Convert `.laz` (compressed LiDAR data) binary file content into CSV rows with selected attributes: `x`, `y`, `z`, `intensity`, `return number`, and `classification`.

---

### 📦 **Key Libraries Used**

* **`laspy` + `lazrs`**: Read compressed `.laz` LiDAR files.
* **`numpy`**: Efficient handling of structured array data.
* **`io.StringIO`**: Temporarily buffer CSV output in-memory.
* **`tempfile`**: Temporarily write `.laz` file content for `laspy` to read.

---

### 🧩 **Core Components**

#### **1. Processor Configuration**

Defined in the `ProcessorDetails` class:

```python
version = '0.0.6'
description = 'Reads a .laz file from FlowFile content and converts it to CSV.'
```

#### **2. Custom Properties**

Two optional tunables:

* `Max Records`: Total points to process (default: 100,000).
* `Chunk Records`: Sub-chunk size (default: 10,000) to avoid high memory use.

#### **3. Main Transformation Logic**

Occurs in the `transform` method:

1. **Read .laz File Content** from FlowFile as binary.
2. **Write to Temporary File** so that `laspy` can open it.
3. **Open with `laspy.open`** using the `lazrs` backend.
4. **Iterate Over Chunks**:

   * For each chunk:

     * Sub-divide into smaller "chunk records" to manage memory.
     * Convert each mini-chunk to a structured NumPy array.
     * Write CSV rows (`x, y, z, intensity, return_num, classification`) to an in-memory buffer.
5. **Cleanup**: Delete the temporary `.laz` file.
6. **Return a New FlowFile** with:

   * Content: CSV string as bytes.
   * Attributes: `laz.converted=true`, `point.count=N`.
   * Relationship: `success`.

#### **4. Error Handling**

* If anything fails (e.g. empty input, parsing issues), return `relationship='failure'` and capture the error message as a FlowFile attribute.

---

### ✅ **Success Case**

If `.laz` file is valid:

* FlowFile content becomes a CSV.
* Attributes added:

  * `"laz.converted": "true"`
  * `"point.count": "XXXXX"`
* Routed to `success`.

### ❌ **Failure Case**

* If error occurs (invalid file, empty, etc.), return:

  * FlowFile with error message as content.
  * Attribute: `"error": "..."`.
  * Routed to `failure`.

---

### 🗺️ Example Output (CSV)

```
x,y,z,intensity,return_num,classification
123.45,678.90,100.23,45,1,2
...
```

---

### 🧠 Summary

This is a clean, efficient NiFi 2 processor for LiDAR preprocessing pipelines, converting binary `.laz` data into structured, consumable CSV format—ideal for downstream analytics or GIS tools.

Would you like a diagram or sample input/output to go with this explanation?


# CML - Geospatial Data Pipeline : LAZ-to-Iceberg.py

This Python script is a geospatial data pipeline that performs the following end-to-end operations:

TThis Python code performs a **complete workflow for processing a LAS/LAZ point cloud file**, converting it to a Spark DataFrame, and then storing it in an **Iceberg-backed Hive table** using **Cloudera Machine Learning (CML)**. Here's a breakdown of what each part does:

---

## 📦 **Imports**

```python
import laspy, os, numpy as np, pyarrow as pa, pyproj, pandas as pd, cml.data_v1 as cmldata, geohash2
from pyspark.sql.types import ...
from pyspark.sql.functions import ...
```

* `laspy`: Reads LAZ/LAS point cloud files.
* `numpy`: Used for structured arrays to hold point data.
* `pyarrow`: (Imported but unused here) often used for converting to Arrow format.
* `pyproj`: (Unused in this snippet) typically for coordinate projections.
* `geohash2`: For spatial indexing with geohashes (also unused here).
* `cml.data_v1`: Cloudera’s data connector to Spark.
* `pyspark`: Used for defining schema, DataFrame operations, and SQL.

---

## 📌 **Function: `process_las_file()`**

```python
def process_las_file(file_path):
    ...
```

* **Opens** a LAS/LAZ file using `laspy`.
* **Reads all points** and extracts: `x, y, z, intensity, return_number, classification`.
* Packs them into a **structured NumPy array** with defined types (`f8 = float64`, `i4 = int32`).
* Returns the point cloud data as a NumPy array (efficient and compact).

---

## 🚀 **Spark Initialization**

```python
from pyspark import SparkContext
SparkContext.setSystemProperty(...)
```

* Customizes Spark environment for resource control:

  * 2 cores
  * 8 GB executor memory
  * Max RPC message size = 512 MB

```python
CONNECTION_NAME = "se-aws-edl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
```

* Gets Spark session from Cloudera Machine Learning (via configured connection).

---

## 🗃️ **Hive Table Setup with Iceberg**

```sql
CREATE TABLE IF NOT EXISTS geospatial.punktwolke ...
```

Creates a table `geospatial.punktwolke` with:

* Columns for point data.
* Partitioning **by `source_file`** — but **note**: `source_file` is **not in the schema** here, so this may fail or be ineffective unless added later.
* Uses **Iceberg** and **Snappy compression**.
* Marks the table as `spatial=true` (custom metadata, likely for query engines or catalogs to treat it as geospatial).

---

## 📂 **File Processing and DataFrame Creation**

```python
laz_file = "/home/cdsw/...laz"
points = process_las_file(laz_file)
```

* Processes a `.laz` file and extracts structured point cloud data.

```python
schema = StructType([...])
df = spark.createDataFrame([...], schema=schema)
```

* Defines a Spark schema for the structured point cloud.
* Converts the NumPy array to a Spark DataFrame.

---

## 🧠 **Temporary View + Hive Insert**

```python
df.createOrReplaceTempView("temp_points")
spark.sql("INSERT INTO TABLE geospatial.punktwolke SELECT * FROM temp_points").show()
```

* Registers the DataFrame as a temporary view.
* Executes SQL to insert data into the Iceberg Hive table.

> `show()` at the end displays the result of the insert query, though it’s not meaningful for INSERTs.


## 🧼 **Cleanup**

```python
spark.stop()
```

* Gracefully stops the Spark session.

---

## ✅ **Summary**

| Component                               | Purpose                                                             |
| --------------------------------------- | ------------------------------------------------------------------- |
| `process_las_file()`                    | Parses a LAS/LAZ file into structured NumPy array                   |
| Spark setup                             | Configures Spark executor resources                                 |
| Hive table creation                     | Sets up Iceberg table in `geospatial` DB                            |
| Spark DataFrame                         | Converts LAS data into a Spark-friendly format                      |
| Insert into Hive                        | Populates Hive table with point cloud data                          |
| (Optional) `process_las_to_arrow_spark` | Presumably more optimized path with Arrow, but not implemented here |




# CML - Geospatial Data Pipeline : LAZ-to-Iceberg-geohash12.py **Coordinate Transformation and Geohashing**

```python
# Set up projection transformer
# EPSG 25832 = UTM zone 32N (used in Central Europe, including Germany)
# EPSG 4326 = WGS84 lat/lon (used in GPS, mapping APIs)
transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
```

* This sets up a **coordinate transformer** using `pyproj.Transformer` that converts from UTM (EPSG:25832) to standard GPS lat/lon (EPSG:4326).
* `always_xy=True` ensures consistent input order (x=Easting, y=Northing).

---

### 🌍 **User-Defined Function (UDF) for Geohash Calculation**

```python
def projected_to_geohash(x, y):
    import geohash2
    try:
        lon, lat = transformer.transform(x, y)  # Note: always_xy=True makes it x, y -> lon, lat
        return geohash2.encode(lat, lon, precision=12)
    except:
        return None
```

* Converts each `(x, y)` point from projected UTM to `(lat, lon)`.
* Then uses the `geohash2` package to compute a **12-character geohash**.

  * Geohash is a compact **string representation of geographic location**, useful for spatial partitioning.
* Returns `None` on any failure (e.g., invalid inputs).

---

### 🧠 **Register as a Spark UDF and Use**

```python
geohash_udf = udf(projected_to_geohash, StringType())
```

* Registers the function as a **Spark UDF** (user-defined function) returning `StringType`.

```python
df = df.withColumn("source_file", lit(filename))
df = df.withColumn("geohash12", geohash_udf(df["x"], df["y"]))
```

* Adds a `source_file` column (metadata).
* Adds a new column `geohash12` to the DataFrame by applying the UDF to each row's `x` and `y`.

---

### 🧊 **Hive Table Creation with Iceberg + Partitioning**

```sql
CREATE TABLE IF NOT EXISTS geospatial.punktwolke_geohash12 (
    x DOUBLE,
    y DOUBLE,
    z DOUBLE,
    intensity INT,
    return_num INT,
    classification INT,
    source_file STRING,
    geohash12 STRING
)
USING ICEBERG
PARTITIONED BY(truncate(6,geohash12))
```

* Defines an **Iceberg table** named `punktwolke_geohash12` in the `geospatial` database.
* The table includes all point cloud fields, the `source_file`, and the new `geohash12` column.
* It uses **Iceberg’s spatial-friendly partitioning**:

  * `PARTITIONED BY(truncate(6, geohash12))` groups records by the **first 6 characters** of the geohash.

    * This balances **spatial locality** with **partition size** (12-character geohash is very precise; 6-character is regional).

---

### 🔎 Summary

| Part                                        | What It Does                                                 |
| ------------------------------------------- | ------------------------------------------------------------ |
| `Transformer.from_crs`                      | Converts coordinates from UTM (EPSG:25832) to WGS84 lat/lon  |
| `projected_to_geohash`                      | Transforms and geohashes each point                          |
| `geohash_udf`                               | Registers the transformation as a Spark function             |
| `withColumn("geohash12", ...)`              | Adds geohash to every row in your point cloud                |
| `CREATE TABLE ... PARTITIONED BY geohash12` | Enables spatially aware querying and optimization in Iceberg |

Let me know if you’d like to visualize these geohashes on a map or explore alternate partition strategies.


## 🔍 What does `PARTITIONED BY(truncate(6, geohash12))` mean?

Iceberg's `truncate` transform:

```sql
truncate(N, column)
```

creates partition values by **truncating strings** to their first `N` characters. In this case:

```sql
truncate(6, geohash12)
```

* Truncates the `geohash12` column to its **first 6 characters**.
* For example:

  * `geohash12 = "u33dc6g3pxzr"`
  * `truncate(6, geohash12)` → `"u33dc6"`

---

## 📁 Why only 6 files created?

When you run:

```sql
INSERT INTO geospatial.punktwolke_geohash12 SELECT * FROM ...
```

Apache Iceberg:

1. Evaluates the `truncate(6, geohash12)` for each row.
2. Groups the data into **partitions** by the result of that truncation.
3. Writes **one file per partition** (by default).

So if your dataset has **only 6 unique 6-character geohash prefixes**, Iceberg creates only **6 physical data files**, one per partition.

---

## ✅ Hidden Partitioning

Iceberg **does not expose** the `truncate(6, geohash12)` column as a physical column in the table:

* This is what **"hidden partitioning"** means.
* You can query it like:

  ```sql
  SELECT COUNT(*) FROM punktwolke_geohash12 WHERE geohash12 LIKE 'u33dc6%'
  ```
* But you **don’t need to manage** partition columns manually like in Hive.

This is unlike traditional Hive tables where partition columns must be **explicit and manually added** to your schema.

---

## 📌 Summary

| Concept                  | Explanation                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------- |
| `truncate(6, geohash12)` | Groups data by the first 6 characters of geohash                                             |
| Hidden partitioning      | Iceberg partitions data under-the-hood without exposing partition fields in the schema       |
| 6 files                  | Only 6 unique `geohash12[0:6]` prefixes in your data, so only 6 partitions/files are created |

---

### Optional Tip:

If you want **more partitions** (for better parallelism), you can:

* Use `truncate(7, geohash12)` or `truncate(8, geohash12)` to increase granularity.
* Use a **bucket transform**, e.g. `bucket(32, geohash12)` for even partition sizes (not spatially aware, though).
* Or combine geohash with `z` elevation or source tile if meaningful.



# Cloudera AI - Sedona Examples : ApacheSedonaSQL.ipynb ApacheSedonaCore.ipynb

These examples are from Apache Sedona documentation

https://github.com/apache/sedona/tree/master/docs/usecases


# Cloudera AI - NVIDIA Rapids rapids-gpu.ipynb
Nvidia cuML, cuDF and cuSpatial

This code snippert processes raw data into an NVIDIA GPU DataFrame (cuDF)

```Python
import cudf
import laspy
import lazrs
import numpy as np

def process_las_to_cudf(file_path):
    with laspy.open(file_path) as las:
        las = las.read()
        return cudf.DataFrame({
            'x': cudf.Series(las.x, dtype='float64'),
            'y': cudf.Series(las.y, dtype='float64'),
            'z': cudf.Series(las.z, dtype='float64'),
            'intensity': cudf.Series(np.asarray(las.intensity), dtype='int32'),
            'return_num': cudf.Series(np.asarray(las.return_number), dtype='int32'),
            'classification': cudf.Series(np.asarray(las.classification), dtype='int32')
        })

# Process file

gdf = process_las_to_cudf(local_path)
```

### Show results
```Python
print(gdf.head())
print(f"Number of points: {len(gdf)}")
```
| x         | y         | z    | intensity | return_num | classification |
|-----------|-----------|------|-----------|------------|----------------|
| 280000.00 | 5652013.19 | 59.88 | 41571    | 1          | 2              |
| 280000.00 | 5652013.48 | 59.93 | 45066    | 1          | 2              |
| 280000.32 | 5652013.23 | 59.89 | 40981    | 1          | 2              |
| 280000.59 | 5652013.26 | 59.88 | 40959    | 1          | 2              |
| 280000.32 | 5652013.52 | 60.06 | 45284    | 1          | 20             |

Number of points: 15,787,378

A quick plot show the result

![](images/plot-punktwolke.png)

# Cloudera Data Warehouse

Hive with Spatial Extensions available in PC CDW (check ESRI Extension)

```sql
show functions like 'st%'
```
Output must list available geospatial functions available for your SQL.

|st_aggr_convexhull|
|st_aggr_union |
|st_area |
...



# Spatial Query Explanation: Point Cloud Analysis - Script: cdw-analyse/hive-geospatial.SQL

```sql
WITH bounds AS (
  SELECT ST_GeomFromText(
    'POLYGON((280000 5626000, 280000 5747500, 288500 5747500, 288500 5626000, 280000 5626000))'
  ) AS region_geom
)
SELECT
  COUNT(*) AS total_points,
  COUNT(IF(classification = 2, 1, NULL)) AS ground_points,
  MIN(z) AS min_z,
  MAX(z) AS max_z,
  AVG(z) AS avg_z
FROM punktwolke AS lp
JOIN bounds AS b
  ON ST_Contains(b.region_geom,
           ST_Point(lp.x, lp.y));
```

This SQL query performs a spatial analysis on point cloud data (likely LiDAR data) within a specific geographic bounding box. The query **counts and analyzes points** that fall within a defined rectangular region, specifically focusing on:
- Total point count
- Ground-classified points (classification = 2)
- Elevation statistics (min, max, average Z values)


**Spatial Bounding Box Definition** (CTE named `bounds`):
   ```sql
   SELECT ST_GeomFromText('POLYGON((280000 5626000, 280000 5747500, 288500 5747500, 288500 5626000, 280000 5626000))') AS region_geom
   ```
   - Creates a polygon in UTM coordinates (likely EPSG:25832 for Germany)
   - Defines a rectangular area ~8.5km wide (east-west) and ~121.5km tall (north-south)

**Spatial Join Condition**:
   ```sql
   ON ST_Contains(b.region_geom, ST_Point(lp.x, lp.y))
   ```
   - Uses `ST_Contains` to filter points inside the bounding box
   - Converts raw X/Y coordinates to spatial points with `ST_Point`

**Point Cloud Metrics**:
   - `COUNT(*)` - Total points in the area
   - `COUNT(IF(classification = 2, 1, NULL))` - Count of ground points (LAS classification standard)
   - `MIN(z)/MAX(z)/AVG(z)` - Elevation statistics

This type of query is essential for:
- Terrain modeling (using ground points)
- Calculating vegetation height (canopy - ground)
- Infrastructure planning
- Flood risk analysis (via elevation stats)

## SQL Script Explanation: Geospatial Metadata Processing - Script: cdw-analyse/hive-metadata.SQL

This script processes geospatial metadata from North Rhine-Westphalia's open geodata portal, transforming raw CSV data into an optimized Iceberg table format.

You have to copy manually the metafile into CDP accessable S3 Bucket und directory: /data/geospatial/nw/tile_metadata

Here's the high-level breakdown:

### Raw Data Ingestion (Phase 1)
- **Source**: CSV files from https://www.opengeodata.nrw.de (3D point cloud metadata)
- **External Table Creation**:
  - Creates `tile_metadata` table pointing to S3 location `/data/geospatial/nw/tile_metadata`
  - Handles German column names (Kachelname = tile name, Aktualitaet = currency date)
  - Skips 7 header rows from the CSV (`skip.header.line.count="7"`)
  - Uses semicolon delimiters (common in German data formats)

### Data Curation (Phase 2)
- **Iceberg Table Creation**:
  - Creates `tile_metadata_ice` with enhanced schema:
    - Adds default values for German state info (`Land = "Nordrhein-Westfalen"`)
    - Includes ownership details (`Eigentuemer`)
    - Standardizes versioning (`Version_Standard = "1.2"`)
    - Documents point cloud classifications (`Punkteklassenbelegung`)
  - Uses Iceberg format for advanced features (time travel, schema evolution)

- **Data Transformation**:
  - Appends `.laz` extension to tile names (LASzip format)
  - Preserves original metadata columns while adding contextual defaults
  - Converts raw CSV data into a production-ready format

### Analytics (Phase 3)
- **Cross-Table Analysis**:
  - Joins metadata with `point_cloud_part` table (assumed to contain actual point cloud data)
  - Aggregates point counts by:
    - Geographic region (`land`)
    - Source file (`source_file`)
    - Geohash (spatial indexing)

- **Query Patterns**:
  - Simple metadata inspection (`SELECT * FROM tile_metadata_ice`)
  - Spatial distribution analysis (geohash-based counts)

### Key Technical Aspects
**Data Lake Architecture**:
   - Raw → Curated pipeline pattern
   - External tables for raw data, managed tables for production

**Geospatial Specifics**:
   - Handles German coordinate reference systems (CRS)
   - Manages point cloud resolution values (Aufloesung)
   - Tracks data currency (Aktualitaet)

**Performance Considerations**:
   - Iceberg format enables efficient point cloud metadata queries
   - Geohash aggregation supports spatial analytics

This script represents a complete ETL pipeline for German governmental geospatial data, from raw ingestion to analytical-ready format.
