create database geospatial;

DROP TABLE if exists point_cloud;
CREATE TABLE IF NOT EXISTS point_cloud (
    x DOUBLE COMMENT 'X coordinate',
    y DOUBLE COMMENT 'Y coordinate',
    z DOUBLE COMMENT 'Z coordinate/elevation',
    intensity INT COMMENT 'Return intensity value',
    return_num INT COMMENT 'Return number',
    classification INT COMMENT 'Point classification',
    source_file STRING
) 
STORED by iceberg stored as parquet; 

DESCRIBE formatted point_cloud;

--
-- wait for data ingested by Nifi
--
select count(1), geohash from point_cloud_part group by geohash;


--
-- Handling meta data 
--
-- requirement: uploaded CSV files should be uploaded to the S3 bucket
-- URL: https://geoportal.nrw/?activetab=map#/datasets/iso/ef0b51eb-31ea-49dd-a1c3-42e565c2b1a1
--
CREATE EXTERNAL TABLE geospatial.tile_metadata (
    Kachelname STRING,
    Aktualitaet STRING,
    Erfassungsmethode INT,
    Fortfuehrung STRING,
    Fortfuehrungsmethode INT,
    Aufloesung DOUBLE,
    Koordinatenreferenzsystem_Lage STRING,
    Koordinatenreferenzsystem_Hoehe STRING,
    Hoehenanomalie STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/data/geospatial/nw/tile_metadata' tblproperties("skip.header.line.count"="7");

describe formatted geospatial.tile_metadata;

select * from tile_metadata;

drop  table geospatial.tile_metadata;

-- 
-- create a Iceberg table and curated version of the data
--
drop table if exists  geospatial.tile_metadata_ice;
CREATE TABLE geospatial.tile_metadata_ice (
    Land  STRING default "Nordrhein-Westfalen",
    Eigentuemer STRING default "Land NRW, Bezirksregierung Koeln, Abteilung Geobasis NRW",
    Aktualitaet_Kachelinformationen DATE default current_date(),
    Version_Standard STRING default "1.2",
    Punktklassenbelegung STRING default "1,2,14,17,18,20,24,26",
    Kachelname STRING,
    Aktualitaet STRING,
    Erfassungsmethode INT,
    Fortfuehrung STRING,
    Fortfuehrungsmethode INT,
    Aufloesung DOUBLE,
    Koordinatenreferenzsystem_Lage STRING,
    Koordinatenreferenzsystem_Hoehe STRING,
    Hoehenanomalie STRING
)
stored by iceberg;


INSERT INTO geospatial.tile_metadata_ice (
    Land,
    Eigentuemer,
    Aktualitaet_Kachelinformationen,
    Version_Standard,
    Punktklassenbelegung,
    Kachelname,
    Aktualitaet,
    Erfassungsmethode,
    Fortfuehrung,
    Fortfuehrungsmethode,
    Aufloesung,
    Koordinatenreferenzsystem_Lage,
    Koordinatenreferenzsystem_Hoehe,
    Hoehenanomalie
)
SELECT
    "Nordrhein-Westfalen" AS Land, 
     "Land NRW, Bezirksregierung Koeln, Abteilung Geobasis NRW" as Eigentuemer,
    current_date() AS Aktualitaet_Kachelinformationen,
    '1.2' AS Version_Standard,
    '1,2,14,17,18,20,24,26' AS Punktklassenbelegung,
    concat(Kachelname,'.laz') ,
    Aktualitaet,
    Erfassungsmethode,
    Fortfuehrung,
    Fortfuehrungsmethode,
    Aufloesung,
    Koordinatenreferenzsystem_Lage,
    Koordinatenreferenzsystem_Hoehe,
    Hoehenanomalie
FROM
    geospatial.tile_metadata;
    
    
select * from tile_metadata_ice;

select m.land, t.source_file,geohash, count(*)
 from tile_metadata_ice m, point_cloud_part t
 where t.source_file = m.kachelname
 group by m.land, t.source_file,geohash;



-- Query with Hive
select * from tile_metadata_ice;

select m.land, t.source_file,geohash, count(*)
 from tile_metadata_ice m, point_cloud_part t
 where t.source_file = m.kachelname
 group by m.land, t.source_file,geohash;



