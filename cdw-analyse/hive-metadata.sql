--
-- Handling meta data 
--
-- requirement: uploaded CSV files should be uploaded to the S3 bucket
-- URL: https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_meta.zip
--
USE geospatial;

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



