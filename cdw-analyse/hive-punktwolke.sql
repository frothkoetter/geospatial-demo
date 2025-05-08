--
-- create Iceberg table for NIFI flow
--
CREATE TABLE IF NOT EXISTS geospatial.punktwolke (
    x DOUBLE COMMENT 'X coordinate',
    y DOUBLE COMMENT 'Y coordinate',
    z DOUBLE COMMENT 'Z coordinate/elevation',
    intensity INT COMMENT 'Return intensity value',
    return_num INT COMMENT 'Return number',
    classification INT COMMENT 'Point classification'
)
stored by ICEBERG
stored as PARQUET;

