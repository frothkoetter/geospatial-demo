--
-- Run a spatial query.
--

use geospatial;

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

