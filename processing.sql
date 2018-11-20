-- Catchment
CREATE TABLE IF NOT EXISTS shape_catchment (
  shape_id TEXT
);

SELECT AddGeometryColumn('shape_catchment', 'catchment', 4326, 'GEOMETRY', 2);

-- Catchment overlap
CREATE TABLE IF NOT EXISTS catchment_overlap (
  shape_id TEXT,
  shape_id2 TEXT
);

SELECT AddGeometryColumn('catchment_overlap', 'overlap', 4326, 'GEOMETRY', 2);

COMMIT;
