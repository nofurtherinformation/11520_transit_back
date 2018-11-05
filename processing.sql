CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;

-- Get UTM zone
-- https://stackoverflow.com/questions/32821176/convert-from-epsg4326-to-utm-in-postgis
CREATE OR REPLACE FUNCTION get_utmzone(input_geom geometry)
  RETURNS integer AS
$BODY$
DECLARE
  zone int;
  pref int;
BEGIN
  IF GeometryType(input_geom) != 'POINT' THEN
    RAISE EXCEPTION 'Input geom must be a point. Currently is: %', GeometryType(input_geom);
  END IF;
  IF ST_Y(input_geom) >0 THEN
    pref:=32600;
  ELSE
    pref:=32700;
  END IF;
  zone = floor((ST_X(input_geom)+180)/6)+1;
  RETURN zone+pref;
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE;

-- Project to UTM
ALTER TABLE gtfs_shapes
 ALTER COLUMN geom TYPE geometry(LINESTRING,4326) 
  USING ST_Transform(geom,SELECT get_utmzone(ST_GeomFromText(ST_Centroid(geom)),4326));

-- 
CREATE TABLE IF NOT EXISTS shape_pairs (
  shape_pair_id TEXT PRIMARY KEY,
  pair_polygon GEOMETRY(POLYGON,4326) NOT NULL
);

INSERT INTO shape_pairs
			(shape_pair_id, route_id, zeroed_position, sequence, stop_id, stop_name, stop_lat, stop_lon, stop_type, stop_headsign)
		SELECT
			d.id AS id,
			CONCAT(d.agency_id, '--', d.linea, '--', d.ruta) AS route_id,
			(d.posicion - COALESCE(r.min_pos, 0)) AS zeroed_position,
			d.sequence AS sequence,
			COALESCE(n.stop_id, d.a_nodo) AS stop_id,
			COALESCE(n.stop_name, d.nombrenodo) AS stop_name,
			COALESCE(n.stop_lat, d.nodo_lat) AS stop_lat,
			COALESCE(n.stop_lon, d.nodo_lon) AS stop_lon,
			COALESCE(n.stop_type, 'U') AS stop_type,
			d.stop_headsign AS stop_headsign
		FROM
			distmatrix AS d
		LEFT OUTER JOIN nodes_refstop AS n
			ON d.a_nodo = n.a_nodo
		LEFT OUTER JOIN routes AS r
			ON CONCAT(d.agency_id, '--', d.linea, '--', d.ruta) = r.route_id
		WHERE
			route_id IN (SELECT DISTINCT route_id FROM trips);

CREATE TABLE IF NOT EXISTS parallelism_shapes (
  shape_id TEXT PRIMARY KEY,
  route_id TEXT NOT NULL,
  parallelism NUMERIC(16,10) NOT NULL
);

CREATE TABLE IF NOT EXISTS parallelism_routes (
  route_id TEXT PRIMARY KEY,
  route_short_name TEXT NOT NULL,
  parallelism NUMERIC(16,10) NOT NULL
);