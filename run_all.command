#!/bin/bash
# This runs all scripts to analyze collinearity of shapes in GTFS feeds

gtfs_path="/Users/jonathanleape/Documents/11.520/inputs/gtfs"
import_gtfs_path="/Users/jonathanleape/Documents/11.520/11520_transit_back/gtfs_SQL_importer/src"
process_path="/Users/jonathanleape/Documents/11.520/11520_transit_back"

# Import GTFS
cd $import_gtfs_path

dropdb gtfs
createdb gtfs -U jonathanleape -w

cat gtfs_tables.sql \
  <(python import_gtfs_to_sql.py $gtfs_path) \
  gtfs_tables_makespatial.sql \
  gtfs_tables_makeindexes.sql \
| psql gtfs
# psql gtfs -c "\dt"

cd $process_path
python collinear_index.py

# import demographics geojsons
ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" "/Users/jonathanleape/Documents/11.520/inputs/demographics/atl_race_2016.geojson" -nln atl_race_2016
# ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" "/Users/jonathanleape/Documents/11.520/inputs/demographics/atl_race_2016.shp" -skip-failures -nlt PROMOTE_TO_MULTI -nln atl_race_2016_shp
python import_demographics.py
python chi2_stat.py
python export_jsons.py

# GeoJSON

exit;