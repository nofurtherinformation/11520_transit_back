#!/bin/bash
# This runs all scripts to analyze collinearity of shapes in GTFS feeds

gtfs_path="/Users/jonathanleape/Documents/11.520/inputs/atlanta/gtfs/"
demo_path="/Users/jonathanleape/Documents/11.520/inputs/atlanta/demographics/race/"

import_gtfs_path="/Users/jonathanleape/Documents/11.520/11520_transit_back/gtfs_SQL_importer/src/"
process_path="/Users/jonathanleape/Documents/11.520/11520_transit_back/"

output_path="/Users/jonathanleape/Documents/11.520/outputs/"

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

# Import demographics geojsons
FILEPATHS=/Users/jonathanleape/Documents/11.520/inputs/demographics/race/*.geojson

t=$(basename "$file1")                        # output is main.one.two.sh
name=$(echo "$t" | sed -e 's/\.[^.]*$//') 

for fp in $FILEPATHS
do
  file=$(basename "$fp") 
  table=$(basename "$file" | sed -e 's/\.[^.]*$//') 
  echo "Importing $table file..."
  ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" $fp -nln $table -lco GEOMETRY_NAME="the_geom"
done

# ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" "/Users/jonathanleape/Documents/11.520/inputs/demographics/atl_race_2016.shp" -skip-failures -nlt PROMOTE_TO_MULTI -nln atl_race_2016_shp

# Process Data
python import_demographics.py $demo_path 50
python chi2_stat.py $demo_path 'dots' 50

# Export Results
python export_jsons.py $demo_path $output_path

exit;