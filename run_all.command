#!/bin/bash
# This runs all scripts to analyze collinearity of shapes in GTFS feeds
city="atlanta"
dots_per_person=100

input_path="/Users/jonathanleape/Documents/11.520/shared/"$city"/2_postgis_inputs/"
gtfs_path=$input_path"gtfs/"
demo_path=$input_path"demographics/race/"
echo $gtfs_path

backend_path="/Users/jonathanleape/Documents/11.520/11520_transit_back/"
import_gtfs_path=$backend_path"gtfs_SQL_importer/src/"

output_path="/Users/jonathanleape/Documents/11.520/shared/"$city"/3_postgis_outputs/"

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

cd $backend_path
python collinear_index.py

# Import demographics geojsons
FILEPATHS=$demo_path"*.geojson"
t=$(basename "$file1")
name=$(echo "$t" | sed -e 's/\.[^.]*$//')
for fp in $FILEPATHS
do
  file=$(basename "$fp") 
  table=$(basename "$file" | sed -e 's/\.[^.]*$//') 
  echo "Importing $table file..."
  ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" $fp -nln $table -lco GEOMETRY_NAME="the_geom"
done

# # Import demographics shapefiles
# FILEPATHS=$demo_path
# t=$(basename "$file1")
# name=$(echo "$t" | sed -e 's/\.[^.]*$//')
# for fp in $FILEPATHS
# do
#   file=$(basename "$fp") 
#   table=$(basename "$file" | sed -e 's/\.[^.]*$//') 
#   echo "Importing $table file..."
#   ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" $fp -skip-failures -nlt PROMOTE_TO_MULTI -nln $table -lco GEOMETRY_NAME="the_geom"
# done


# ogr2ogr -f "PostgreSQL" PG:"dbname=gtfs user=jonathanleape" $fp -skip-failures -nlt PROMOTE_TO_MULTI -nln $table

# Process Data
python import_demographics.py $demo_path $dots_per_person
python chi2_stat.py $demo_path 'polygons' # ['centroids', 'dots $dots_per_person', 'polygons']

# # Export Results
python combine_results.py $demo_path .25 .05 # [$demo_path, min_collinear_index, max_p_val]
python export_jsons.py $demo_path $output_path

exit;