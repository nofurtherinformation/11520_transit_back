#!/bin/bash
# This runs all scripts to analyze parallelism and stop saturation for GTFS feeds

gtfs_path="/Users/jonathanleape/Documents/11.520/input_data/gtfs"
import_gtfs_path="/Users/jonathanleape/Documents/11.520/back_end/gtfs_SQL_importer/src"
TransitFeed_path="/Users/jonathanleape/TransitFeed"
process_path="/Users/jonathanleape/Documents/11.520/back_end"

# Validate GTFS
#cd TransitFeed_path;
#feedvalidator.py gtfsPath -m -l 100;

# Import GTFS
cd $import_gtfs_path
ls
dropdb gtfs
createdb gtfs -U jonathanleape -w
cat gtfs_tables.sql \
  <(python import_gtfs_to_sql.py $gtfs_path) \
  gtfs_tables_makespatial.sql \
  gtfs_tables_makeindexes.sql \
| psql gtfs
psql gtfs -c "\dt"

cd $process_path
cat processing.sql \
| psql gtfs
psql gtfs -c "\dt"

# Import demand data

# Calculate KPIs

# Import Twitter complaints

# Animation

# Parallelism
cd $process_path
python parallelism.py

# Stop Saturation

# GeoJSON
#cd /Users/jonathanleape/GoogleDrive/GTFS/gtfs_generator/data/outputs/zipped;
#python /Users/jonathanleape/Documents/MFC/Bogota/gtfs_generator_p/scripts/11_gtfs2geojson.py -o /Users/jonathanleape/Documents/MFC/Bogota/gtfs_generator_p/data/outputs/extras/bogota_routes.geojson -r bogota.zip;
#python /Users/jonathanleape/Documents/MFC/Bogota/gtfs_generator_p/scripts/11_gtfs2geojson.py -o /Users/jonathanleape/Documents/MFC/Bogota/gtfs_generator_p/data/outputs/extras/bogota_stops.geojson -s bogota.zip;

exit;