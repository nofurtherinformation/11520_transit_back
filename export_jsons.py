# -*- coding: utf-8 -*-
'''
----------------------------------
- Export JSONs
----------------------------------
'''
# libraries
import psycopg2
import time
import sys
import math
import os

reload(sys)
sys.setdefaultencoding('utf8')

# arguments
demo_path = sys.argv[1]
output_path = sys.argv[2] if len(sys.argv) > 2 else './outputs/'

# start time
print ' '
print '--Starting ' + sys.argv[0] + '--'
print ' '

# Establish a Postgres connection
db_host 	= 'localhost'
db_port 	= '5432'
db_user 	= 'jonathanleape'
db_passwd	= 'mejorparatodos'
db_database = 'gtfs'

try:
	database = psycopg2.connect(database = db_database, user = db_user, password = db_passwd, host = db_host, port = db_port)
	print 'database connection successful.'
except:
	print 'database connection failed.'

cursor = database.cursor()


def export_geojson(filepath, input_geom, id):

    # export geojson
    cursor.execute("""
        COPY (
        SELECT jsonb_build_object(
            'type',     'FeatureCollection',
            'features', jsonb_agg(features.feature)
        )
        FROM (
        SELECT jsonb_build_object(
            'type',       'Feature',
            '""" + id + """',         """ + id + """,
            'geometry',   ST_AsGeoJSON(ST_Transform(the_geom,4326))::jsonb,
            'properties', to_jsonb(inputs) - 'the_geom'
        ) AS feature
        FROM (SELECT * FROM """ + input_geom + """) inputs) features)
        TO '""" + filepath + input_geom + """.geojson';
    """)
    database.commit()


def export_json(filepath, input):

    # export json
    cursor.execute("""
        COPY (
        SELECT array_to_json(array_agg(row_to_json(t)))  
        FROM (  
            SELECT * FROM """ + input + """
        ) t )
        TO '""" + filepath + input + """.json';
    """)
    database.commit()

print 'Exporting transit geojsons.'

export_geojson(output_path,'route_shapes', 'route_id')
export_geojson(output_path,'route_catchments', 'route_id')
export_geojson(output_path,'route_stops', 'route_id')
export_geojson(output_path,'route_stop_catchments', 'route_id')

for filename in os.listdir(demo_path):
    
	if filename.endswith(".geojson"): 
		
		f = os.path.splitext(filename)[0]

        print 'Eporting results for ' + f + ' demographics.'
        export_json(output_path,'route_profiles_' + f)
        export_json(output_path,'results_' + f)
        export_geojson(output_path, f + '_dots', 'demographic')
        export_json(output_path, 'stop_profiles_' + f)
        export_json(output_path, 'routes_' + f)

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--GeoJSON and JSON files exported--'
print ' '