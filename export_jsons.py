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
print '--Starting--'
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

	# # get geometry type
    # cursor.execute('SELECT ST_GeometryType(the_geom) FROM ' + input_geom + ';')
    # geom_type = cursor.fetchone()[0].replace('ST_','').upper()
    # print 'geometry type is: ' + geom_type

    # # project to wgs84
    # cursor.execute('DROP TABLE IF EXISTS ' + input_geom + '_wgs84;')
    # cursor.execute('CREATE TABLE ' + input_geom + '_wgs84 AS SELECT * FROM ' + input_geom + ';')
    # cursor.execute('ALTER TABLE ' + input_geom + '_wgs84 ALTER COLUMN the_geom TYPE geometry(' + geom_type + ',4326) USING ST_Transform(the_geom,4326);')
    # database.commit()

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
            """ + id + """,         """ + id + """,
            'geometry',   ST_AsGeoJSON(ST_Transform(the_geom,4326))::jsonb,
            'properties', to_jsonb(inputs) - '""" + id + """' - 'the_geom'
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

export_json(output_path,'gtfs_routes')

for filename in os.listdir(demo_path):
    
	if filename.endswith(".geojson"): 
		
		f = os.path.splitext(filename)[0]
		
		print 'Eporting results for ' + f + ' demographics.'
        export_json(output_path,'route_' + f)
        export_json(output_path,'results_' + f)
        export_geojson(output_path, f + '_dots', 'demographic')