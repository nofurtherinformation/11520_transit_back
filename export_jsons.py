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

reload(sys)
sys.setdefaultencoding('utf8')

# arguments
# path_gtfs = sys.argv[1] if len(sys.argv) > 1 else ''

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

	# get geometry type
    cursor.execute('SELECT ST_GeometryType(the_geom) FROM ' + input_geom + ';')
    geom_type = cursor.fetchone()[0].replace('ST_','').upper()
    print 'geometry type is: ' + geom_type

    # project to wgs84
    cursor.execute('DROP TABLE IF EXISTS ' + input_geom + '_wgs84;')
    cursor.execute('CREATE TABLE ' + input_geom + '_wgs84 AS SELECT * FROM ' + input_geom + ';')
    cursor.execute('ALTER TABLE ' + input_geom + '_wgs84 ALTER COLUMN the_geom TYPE geometry(' + geom_type + ',4326) USING ST_Transform(the_geom,4326);')
    database.commit()

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
            'id',         """ + id + """,
            'geometry',   ST_AsGeoJSON(the_geom)::jsonb,
            'properties', to_jsonb(inputs) - '""" + id + """' - 'the_geom'
        ) AS feature
        FROM (SELECT * FROM """ + input_geom + """_wgs84) inputs) features)
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


filepath = '/Users/jonathanleape/Documents/11.520/outputs/'

export_geojson(filepath,'route_shapes', 'route_id')
export_geojson(filepath,'shape_catchments', 'route_id')
export_geojson(filepath,'route_stops', 'route_id')
export_geojson(filepath,'route_stop_catchments', 'route_id')
export_geojson(filepath,'atl_race_2016_dots', 'ethnicity')

export_json(filepath,'gtfs_routes')
demographics = 'atl_race_2016'
export_json(filepath,'route_' + demographics)
export_json(filepath,'results_' + demographics)