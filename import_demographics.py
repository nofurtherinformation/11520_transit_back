# -*- coding: utf-8 -*-
'''
--------------------------------
- Import Demographic Data
--------------------------------
'''
# libraries
import psycopg2
import time
import sys
import math
import utm
# import osgeo.ogr
# local
# from project_to_utm import get_utm_code, project_geom

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


def get_utm_code(input_geom):

	# get centroid coords
	cursor.execute('SELECT ST_X(ST_Transform(ST_Centroid(the_geom),4236)) FROM ' + input_geom + ';')
	centroid_lon = cursor.fetchone()[0]
	print 'centroid lon is: ' + str(centroid_lon)

	cursor.execute('SELECT ST_Y(ST_Transform(ST_Centroid(the_geom),4236)) FROM ' + input_geom + ';')
	centroid_lat = cursor.fetchone()[0]
	print 'centroid lat is: ' + str(centroid_lat)

	database.commit()

	# determine UTM zone
	utm_band = math.floor((centroid_lon + 180) / 6 ) % 60 + 1
	if centroid_lat >= 0:
		epsg_code = 32600 + utm_band
	else:
		epsg_code = 32700 + utm_band
	print 'UTM EPSG code is: ' + str(epsg_code)
	return int(epsg_code)


def project_geom(input_geom, epsg_code):

	# get original SRID
	cursor.execute('SELECT ST_SRID(the_geom) FROM ' + input_geom + ';')
	original_SRID = str(cursor.fetchone()[0])
	print 'original SRID is: ' + original_SRID

	# get geometry type
	cursor.execute('SELECT ST_GeometryType(the_geom) FROM ' + input_geom + ';')
	geom_type = cursor.fetchone()[0].replace('ST_','').upper()
	print 'geometry type is: ' + geom_type

	project_sql = 'ALTER TABLE ' + input_geom + ' ALTER COLUMN the_geom TYPE geometry(' + geom_type + ',' + str(epsg_code) + ') USING ST_Transform(the_geom,' + str(epsg_code) + ');'
	cursor.execute(project_sql)

	database.commit()


def import_demographics_csv(filename, epsg_code):

    filepath = demographics_path + filename + '.csv'

    cursor.execute('DROP TABLE IF EXISTS ' + filename)
    print 'Dropped ' + filename + ' table.'

    cursor.execute("""
    CREATE TABLE """ + filename + """(
        GEOID TEXT,
        lon DOUBLE PRECISION,
        lat DOUBLE PRECISION,
        am_indian DOUBLE PRECISION, 
        asian DOUBLE PRECISION,
        black DOUBLE PRECISION,
        latino DOUBLE PRECISION,
        pacific DOUBLE PRECISION,
        white DOUBLE PRECISION,
        mixed DOUBLE PRECISION,
        other DOUBLE PRECISION,
        total DOUBLE PRECISION);
        """)

    cursor.execute("COPY " + filename + " FROM '" + filepath + "' CSV HEADER;")

    cursor.execute("SELECT AddGeometryColumn('" + filename + "', 'the_geom', " + str(epsg_code) + ", 'POINT', 2);")

    cursor.execute("UPDATE " + filename + " SET the_geom = ST_SetSRID(ST_MakePoint(lon, lat), 4236);")

    database.commit()

    print filename + ' centroids imported.'

demographics_path = '/Users/jonathanleape/Documents/11.520/inputs/demographics/'
filename = 'atl_race_2016'

import_demographics_csv(filename, 4236)
epsg_code = get_utm_code(filename)
project_geom(filename, epsg_code)

# def import_demographics_geojson(filepath):

# def import_demographics_shp(filepath):

# def polygons2centroids(polygons):

