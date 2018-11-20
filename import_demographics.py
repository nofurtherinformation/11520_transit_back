# -*- coding: utf-8 -*-
'''
--------------------------------
- Calculate Parallelism Index
--------------------------------
'''
# libraries
import psycopg2
import time
import sys
import math
import utm
import osgeo.ogr
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


def pop_shape_catchment(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS shape_catchment')
	print 'Dropped shape_catchment table.'

	cursor.execute("""
		CREATE TABLE shape_catchment (shape_id TEXT);
		""")

	cursor.execute("""
		SELECT AddGeometryColumn('shape_catchment', 'catchment', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO shape_catchment
		(shape_id, catchment)
		SELECT
			shape_id,
			ST_BUFFER(the_geom, 800, 'endcap=round join=round') AS catchment
		FROM
			gtfs_shape_geoms
		GROUP BY 
		shape_id, 
		the_geom;
		""")

	cursor.execute("""
		CREATE INDEX "shape_catchment_gist" ON "shape_catchment" using gist ("catchment");
		""")
	
	database.commit()

	print 'shape_catchment created.'


def import_