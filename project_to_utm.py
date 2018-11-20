# -*- coding: utf-8 -*-
'''
--------------------------------
- Functions to project to UTM
--------------------------------
'''

import psycopg2
import time
import sys
import math
import utm

def get_utm_code(database,input_geom):
    
    cursor = database.cursor()
    
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