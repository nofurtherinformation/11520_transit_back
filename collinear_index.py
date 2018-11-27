# -*- coding: utf-8 -*-
'''
--------------------------------
- Calculate Collinearity Index
--------------------------------
'''
# libraries
import psycopg2
import time
import sys
import math
import utm
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
	cursor.execute('SELECT ST_X(ST_Transform(ST_Centroid(the_geom),4326)) FROM ' + input_geom + ';')
	centroid_lon = cursor.fetchone()[0]
	print 'centroid lon is: ' + str(centroid_lon)

	cursor.execute('SELECT ST_Y(ST_Transform(ST_Centroid(the_geom),4326)) FROM ' + input_geom + ';')
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


def route_shapes(epsg_code):
	
	cursor.execute('DROP TABLE IF EXISTS route_shapes')
	print 'Dropped route_shapes table.'

	cursor.execute('CREATE TABLE route_shapes (route_id TEXT, shape_id TEXT);')

	cursor.execute("""
		SELECT AddGeometryColumn('route_shapes', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_shapes
		SELECT DISTINCT ON (route_id, shape_id)
			t.route_id,
			sh.shape_id,
			sh.the_geom
		FROM
			(SELECT DISTINCT ON (route_id) route_id, trip_id, shape_id FROM gtfs_trips) AS t,
			gtfs_shape_geoms AS sh
		WHERE
			t.shape_id = sh.shape_id
		ORDER BY
			route_id;
		""")

	database.commit()

	print 'route_shapes created.'
	

def route_catchments(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS route_catchments')
	print 'Dropped route_catchments table.'

	cursor.execute("""
		CREATE TABLE route_catchments (route_id TEXT);
		""")

	cursor.execute("""
		SELECT AddGeometryColumn('route_catchments', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_catchments
		(route_id, the_geom)
		SELECT
			rs.route_id,
			ST_BUFFER(the_geom, 500, 'endcap=round join=round') AS the_geom
		FROM
			route_shapes AS rs;
		""")

	cursor.execute("""
		CREATE INDEX "route_catchments_gist" ON "route_catchments" using gist ("the_geom");
		""")
	
	database.commit()

	print 'route_catchments created.'


def catchment_overlap(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS catchment_overlap')
	print 'Dropped catchment_overlap table.'

	cursor.execute("""
		CREATE TABLE catchment_overlap (route_id TEXT, route_id2 TEXT);
		""")

	cursor.execute("""
		SELECT AddGeometryColumn('catchment_overlap', 'overlap', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO catchment_overlap
		(route_id, route_id2, overlap)
		SELECT
			sc.route_id,
			sc2.route_id AS route_id2,
			ST_INTERSECTION(sc.the_geom, sc2.the_geom) AS overlap
		FROM
			route_catchments AS sc,
			route_catchments AS sc2
		WHERE
			sc.route_id != sc2.route_id AND
			ST_IsEmpty(ST_INTERSECTION(sc.the_geom, sc2.the_geom)) = FALSE;
		""")

	cursor.execute("""
		CREATE INDEX "catchment_overlap_gist" ON "catchment_overlap" using gist ("overlap");
		""")

	database.commit()

	print 'catchment_overlap created.'


def filter_overlap(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS filtered_overlap')
	print 'Dropped filtered_overlap table.'

	cursor.execute("""
		CREATE TABLE filtered_overlap (shape_id TEXT, shape_id2 TEXT);
		""")

	cursor.execute("""
		SELECT AddGeometryColumn('filtered_overlap', 'overlap', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO filtered_overlap
		(shape_id, shape_id2, overlap)
		SELECT
			co.shape_id,
			co.shape_id2,
			co.overlap
		FROM
			catchment_overlap AS co,
			gtfs_trips AS t1,
			gtfs_trips AS t2,
			gtfs_routes AS r1,
			gtfs_routes AS r2
		WHERE
			co.shape_id = t1.shape_id AND
			co.shape_id2 = t2.shape_id AND
			t1.route_id = r1.route_id AND
			t2.route_id = r2.route_id AND
			r1.route_id != r2.route_id AND
			t1.service_id = t2.service_id AND
			co.shape_id != co.shape_id2 AND
			ST_AREA(co.overlap) > 0
		ORDER BY
			co.shape_id,
			co.shape_id2;
		""")

	cursor.execute("""
		CREATE INDEX "filtered_overlap_gist" ON "filtered_overlap" using gist ("overlap");
		""")

	database.commit()

	print 'catchment_overlap filtered as filtered_overlap.'


def collinear_index():

	cursor.execute('DROP TABLE IF EXISTS collinear_index')
	print 'Dropped collinear_index table.'

	cursor.execute("""
		CREATE TABLE collinear_index (route_id TEXT, route_id2 TEXT, area1 REAL, area2 REAL, overlap_area REAL, index REAL);
		""")

	cursor.execute("""
		INSERT INTO collinear_index
		(route_id, route_id2, area1, area2, overlap_area, index)
		SELECT
			co.route_id,
			co.route_id2,
			ST_AREA(sc1.the_geom),
			ST_AREA(sc2.the_geom),
			ST_AREA(co.overlap),
			ST_AREA(co.overlap) / ST_AREA(sc1.the_geom)
		FROM
			route_catchments AS sc1,
			route_catchments AS sc2,
			catchment_overlap AS co
		WHERE
			co.route_id = sc1.route_id AND
			co.route_id2 = sc2.route_id
		ORDER BY ST_AREA(co.overlap) / ST_AREA(sc1.the_geom) DESC;
		""")

	database.commit()


utm_code = get_utm_code('gtfs_stops')
project_geom('gtfs_shape_geoms', utm_code)
project_geom('gtfs_stops', utm_code)
route_shapes(utm_code)
route_catchments(utm_code)
catchment_overlap(utm_code)
# filter_overlap(utm_code)
collinear_index()


# def catchment_demographics():

# def overlap_demographics():

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--Finished--'
print ' '