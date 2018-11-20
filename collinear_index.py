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


def pop_catchment_overlap(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS catchment_overlap')
	print 'Dropped catchment_overlap table.'

	cursor.execute("""
		CREATE TABLE catchment_overlap (shape_id TEXT, shape_id2 TEXT);
		""")

	cursor.execute("""
		SELECT AddGeometryColumn('catchment_overlap', 'overlap', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO catchment_overlap
		(shape_id, shape_id2, overlap)
		SELECT
			sc.shape_id,
			sc2.shape_id,
			ST_INTERSECTION(sc.catchment, sc2.catchment) AS overlap
		FROM
			shape_catchment AS sc,
			shape_catchment AS sc2
		WHERE
			sc.shape_id != sc2.shape_id AND
			ST_IsEmpty(ST_INTERSECTION(sc.catchment, sc2.catchment)) = FALSE;
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


def calc_collinear_index():

	cursor.execute('DROP TABLE IF EXISTS collinear_index')
	print 'Dropped collinear_index table.'

	cursor.execute("""
		CREATE TABLE collinear_index (shape_id TEXT, shape_id2 TEXT, area1 REAL, area2 REAL, overlap_area REAL, index REAL);
		""")

	# with filtering

	# cursor.execute("""
	# 	INSERT INTO collinear_index
	# 	(shape_id, shape_id2, area1, area2, overlap_area, index)
	# 	SELECT
	# 		co.shape_id,
	# 		co.shape_id2,
	# 		ST_AREA(sc1.catchment),
	# 		ST_AREA(sc2.catchment),
	# 		ST_AREA(co.overlap),
	# 		ST_AREA(co.overlap) / ST_AREA(sc1.catchment)
	# 	FROM
	# 		shape_catchment AS sc1,
	# 		shape_catchment AS sc2,
	# 		catchment_overlap AS co,
	# 		gtfs_trips AS t1,
	# 		gtfs_trips AS t2,
	# 		gtfs_routes AS r1,
	# 		gtfs_routes AS r2
	# 	WHERE
	# 		co.shape_id = sc1.shape_id AND
	# 		co.shape_id2 = sc2.shape_id AND
	# 		co.shape_id = t1.shape_id AND
	# 		co.shape_id2 = t2.shape_id AND
	# 		t1.route_id = r1.route_id AND
	# 		t2.route_id = r2.route_id AND
	# 		r1.route_id != r2.route_id AND
	# 		t1.service_id = t2.service_id
	# 	ORDER BY ST_AREA(co.overlap) / ST_AREA(sc1.catchment) DESC;
	# 	""")
	
	cursor.execute("""
		INSERT INTO collinear_index
		(shape_id, shape_id2, area1, area2, overlap_area, index)
		SELECT
			co.shape_id,
			co.shape_id2,
			ST_AREA(sc1.catchment),
			ST_AREA(sc2.catchment),
			ST_AREA(co.overlap),
			ST_AREA(co.overlap) / ST_AREA(sc1.catchment)
		FROM
			shape_catchment AS sc1,
			shape_catchment AS sc2,
			catchment_overlap AS co
		WHERE
			co.shape_id = sc1.shape_id AND
			co.shape_id2 = sc2.shape_id
		ORDER BY ST_AREA(co.overlap) / ST_AREA(sc1.catchment) DESC;
		""")

	database.commit()


utm_code = get_utm_code('gtfs_stops')
project_geom('gtfs_shape_geoms', utm_code)
project_geom('gtfs_stops', utm_code)
pop_shape_catchment(utm_code)
pop_catchment_overlap(utm_code)
# filter_overlap(utm_code)
calc_collinear_index()


# def catchment_demographics():

# def overlap_demographics():

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--Finished--'
print ' '