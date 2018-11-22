# -*- coding: utf-8 -*-
'''
----------------------------------
- Calculate Chi-squared Statistic
----------------------------------
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

def stop_catchments(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS stop_catchments')

	cursor.execute('CREATE TABLE stop_catchments (stop_id TEXT, stop_name TEXT);')

	cursor.execute("""
		SELECT AddGeometryColumn('stop_catchments', 'catchment', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO stop_catchments
		(stop_id, stop_name, catchment)
		SELECT
			stop_id,
			stop_name,
			ST_BUFFER(the_geom, 500, 'quad_segs=8') AS catchment
		FROM
			gtfs_stops;
		""")

	database.commit()


def route_stops():

	cursor.execute('DROP TABLE IF EXISTS route_stops')

	cursor.execute('CREATE TABLE route_stops (route_id TEXT, stop_id TEXT, stop_sequence INT);')

	cursor.execute("""
		INSERT INTO route_stops
		SELECT DISTINCT ON (route_id, stop_id)
			t.route_id,
			s.stop_id,
			st.stop_sequence
		FROM
			(SELECT DISTINCT ON (route_id) route_id, trip_id FROM gtfs_trips) AS t,
			gtfs_stops AS s,
			gtfs_stop_times AS st
		WHERE
			t.trip_id = st.trip_id AND
			s.stop_id = st.stop_id
		ORDER BY
			route_id,
			stop_id,
			stop_sequence;
	""")

	database.commit()

def stop_demographics(filename):

	cursor.execute('DROP TABLE IF EXISTS stop_' + filename)

	cursor.execute("""
		CREATE TABLE stop_""" + filename + """ (
			stop_id TEXT, 
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

	cursor.execute("""
		INSERT INTO stop_""" + filename + """
		SELECT
			stop_id,
			COALESCE(SUM(""" + filename + """.am_indian),0) AS am_indian, 
			COALESCE(SUM(""" + filename + """.asian),0) AS asian,
			COALESCE(SUM(""" + filename + """.black),0) AS black,
			COALESCE(SUM(""" + filename + """.latino),0) AS latino,
			COALESCE(SUM(""" + filename + """.pacific),0) AS pacific,
			COALESCE(SUM(""" + filename + """.white),0) AS white,
			COALESCE(SUM(""" + filename + """.mixed),0) AS mixed,
			COALESCE(SUM(""" + filename + """.other),0) AS other,
			COALESCE(SUM(""" + filename + """.total),0) AS total
		FROM
			stop_catchments
			LEFT JOIN """ + filename + """
			ON ST_Intersects(stop_catchments.catchment, """ + filename + """.the_geom)
		GROUP BY
			stop_id
		""")

	database.commit()


def route_demographics(filename):

	cursor.execute('DROP TABLE IF EXISTS route_' + filename)

	cursor.execute("""
		CREATE TABLE route_""" + filename + """ (
			route_id TEXT, 
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

	cursor.execute("""
		INSERT INTO route_""" + filename + """
		SELECT
			route_id,
			SUM(stop_""" + filename + """.am_indian) AS am_indian, 
			SUM(stop_""" + filename + """.asian) AS asian,
			SUM(stop_""" + filename + """.black) AS black,
			SUM(stop_""" + filename + """.latino) AS latino,
			SUM(stop_""" + filename + """.pacific) AS pacific,
			SUM(stop_""" + filename + """.white) AS white,
			SUM(stop_""" + filename + """.mixed) AS mixed,
			SUM(stop_""" + filename + """.other) AS other,
			SUM(stop_""" + filename + """.total) AS total
		FROM
			route_stops,
			stop_""" + filename + """
		WHERE
			route_stops.stop_id = stop_""" + filename + """.stop_id
		GROUP BY
			route_id;
		""")
	
	database.commit()

def chi2_stat(filename):

	cursor.execute('DROP TABLE IF EXISTS route_' + filename)

	cursor.execute("""
		CREATE TABLE route_""" + filename + """ (
			route_id TEXT, 

epsg_code = 32616
stop_catchments(epsg_code)
route_stops()
stop_demographics('atl_race_2016')
route_demographics('atl_race_2016')


