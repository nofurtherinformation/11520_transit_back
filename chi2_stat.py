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
		SELECT AddGeometryColumn('stop_catchments', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO stop_catchments
		(stop_id, stop_name, the_geom)
		SELECT
			stop_id,
			stop_name,
			ST_BUFFER(the_geom, 500, 'quad_segs=8') AS the_geom
		FROM
			gtfs_stops;
		""")

	database.commit()


def route_stops(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS route_stops')

	cursor.execute('CREATE TABLE route_stops (route_id TEXT, stop_id TEXT, stop_sequence INT);')

	cursor.execute("""
		SELECT AddGeometryColumn('route_stops', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_stops
		SELECT DISTINCT ON (route_id, stop_id)
			t.route_id,
			s.stop_id,
			st.stop_sequence,
			s.the_geom
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


def route_stop_catchments(epsg_code):

	cursor.execute('DROP TABLE IF EXISTS route_stop_catchments')

	cursor.execute('CREATE TABLE route_stop_catchments (route_id TEXT, stop_id TEXT, stop_sequence INT);')

	cursor.execute("""
		SELECT AddGeometryColumn('route_stop_catchments', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_stop_catchments
		SELECT
			rs.route_id,
			rs.stop_id,
			rs.stop_sequence,
			sc.the_geom
		FROM
			route_stops AS rs,
			stop_catchments AS sc
		WHERE
			rs.stop_id = sc.stop_id;
		""")
	
	database.commit()


def stop_demographics(filename, inputType):

	cursor.execute('DROP TABLE IF EXISTS stop_' + filename)

	cursor.execute("""
		CREATE TABLE stop_""" + filename + """ (
			stop_id TEXT, 
			am_indian REAL, 
			asian REAL,
			black REAL,
			latino REAL,
			pacific REAL,
			white REAL,
			mixed REAL,
			other REAL,
			total REAL);
		""")

	if inputType == 'centroids':

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
				ON ST_Intersects(stop_catchments.the_geom, """ + filename + """.the_geom)
			GROUP BY
				stop_id
			""")

	elif inputType == 'dots':

		cursor.execute("""
			INSERT INTO stop_""" + filename + """
			SELECT
				sc.stop_id,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'am_indian') as am_indian, 
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'asian') as asian,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'black') as black,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'latino') as latino,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'pacific') as pacific,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'white') as white,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'mixed') as mixed,
				COUNT(dump.ethnicity) FILTER (WHERE ethnicity = 'other') as other,
				COUNT(dump.ethnicity) as total
			FROM 
				stop_catchments AS sc,
				(SELECT ethnicity, (ST_Dump(the_geom)).geom AS the_geom FROM atl_race_2016_dots) AS dump
			WHERE 
				ST_Contains(sc.the_geom,dump.the_geom)
			GROUP BY
				sc.stop_id;
			""")

			# SELECT 
			# 	sc.stop_id,
			# 	dump.ethnicity
			# FROM 
			# 	stop_catchments AS sc,
			# 	(SELECT ethnicity, (ST_Dump(the_geom)).geom AS the_geom FROM atl_race_2016_dots) AS dump
			# WHERE 
			# 	ST_Contains(sc.the_geom,dump.the_geom);

	database.commit()


def route_demographics(filename):

	cursor.execute('DROP TABLE IF EXISTS route_' + filename)

	cursor.execute("""
		CREATE TABLE route_""" + filename + """ (
			route_id TEXT, 
			am_indian REAL, 
			asian REAL,
			black REAL,
			latino REAL,
			pacific REAL,
			white REAL,
			mixed REAL,
			other REAL,
			total REAL);
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

	cursor.execute('DROP TABLE IF EXISTS chi2_' + filename)

	cursor.execute("""
		CREATE TABLE chi2_""" + filename + """ (
			route_id TEXT,
			route_id2 TEXT,
			chi2 REAL,
			p_val REAL);
		""")

	# 8 - 1 = 7 degrees of freedom => significant at 5% if chi2 over 14.067
	cursor.execute("""
		INSERT INTO chi2_""" + filename + """
		SELECT
			r1.route_id,
			r2.route_id AS route_id2,
			CASE WHEN r1.total = 0 THEN 0 ELSE 
			COALESCE((r1.am_indian/r1.total - r2.am_indian/r2.total)^2 / (NULLIF(r1.am_indian,0)/r1.total),0) + 
			COALESCE((r1.asian/r1.total - r2.asian/r2.total)^2 / (NULLIF(r1.asian,0)/r1.total),0) + 
			COALESCE((r1.black/r1.total - r2.black/r2.total)^2 / (NULLIF(r1.black,0)/r1.total),0) + 
			COALESCE((r1.latino/r1.total - r2.latino/r2.total)^2 / (NULLIF(r1.latino,0)/r1.total),0) + 
			COALESCE((r1.pacific/r1.total - r2.pacific/r2.total)^2 / (NULLIF(r1.pacific,0)/r1.total),0) + 
			COALESCE((r1.white/r1.total - r2.white/r2.total)^2 / (NULLIF(r1.white,0)/r1.total),0) + 
			COALESCE((r1.mixed/r1.total - r2.mixed/r2.total)^2 / (NULLIF(r1.mixed,0)/r1.total),0) + 
			COALESCE((r1.other/r1.total - r2.other/r2.total)^2 / (NULLIF(r1.other,0)/r1.total),0)
			END AS chi2,
			1 AS p_val
		FROM
			route_""" + filename + """ AS r1,
			route_""" + filename + """ AS r2
		WHERE
			r1.route_id != r2.route_id
		ORDER BY
			chi2 DESC;
		""")

	database.commit()


def results(filename):

	cursor.execute('DROP TABLE IF EXISTS results_' + filename)

	cursor.execute("""
		CREATE TABLE results_""" + filename + """ (
			route_id TEXT,
			route_id2 TEXT,
			chi2 REAL,
			p_val REAL,
			collinear_index REAL);
		""")

	print 'Created results_' + filename
	
	cursor.execute("""
		INSERT INTO results_""" + filename + """
		SELECT
			ci.route_id,
			ci.route_id2,
			ch.chi2,
			ch.p_val,
			ci.index AS collinear_index
		FROM
			collinear_index AS ci,
			chi2_""" + filename + """ AS ch
		WHERE
			ci.route_id = ch.route_id AND
			ci.route_id2 = ch.route_id2
		ORDER BY
			ci.index DESC;
		""")
	
	database.commit()

epsg_code = 32616
stop_catchments(epsg_code)
route_stops(epsg_code)
route_stop_catchments(epsg_code)
stop_demographics('atl_race_2016', 'dots')
route_demographics('atl_race_2016')
chi2_stat('atl_race_2016')
results('atl_race_2016')