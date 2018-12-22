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
import os
from scipy.stats import chi2

reload(sys)
sys.setdefaultencoding('utf8')

# arguments

if len(sys.argv) == 3 and sys.argv[2] == 'dots':
	dpp = 10

if len(sys.argv) < 3:
	inputType = 'polygons'
else:
	inputType = sys.argv[2]

if len(sys.argv) < 2:
	demo_path = '/Users/jonathanleape/Documents/11.520/shared/atlanta/2_postgis_inputs/demographics/race/'
else: 
	demo_path = sys.argv[1]

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

def get_SRID(input_geom):

	cursor.execute('SELECT ST_SRID(the_geom) FROM ' + input_geom + ';')
	SRID = str(cursor.fetchone()[0])
	print 'The current layer SRID is: ' + SRID
	return SRID

def demographic_fields(table):

	cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + table + "';")
	columns = cursor.fetchall()
	fields = []
	for c in columns:
		if c[0] not in ('geoid', 'total', 'area1', 'the_geom', 'ogc_fid'):
			fields.append(c[0])
	return fields

def stop_catchments(epsg_code):

	print 'Creating stop_catchments layer of stop buffers.'
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

	print 'Creating route_stops layer of stop sequences by route.'
	cursor.execute('DROP TABLE IF EXISTS route_stops')
	cursor.execute('CREATE TABLE route_stops (route_id TEXT, direction_id INT, stop_id TEXT, stop_sequence INT);')
	cursor.execute("""
		SELECT AddGeometryColumn('route_stops', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_stops
		SELECT DISTINCT ON (route_id, direction_id, stop_id)
			t.route_id,
			t.direction_id,
			s.stop_id,
			st.stop_sequence,
			s.the_geom
		FROM
			(SELECT DISTINCT ON (route_id) route_id, direction_id, trip_id FROM gtfs_trips) AS t,
			gtfs_stops AS s,
			gtfs_stop_times AS st
		WHERE
			t.trip_id = st.trip_id AND
			s.stop_id = st.stop_id
		ORDER BY
			route_id,
			direction_id,
			stop_id,
			stop_sequence;
	""")

	database.commit()


def route_stop_catchments(epsg_code):

	print 'Creating route_stop_catchments layer of stop buffers by route.'
	cursor.execute('DROP TABLE IF EXISTS route_stop_catchments')
	cursor.execute('CREATE TABLE route_stop_catchments (route_id TEXT, direction_id INT, stop_id TEXT, stop_sequence INT);')
	cursor.execute("""
		SELECT AddGeometryColumn('route_stop_catchments', 'the_geom', """ + str(epsg_code) + """, 'GEOMETRY', 2);
		""")

	cursor.execute("""
		INSERT INTO route_stop_catchments
		SELECT
			rs.route_id,
			rs.direction_id,
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

	print 'Creating table of demographic profiles by stop with ' + filename + ' ' + inputType + '.'
	cursor.execute('DROP TABLE IF EXISTS stop_profiles_' + filename)

	cursor.execute("""
		CREATE TABLE stop_profiles_""" + filename + """ (
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
			INSERT INTO stop_profiles_""" + filename + """
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
			INSERT INTO stop_profiles_""" + filename + """
			SELECT
				sc.stop_id,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'am_indian') AS am_indian,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'asian') AS asian,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'black') AS black,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'latino') AS latino,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'pacific') AS pacific,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'white') AS white,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'mixed') AS mixed,
				""" + dpp + """ * COUNT(dump.demographic) FILTER (WHERE demographic = 'other') AS other,
				""" + dpp + """ * COUNT(dump.demographic) AS total
			FROM 
				stop_catchments AS sc,
				(SELECT demographic, (ST_Dump(the_geom)).geom AS the_geom FROM """ + filename + """_dots) AS dump
			WHERE 
				ST_Contains(sc.the_geom,dump.the_geom)
			GROUP BY
				sc.stop_id;
			""")
	
	elif inputType == 'polygons':

		cursor.execute('ALTER TABLE ' + filename + ' ADD COLUMN IF NOT EXISTS area1 real;')
		cursor.execute('UPDATE ' + filename + '	SET	area1 = ST_Area(the_geom);')

		cursor.execute("""
			INSERT INTO stop_profiles_""" + filename + """
			SELECT
				stop_id,
				SUM(am_indian * area2 / area1) AS am_indian, 
				SUM(asian * area2 / area1) AS asian, 
				SUM(black * area2 / area1) AS black,
				SUM(latino * area2 / area1) AS latino,
				SUM(pacific * area2 / area1) AS pacific,
				SUM(white * area2 / area1) AS white,
				SUM(mixed * area2 / area1) AS mixed,
				SUM(other * area2 / area1) AS other,
				SUM(total * area2 / area1) AS total
			FROM (
				SELECT 
					sc.stop_id,
                    d.area1, 
                    ST_Area((ST_Dump(ST_Intersection(sc.the_geom, d.the_geom))).geom) AS area2,
					d.am_indian, 
                    d.asian, 
                    d.black,
					d.latino,
					d.pacific,
					d.white,
					d.mixed,
					d.other,
					d.total				
				FROM stop_catchments AS sc
					INNER JOIN """ + filename + """ AS d
					ON ST_Intersects(sc.the_geom, d.the_geom)) AS clipped
			GROUP BY
				stop_id;
			""")

	database.commit()


def route_demographics(filename):

	print 'Creating table of demographic profiles by routes with ' + filename + ' data.'
	cursor.execute('DROP TABLE IF EXISTS route_profiles_' + filename)

	cursor.execute("""
		CREATE TABLE route_profiles_""" + filename + """ (
			route_id TEXT, 
			direction_id INT,
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
		INSERT INTO route_profiles_""" + filename + """
		SELECT
			route_id,
			direction_id,
			SUM(stop_profiles_""" + filename + """.am_indian) AS am_indian, 
			SUM(stop_profiles_""" + filename + """.asian) AS asian,
			SUM(stop_profiles_""" + filename + """.black) AS black,
			SUM(stop_profiles_""" + filename + """.latino) AS latino,
			SUM(stop_profiles_""" + filename + """.pacific) AS pacific,
			SUM(stop_profiles_""" + filename + """.white) AS white,
			SUM(stop_profiles_""" + filename + """.mixed) AS mixed,
			SUM(stop_profiles_""" + filename + """.other) AS other,
			SUM(stop_profiles_""" + filename + """.total) AS total
		FROM
			route_stops,
			stop_profiles_""" + filename + """
		WHERE
			route_stops.stop_id = stop_profiles_""" + filename + """.stop_id
		GROUP BY
			route_id,
			direction_id;
		""")
	
	database.commit()

def chi2_stat(filename, df):

	print 'Calculating chi-squared statistic with ' + str(df) + ' degrees of freedom for each route pair with ' + filename + ' data.'
	cursor.execute('DROP TABLE IF EXISTS chi2_' + filename)
	cursor.execute("""
		CREATE TABLE chi2_""" + filename + """ (
			route_id TEXT,
			direction_id INT,
			route_id2 TEXT,
			direction_id2 INT,
			chi2 REAL,
			p_val REAL);
		""")

	cursor.execute("""	
		SELECT
			r1.route_id,
			r1.direction_id,
			r2.route_id AS route_id2,
			r2.direction_id AS direction_id2,
			CASE WHEN r1.total = 0 THEN 0 ELSE 
			COALESCE((r1.am_indian/r1.total - r2.am_indian/r2.total)^2 / (NULLIF(r1.am_indian,0)/r1.total),0) + 
			COALESCE((r1.asian/r1.total - r2.asian/r2.total)^2 / (NULLIF(r1.asian,0)/r1.total),0) + 
			COALESCE((r1.black/r1.total - r2.black/r2.total)^2 / (NULLIF(r1.black,0)/r1.total),0) + 
			COALESCE((r1.latino/r1.total - r2.latino/r2.total)^2 / (NULLIF(r1.latino,0)/r1.total),0) + 
			COALESCE((r1.pacific/r1.total - r2.pacific/r2.total)^2 / (NULLIF(r1.pacific,0)/r1.total),0) + 
			COALESCE((r1.white/r1.total - r2.white/r2.total)^2 / (NULLIF(r1.white,0)/r1.total),0) + 
			COALESCE((r1.mixed/r1.total - r2.mixed/r2.total)^2 / (NULLIF(r1.mixed,0)/r1.total),0) + 
			COALESCE((r1.other/r1.total - r2.other/r2.total)^2 / (NULLIF(r1.other,0)/r1.total),0)
			END AS chi2
		FROM
			route_profiles_""" + filename + """ AS r1,
			route_profiles_""" + filename + """ AS r2
		WHERE
			r1.route_id != r2.route_id
		ORDER BY
			chi2 DESC;
		""")

	for row in cursor.fetchall():
		p_val = 1 - chi2.cdf(row[4], df)
		cursor.execute('INSERT INTO chi2_' + filename + ' VALUES (%s, %s, %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4], p_val))

	database.commit()


epsg_code = get_SRID('gtfs_stops')
stop_catchments(epsg_code)
route_stops(epsg_code)
route_stop_catchments(epsg_code)

for filename in os.listdir(demo_path):
    
	if filename.endswith(".geojson"): 
		
		f = os.path.splitext(filename)[0]

		# geoprocessing demographic data
		stop_demographics(f, inputType)
		route_demographics(f)

		# calculate chi-squared stat
		groups = demographic_fields(f)
		df = len(groups) - 1
		chi2_stat(f, df)

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--Chi-squared statistics and p-values calculated--'
print ' '