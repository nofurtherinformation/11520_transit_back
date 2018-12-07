# -*- coding: utf-8 -*-
'''
----------------------------------
- Combine results
----------------------------------
'''
# libraries
import psycopg2
import time
import sys
import math
import utm
import os

reload(sys)
sys.setdefaultencoding('utf8')

# arguments
demo_path = sys.argv[1] if len(sys.argv) > 1 else ''
min_collinear_index = sys.argv[2] if len(sys.argv) > 2 else 0
max_p_val = sys.argv[3] if len(sys.argv) > 3 else 1
	
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

def results(filename, min_collinear_index, max_p_val):

	print 'Consolidating collinearity indices and chi-square stats by route-pair for ' + filename + ' data.'
	cursor.execute('DROP TABLE IF EXISTS results_' + filename)
	cursor.execute("""
		CREATE TABLE results_""" + filename + """ (
			route_id TEXT,
			route_short_name TEXT,
			agency_name TEXT,
			route_id2 TEXT,
			route_short_name2 TEXT,
			agency_name2 TEXT,
			chi2 REAL,
			p_val REAL,
			collinear_index REAL);
		""")
	
	cursor.execute("""
		INSERT INTO results_""" + filename + """
		SELECT
			ci.route_id,
			r.route_short_name AS route_short_name,
			a.agency_name AS agency_name,
			ci.route_id2,
			r2.route_short_name AS route_short_name2,
			a2.agency_name AS agency_name2,
			ch.chi2,
			ch.p_val,
			ci.index AS collinear_index
		FROM
			collinear_index AS ci,
			chi2_""" + filename + """ AS ch,
			gtfs_routes AS r,
			gtfs_routes AS r2,
			gtfs_agency AS a,
			gtfs_agency AS a2
		WHERE
			ci.route_id = ch.route_id AND
			ci.route_id2 = ch.route_id2 AND
			r.route_id = ci.route_id AND
			r2.route_id = ci.route_id2 AND
			r.agency_id = a.agency_id AND
			r2.agency_id = a2.agency_id AND
			ch.p_val < """ + max_p_val + """ AND
			ci.index > """ + min_collinear_index + """ 
		ORDER BY
			ci.index DESC;
		""")
	
	database.commit()


def routes(filename):

    print 'Creating table of routes with significant results when using ' + filename + ' demographic data.'
    cursor.execute('DROP TABLE IF EXISTS routes_' + filename + ';')
    cursor.execute('CREATE TABLE routes_' + filename + ' (route_id TEXT, agency_name TEXT, route_short_name TEXT);')

    cursor.execute("""
		INSERT INTO routes_""" + filename + """
		(route_id, agency_name, route_short_name)
		SELECT
			r.route_id,
			a.agency_name,
			COALESCE(r.route_short_name, r.route_long_name) AS route_short_name
		FROM
			gtfs_agency AS a,
			gtfs_routes AS r,
            results_""" + filename + """ AS res
		WHERE
			r.agency_id = a.agency_id AND
            r.route_id = res.route_id;
		""")    

    database.commit()

for filename in os.listdir(demo_path):
    
	if filename.endswith(".geojson"): 
		
		f = os.path.splitext(filename)[0]
		results(f, min_collinear_index, max_p_val)
        routes(f)

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--Collinear Index and Chi-squared results combined--'
print ' '