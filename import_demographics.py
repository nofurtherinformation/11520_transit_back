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
import os
# import ogr2ogr
# import osgeo
# import gdaltools

# local
# from project_to_utm import get_utm_code, project_geom

reload(sys)
sys.setdefaultencoding('utf8')

# arguments
demo_path = sys.argv[1]
dpp = sys.argv[2] if len(sys.argv) > 2 else 10

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

# ogr = gdaltools.ogr2ogr()
# conn = gdaltools.PgConnectionString(dbname = db_database, user = db_user, password = db_passwd, host = db_host, port = db_port)

# def main(path, filename):
#   #note: main is expecting sys.argv, where the first argument is the script name
#   #so, the argument indices in the array need to be offset by 1
#   ogr2ogr.main(["","-f", "PostgreSQL", filename, path + filename + '.geojson'])


def get_utm_code(input_geom):

	print 'Idenitifying UTM zone of demographic data.'
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

	print 'Projecting demographic data to UTM for geoprocessing.'
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


# def import_demographics_csv(path, filename, epsg_code):

#     filepath = path + filename + '.csv'

#     cursor.execute('DROP TABLE IF EXISTS ' + filename)
#     print 'Dropped ' + filename + ' table.'

#     cursor.execute("""
#     CREATE TABLE """ + filename + """(
#         GEOID TEXT,
#         lon REAL,
#         lat REAL,
#         am_indian REAL, 
#         asian REAL,
#         black REAL,
#         latino REAL,
#         pacific REAL,
#         white REAL,
#         mixed REAL,
#         other REAL,
#         total REAL);
#         """)

#     cursor.execute("COPY " + filename + " FROM '" + filepath + "' CSV HEADER;")

#     cursor.execute("SELECT AddGeometryColumn('" + filename + "', 'the_geom', " + str(epsg_code) + ", 'POINT', 2);")

#     cursor.execute("UPDATE " + filename + " SET the_geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);")

#     database.commit()

#     print filename + ' centroids imported.'


# def import_geojson(path, filename, epsg_code):

# 	cursor.execute('DROP TABLE IF EXISTS ' + filename)
# 	print 'Dropped ' + filename + ' table.'

# 	ogr.set_input(filename + '.geojson', srs='EPSG:4326')
# 	ogr.set_output(conn, table_name=filename)
# 	ogr.execute()

# 	print filename + ' geojson imported.'

def demographic_dots(filename, epsg_code, dots):

	print 'Creating dot density cloud for ' + filename + ' data.'
	cursor.execute('DROP TABLE IF EXISTS ' + filename + '_dots;')
	cursor.execute('CREATE TABLE ' + filename + '_dots (demographic TEXT, t INT);')
	cursor.execute("SELECT AddGeometryColumn('" + filename + "_dots', 'the_geom', " + str(epsg_code) + ", 'MULTIPOINT', 2);")

	cursor.execute("""
		INSERT INTO """ + filename + """_dots
	 	(demographic, t, the_geom)
	 	SELECT 
			'am_indian' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, am_indian::integer / """ + dots + """) as the_geom
		FROM """ + filename + """ 

		UNION

		SELECT 
			'asian' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, asian::integer / """ + dots + """) as the_geom 
		FROM """ + filename + """ 

		UNION

		SELECT 
			'black' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, black::integer / """ + dots + """) as the_geom
		FROM """ + filename + """

		UNION

		SELECT 
			'latino' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, latino::integer / """ + dots + """) as the_geom
		FROM """ + filename + """

		UNION
		
		SELECT 
			'pacific' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, pacific::integer / """ + dots + """) as the_geom
		FROM """ + filename + """

		UNION
		
		SELECT 
			'white' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, white::integer / """ + dots + """) as the_geom
		FROM """ + filename + """

		UNION
		
		SELECT 
			'mixed' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, mixed::integer / """ + dots + """) as the_geom
		FROM """ + filename + """

		UNION
		
		SELECT 
			'other' as demographic,
			""" + dots + """ as dpp,
			ST_GeneratePoints(the_geom, other::integer / """ + dots + """) as the_geom
		FROM """ + filename + """;
	""")

	database.commit()


for filename in os.listdir(demo_path):
    
	if filename.endswith(".geojson"): 
		
		f = os.path.splitext(filename)[0]
		
		# project layer
		epsg_code = get_utm_code(f)
		project_geom(f, epsg_code)

		# create dot density cloud
		demographic_dots(f, epsg_code, dpp)


