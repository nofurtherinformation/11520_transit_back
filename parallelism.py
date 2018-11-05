# -*- coding: utf-8 -*-
'''
--------------------------------
- Calculate Parallelism Index
--------------------------------
'''

import psycopg2
import time
import sys

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

# Truncate intermediate tables
cursor.execute('TRUNCATE shape_pairs, parallelism_shapes, parallelism_routes')
print 'Truncated tables.'

# Create shape pairs table
def create_shape_pairs():

	# adds stop sequence to distance matrix
	cursor.execute("""
		UPDATE distmatrix AS dist
		SET 
		sequence = sub.seq
		FROM
			(SELECT id, row_number() OVER (PARTITION BY agency_id, linea, ruta ORDER BY posicion ASC) AS seq
			FROM distmatrix) AS sub
		WHERE dist.id = sub.id;
		""")

	database.commit()

	print 'Stop sequence added to distance matrix.'

create_shape_pairs()

# Close the cursor
cursor.close()

# Close the database connection
database.close()

print ' '
print '--Finished--'
print ' '