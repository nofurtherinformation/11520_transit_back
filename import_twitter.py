# -*- coding: utf-8 -*-
'''
--------------------------------
- Import GTFS As Is
--------------------------------
'''

import datetime
import psycopg2
import os
import re
import sys
import time
import csv

# app specific
import config
import common

reload(sys)
sys.setdefaultencoding('utf8')

# argument is tag for GTFS feed. Ex. 'sdm'
path_gtfs = sys.argv[1] if len(sys.argv) > 1 else ''
tag = sys.argv[2] if len(sys.argv) > 2 else ''

# start time
time_start = time.time()
print ' '
print '--Importing STARTED--'
print ' '

# Establish a Postgres connection

# comment out if using config.py
# db_host 	= 'localhost'
# db_port 	= '5432'
# db_user 	= 'transmilenio'
# db_passwd	= 'mejorparatodos'
# db_database = 'gtfs'

try:
	database = psycopg2.connect(database = config.db_database, user = config.db_user, password = config.db_passwd, host = config.db_host, port = config.db_port)
	# database = psycopg2.connect(database = db_database, user = db_user, password = db_passwd, host = db_host, port = db_port)
	print 'database connection successful.'
except:
	print 'database connection failed.'

cursor = database.cursor()


# function to import CSV or TXT files
def importCSV(f, table):

	reader = csv.reader(file(f, 'rU'))
	headers = next(reader)
	print headers
	print ' '

	io = open(f, 'r')
	cursor.copy_from(io, table, ',')
    # io.close()

# List of files to import

input_filenames = []

for root, directories, filenames in os.walk(path_gtfs):		
	for filename in filenames:
		if os.path.splitext(os.path.join(root,filename))[1].lower() in ('.xlsx', '.xls', '.csv', '.kml', '.txt'):
			input_filenames.append(filename)

for f in input_filenames:

	print f
	print ' '
	table = f.replace('.txt', '_' + tag)

	# cursor.execute('TRUNCATE ' + table + ';')
	# database.commit()

	importCSV(path_gtfs + '/' + f, table)


# Close the cursor
cursor.close()

# Close the database connection
database.close()

print 'Imported GTFS in ' + str(common.seconds_readable(int(time.time() - time_start)))