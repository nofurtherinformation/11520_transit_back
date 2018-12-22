# -*- coding: utf-8 -*-
'''
----------------------------------
- Generate Mapbox Vector Tiles
----------------------------------
'''
# libraries
from __future__ import unicode_literals
import os
import shutil
import math
import psycopg2
import sys
import math
import utm
from flask import Flask, render_template, make_response
app = Flask(__name__)

# arguments
CACHE_DIR = sys.argv[1]

# Establish a Postgres connection
db_host 	= 'localhost'
db_port 	= '5432'
db_user 	= 'jonathanleape'
db_passwd	= 'mejorparatodos'
db_database = 'gtfs'

def tile_ul(x, y, z):
    n = 2.0 ** z
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_deg = math.degrees(lat_rad)
    return  lon_deg,lat_deg

def get_tile(z,x,y):
    xmin,ymin = tile_ul(x, y, z)
    xmax,ymax = tile_ul(x + 1, y + 1, z)
	
    tile = None
	
    tilefolder = "{}/{}/{}".format(CACHE_DIR,z,x)
    tilepath = "{}/{}.pbf".format(tilefolder,y)
    if not os.path.exists(tilepath): 
        database = psycopg2.connect(database = db_database, user = db_user, password = db_passwd, host = db_host, port = db_port)
        cursor = database.cursor()

        cursor.execute("""
            SELECT ST_AsMVT(tile) 
            FROM (
                SELECT 
                demographic,
                ST_AsMVTGeom(
                    the_geom, 
                    ST_Makebox2d(
                        ST_transform(
                            ST_SetSrid(
                                ST_MakePoint(%s,%s),4326),3857),
                        ST_transform(
                            ST_SetSrid(
                                ST_MakePoint(%s,%s),4326),3857)), 
                    4096, 
                    0, 
                    false) AS geom 
                FROM race_2016_dots) AS tile""",(xmin,ymin,xmax,ymax))
        tile = str(cursor.fetchone()[0])
        
        if not os.path.exists(tilefolder):
            os.makedirs(tilefolder)
        
        with open(tilepath, 'wb') as f:
            f.write(tile)
            f.close()

        cursor.close()
        database.close()
    else:
        tile = open(tilepath, 'rb').read()
	
    return tile

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/tiles')
# @app.route('/tiles/<int:z>/<int:x>/<int:y>', methods=['GET'])
def tiles(z=0, x=0, y=0):
	tile = get_tile(z, x, y)
	# response = make_response(tile)
	# response.headers['Content-Type'] = "application/octet-stream"
	# return response

tiles(10, 269, 409)