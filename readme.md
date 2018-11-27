---
title: "PostGIS documentation"
author: "Jonathan Leape"
date: "11/26/2018"
...
projection: WGS84 (4326)

# PostGIS backend for Chiromancy of Collinear Competition

## Background

This study seeks to identify cases where transit routes in collinear competition serve populations with statistically distinct demographic profiles.

The back-end aims to automate the processing of GTFS and demographic data so that the study can be easily replicated in other cities.

The back-end uses a bash file to execute a series of python scripts that wrap PostGIS queries. 

## Inputs

The analysis relies on two inputs: GTFS of the transit system and geographic layers of demographic data.

### GTFS

The program can accommodate any valid GTFS feed that includes shapes.txt, the file defining the geometry of routes. 

In cities with multiple GTFS feeds, the feeds must be combined into a single valid feed. [Transit Feed Merge Tool](https://github.com/google/transitfeed/blob/master/merge.py)

### Demographics

The program will be further developed to be more flexible in its requirements for demographic data, which is generally much less standardized and more context-specific than GTFS data.

Currently, the program expects demographic inputs as Polygon GeoJSON files with the following properites:

#### Race

"GEOID": FIPS code for location
"am_indian": number of residents who identify as "American Indian" only
"asian": ... " " ... "Asian" only
"black": ... " " ... "Black" only
"latino": ... " " ... "Latino" only
"pacific": ... " " ... "Pacific Islander" only
"white": ... " " ... "White" only
"mixed": number of residents who identify with two or more races
"other": number of residents who identify a race not listed above

#### Income

"GEOID": FIPS code for location
"15k": number of residents who live in households with annual income less than $15,000
"25k": ... " " ... $25,000
"35k": ... " " ... $35,000
"45k": ... " " ... $45,000
"60k": ... " " ... $60,000
"100k": ... " " ... $100,000
"150k": ... " " ... $150,000
"rich": number of residents who live in households with annual income great than $150,000

## Processing

To process the data, set the paths and parameters in the run_all.command bash script. Close the file and double click it to execute.

The program will output the following files:

### GeoJSONs

#### route_shapes.geojson

A LINESTRING layer representing geometries of all routes found in GTFS

"route_id": unique identifier of route from routes.txt
"shape_id": unique identifier of associated shape from shapes.txt

#### route_catchments.geojson

A POLYGON layer representing the potential catchment areas of all routes found in GTFS, defined as a 500m buffer of associated shape.

"route_id": unique identifier of route from routes.txt

#### route_stops.geojson

A MULTIPOINT layer representing the sequences of stops associated with each route

"route_id": unique identifier of route from routes.txt
"stop_id": unique identifier of stop from stops.txt
"stop_sequence": ordinal ranking of stop from stop_times.txt

#### route_stop_catchments.geojson

A MULTIPOLYGON layer representing the catchment areas of stops associated with each route

"route_id": unique identifier of route from routes.txt
"stop_id": unique identifier of stop from stops.txt
"stop_sequence": ordinal ranking of stop from stop_times.txt

#### demographic_dots.geojson

A MULTIPOINT layer where each point represents a number of residents determined in the run_all.command.

"demographic": demographic group to which the residents represented by the point belong
"dpp": dots per person, as defined by user in run_all.command

### JSONs

#### route_demographic.json

A file with the number of residents in each demographic group within the catchment areas of the routes.

"route_id": unique identifier of route from routes.txt

Additional keys depend on the specifications of the demographic input files.

#### results_demographic.json

"route_id": unique identifier of study route from routes.txt
"route_id2": unique identifier of comparison route from routes.txt
"collinear_index": indicator of collinear competition, calculated as the ratio of the overlap in catchment areas of the two routes to the catchment area of the study route
"chi2_stat": Chi-squared statistic indicating the degree of differentiation between the catchment area demographics of the two routes
"p_val": P-value of Chi-squared statistics, indicating the probability that the differentiation in demographics is due to chance