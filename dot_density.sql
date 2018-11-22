-- Source code from https://github.com/CartoDB/crankshaft/blob/develop/src/pg/sql/30_dot_density.sql

CREATE OR REPLACE FUNCTION cdb_dot_density(geom geometry , no_points Integer, max_iter_per_point Integer DEFAULT 1000)
RETURNS GEOMETRY AS $$
DECLARE
  extent GEOMETRY;
  test_point Geometry;
  width                NUMERIC;
  height               NUMERIC;
  x0                   NUMERIC;
  y0                   NUMERIC;
  xp                   NUMERIC;
  yp                   NUMERIC;
  no_left              INTEGER;
  remaining_iterations INTEGER;
  points               GEOMETRY[];
  bbox_line            GEOMETRY;
  intersection_line    GEOMETRY;
BEGIN
  extent  := ST_Envelope(geom);
  width   := ST_XMax(extent) - ST_XMIN(extent);
  height  := ST_YMax(extent) - ST_YMIN(extent);
  x0 	  := ST_XMin(extent);
  y0 	  := ST_YMin(extent);
  no_left := no_points;

  LOOP
    if(no_left=0) THEN
      EXIT;
    END IF;
    yp = y0 + height*random();
    bbox_line  = ST_MakeLine(
      ST_SetSRID(ST_MakePoint(yp, x0),4326),
      ST_SetSRID(ST_MakePoint(yp, x0+width),4326)
    );
    intersection_line = ST_Intersection(bbox_line,geom);
  	test_point = ST_LineInterpolatePoint(st_makeline(st_linemerge(intersection_line)),random());
	  points := points || test_point;
	  no_left = no_left - 1 ;
  END LOOP;
  RETURN ST_Collect(points);
END;
$$
LANGUAGE plpgsql VOLATILE;


SELECT ST_TRANSFORM(CDB_DotDensityfast(the_geom,asian_pop/100),3857) as the_geom_webmercator,
'asian' as ethnicity,
1 as t 
FROM us_census_acs2013_5yr_census_tract_copy  

UNION

SELECT ST_TRANSFORM(CDB_DotDensityfast(the_geom,black_pop/100),3857) as the_geom_webmercator, 
'black' as ethnicity, 
1 as t 
FROM us_census_acs2013_5yr_census_tract_copy

UNION 

SELECT ST_TRANSFORM(CDB_DotDensityfast(the_geom,white_pop/100),3857) as the_geom_webmercator, 
'white' as ethnicity, 
1 as t 
FROM us_census_acs2013_5yr_census_tract_copy  

UNION 

SELECT ST_TRANSFORM(CDB_DotDensityfast(the_geom,hispanic_or_latino_pop/100),3857) as the_geom_webmercator, 
'hispanic' as ethnicity, 
1 as t 
FROM us_census_acs2013_5yr_census_tract_copy
