#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import json
from geojson import loads, Feature, FeatureCollection

db_host = "localhost"
db_user = "pluto"
db_passwd = "secret"
db_database = "py_geoan_cb"
db_port = "5432"

# connect to DB
conn = psycopg2.connect(host=db_host, user=db_user, port=db_port,
                        password=db_passwd, database=db_database)

# create a cursor
cur = conn.cursor()

# define start and end coordinates in EPSG:3857
# set start and end floor level as integer 0,1,2 
x_start_coord = 1587848.414
y_start_coord = 5879564.080
start_floor = 2

x_end_coord = 1588005.547
y_end_coord = 5879736.039
end_floor = 2


# find the start node id within 1 meter of the given coordinate
start_node_query = """
    SELECT id FROM geodata.networklines_3857_vertices_pgr AS p
    WHERE ST_DWithin(the_geom, ST_GeomFromText('POINT(%s %s)',3857), 1)
    AND ST_Z(the_geom) = %s;"""

# locate the end node id within 1 meter of the given coordinate
end_node_query = """
    SELECT id FROM geodata.networklines_3857_vertices_pgr AS p
    WHERE ST_DWithin(the_geom, ST_GeomFromText('POINT(%s %s)',3857), 1)
    AND ST_Z(the_geom) = %s;"""

# run query and pass in the 3 variables to the query
cur.execute(start_node_query, (x_start_coord, y_start_coord, start_floor))
start_node_id = int(cur.fetchone()[0])

# get the end node id as an integer
cur.execute(end_node_query, (x_end_coord, y_end_coord, end_floor))
end_node_id = int(cur.fetchone()[0])


# pgRouting query to return list of segments representing
# shortest path Dijkstra results as GeoJSON

routing_query = '''
    SELECT seq, id1 AS node, id2 AS edge, ST_Length(wkb_geometry) AS cost, layer,
      type_id, ST_AsGeoJSON(wkb_geometry) AS geoj
      FROM pgr_dijkstra(
        'SELECT ogc_fid as id, source, target, st_length(wkb_geometry) AS cost,
         layer, type_id
         FROM geodata.networklines_3857',
        %s, %s, FALSE, FALSE
      ) AS dij_route
      JOIN  geodata.networklines_3857 AS input_network
      ON dij_route.id2 = input_network.ogc_fid ;
  '''


# run shortest path query
cur.execute(routing_query, (start_node_id, end_node_id))

route_segments = cur.fetchall()

route_result = []

# create the list of new GeoJSON
for segment in route_segments:
    seg_cost = segment[3]     
    layer_level = segment[4]  
    seg_type = segment[5]
    geojs = segment[6]        
    geojs_geom = loads(geojs) 
    geojs_feat = Feature(geometry=geojs_geom, properties={'floor': layer_level,
                                                          'cost': seg_cost,
                                                          'type_id': seg_type})
    route_result.append(geojs_feat)

# using the geojson module to create GeoJSON Feature Collection
geojs_fc = FeatureCollection(route_result)

# define the output folder and GeoJSON file name
output_geojson_route = "../geodata/ch08_indoor_3d_route.geojson"

def write_geojson():
    with open(output_geojson_route, "w") as geojs_out:
        geojs_out.write(json.dumps(geojs_fc))


write_geojson()

cur.close()
conn.close()