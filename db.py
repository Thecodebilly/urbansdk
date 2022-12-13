import psycopg2
import json
import geopandas as gpd
import os
### TOP SECRET CRERDENTIALS ###
conn = psycopg2.connect(
   database="mydb",
    user='myuser',
    password='mypass',
    host='127.0.0.1',
    port= '5432'
)
### DO NOT SELL TO COMPETITORS ###
#(I would not actually do this, but for convience's sake I am storing these in a way that is not a pain to use)
cur = conn.cursor()

def create_and_populate_geo_tables(args):
    reset_counties_table()
    reset_states_table()
    with open(os.path.join(args.dot_data,"counties.geojson")) as f:
        counties_geojson = json.load(f)
    insert_to_counties_from_geojson(counties_geojson)
    states_geojson=shapefile_to_geojson(os.path.join(args.dot_data,"shape_file/tl_2022_us_state.shp"))
    with open(os.path.join(args.dot_data,"states.geojson"), 'w') as f:
        json.dump(states_geojson, f)
    insert_into_states_from_geojson(states_geojson)


def reset_counties_table():
    cur.execute("DROP TABLE IF EXISTS counties;")
    cur.execute("CREATE TABLE counties (id serial PRIMARY KEY, name varchar(255), geom geometry);")
    conn.commit()

def reset_states_table():
    cur.execute("DROP TABLE IF EXISTS states;")
    cur.execute("CREATE TABLE states (id serial PRIMARY KEY, name varchar(255), geom geometry);")
    conn.commit()

def reset_bridges_table():
    cur.execute("DROP TABLE IF EXISTS bridges;")
    cur.execute("CREATE TABLE bridges (id serial PRIMARY KEY, struct_number varchar(255), geojson geometry, long varchar(255), lat varchar(255), county varchar(255), state varchar(255));")
    conn.commit()

def insert_to_counties_from_geojson(counties_geojson):
    for county in counties_geojson['features']:
        cur.execute(f"INSERT INTO counties (name, geom) VALUES ('{county['properties']['name']}', ST_GeomFromGeoJSON('{json.dumps(county['geometry'])}'));")
    conn.commit()

def insert_into_states_from_geojson(states_geojson):
    for state in states_geojson['features']:
        cur.execute(f"INSERT INTO states (name, geom) VALUES ('{state['properties']['NAME']}', ST_GeomFromGeoJSON('{json.dumps(state['geometry'])}'));")
    conn.commit()

def insert_into_bridges_from_df(insert_bridges_df):
    for index,row in insert_bridges_df.iterrows():
        cur.execute(f"INSERT INTO bridges (struct_number, long, lat, state, county) VALUES ('{row['struct_number'].strip()}', {row['long']}, {row['lat']}, '{row['state']}', '{row['county']}')")
    conn.commit()



# with geojson (unknown GeoJSON type error)
#def insert_into_bridges_from_df(insert_bridges_df):
#    for index,row in insert_bridges_df.iterrows():
#        cur.execute(f"INSERT INTO bridges (struct_number, long, lat, geojson) VALUES ('{row['struct_number'].strip()}', {row['long']}, {row['lat']}, ST_GeomFromGeoJSON('{json.dumps(row['geojson'])}'))")
#    conn.commit()


def shapefile_to_geojson(shapefile_path):
    gdf = gpd.read_file(shapefile_path)
    return json.loads(gdf.to_json())

def find_county_by_coordinates(long, lat):
    cur.execute(f"SELECT name FROM counties WHERE ST_Contains(geom, ST_GeomFromText('POINT({long} {lat})'));")
    return cur.fetchall()

#SELECT * FROM counties where ST_Contains(geom, ST_GeomFromText('POINT(80.40191 32.20044)'));
def find_state_by_coordinates(long, lat):
    cur.execute(f"SELECT name FROM states WHERE ST_Contains(geom, ST_GeomFromText('POINT({long} {lat})'));")
    return cur.fetchall()

def query_counties():
    cur.execute("SELECT * FROM counties;")
    result=cur.fetchall()
    return result

def query_states():
    cur.execute("SELECT * FROM counties;")
    result=cur.fetchall()
    return result

