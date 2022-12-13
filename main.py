
import pandas as pd
import argparse

import db

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dot_data", default=".data", help="path to .data directory")
    args = parser.parse_args()
    
    db.create_and_populate_geo_tables(args)

    bridge_inspection_df=pd.read_csv(os.path.join(args.dot_data,'bridge_inspections.csv'))
    print(f"STRUCTURE_NUMBER_008 is a unique key: {bridge_inspection_df['STRUCTURE_NUMBER_008'].value_counts()[0]==1}")
    insert_bridges_df=pd.DataFrame()
    bridge_inspection_df['STRUCTURE_NUMBER_008'].str.replace('\W', '', regex=True)
    insert_bridges_df['struct_number']= bridge_inspection_df['STRUCTURE_NUMBER_008'].str.replace('\W', '', regex=True)
    insert_bridges_df['long']=bridge_inspection_df['LONG_017']/1000000
    insert_bridges_df['lat']=bridge_inspection_df['LAT_016']/1000000
    insert_bridges_df['geojson']=insert_bridges_df.apply(lambda x: f'{{"type":"Point","coordinates":[{x["long"]},{x["lat"]}]}}', axis=1)
    
    #I am unsure why I am unable to use ST_Contains in the way that I think that I should be able to
    #given slightly more time I think that I would be able to figure it out.
    #Instead I will pretend this works as I imagined it to, returning a state and county for each bridge's coordinate (in two separate functions) 
    #and I will create an insert function that will insert the bridge data into the database
    insert_bridges_df['state']="Jacksonville"
    insert_bridges_df['county']="FL"
    insert_bridges_df['county']=insert_bridges_df.apply(lambda x: db.find_county_by_coordinates(x['long'], x['lat'],axis=1))
    insert_bridges_df['state']=insert_bridges_df.apply(lambda x: db.find_state_by_coordinates(x['long'], x['lat'],axis=1))
    
    db.insert_into_bridges_from_df(insert_bridges_df)
    

    #make gpd to try to calculate state and county

    #from shapely.geometry import Point, Polygon
    #counties_gpd=gpd.GeoDataFrame.from_features(counties_geojson['features'])
    #states_gpd=gpd.GeoDataFrame.from_features(states_geojson['features'])
    #_pnts = [Point(3, 3), Point(8, 8), Point(11, 11)]

if __name__ == '__main__':
    main()
