import os
import sys
sys.path.append("..")

import pandas as pd
import geopandas as gpd
from fluxsus.DBFIX import DBFIX

# -- specific for my data
def set_geodata(cres_df, macro_df, muni_df):
    '''
    
    '''
    macro_df["ID"] = macro_df["ID"].astype(int)
    macro_df = macro_df.rename({"ID": "MACRO_ID", "Macro Shee": "MACRO_NOME", "geometry": "geometry_macro"}, axis=1)
    muni_df["GEO6"] = muni_df["GEO6"].astype(str)
    muni_df = muni_df.rename({"GEO6": "GEOCOD6", "CD_GEOCODM": "GEOCOD7", "MACRO": "MACRO_ID", "CRES": "CRES_ID", "geometry": "geometry_municip"}, axis=1)
    cres_df = cres_df.rename({"ID": "CRES_ID", "geometry": "geometry_cres"}, axis=1)
    cres_df["CRES_ID"] = cres_df["CRES_ID"].astype(int)

    muni_df1 = muni_df.merge(macro_df, on="MACRO_ID", how='left').merge(cres_df.drop("CRES", axis=1), on="CRES_ID", how="left")
    muni_df1 = muni_df1.set_geometry("geometry_municip")
    muni_df1['centroid_municip'] = muni_df1.centroid
    muni_df1['municip_lon'] = muni_df1['centroid_municip'].apply(lambda x: x.x)
    muni_df1['municip_lat'] = muni_df1['centroid_municip'].apply(lambda x: x.y)
    return muni_df1

if __name__=="__main__":
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    geopath = os.path.join(datapath, "shapefilesceqgis")

    cres_df = gpd.read_file( os.path.join(geopath, "CRES.shp") )
    muni_df = gpd.read_file( os.path.join(geopath, "Ceará MUN.shp") )
    macro_df = gpd.read_file( os.path.join(geopath, "Macro Ceará.shp") )

    muni_df1 = set_geodata(cres_df, macro_df, muni_df)
    muni_df1.to_parquet( os.path.join(geopath, "ce_geodata.parquet") )