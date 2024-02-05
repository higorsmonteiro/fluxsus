import os
import sys
sys.path.append("..")

import pandas as pd
import geopandas as gpd
from fluxsus.DBFIX import DBFIX
import fluxsus.fluxnets.fnets_utils as futils

# -- specific for my data
def set_geodata(cres_df, macro_df, muni_df, pop_df=None):
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
    macro_proposal = futils.macro_proposal()
    muni_df1["MACRO_ID_PROPOSAL"] = muni_df1["NM_MUNICIP"].map(macro_proposal)

    muni_df1['centroid_municip'] = muni_df1.centroid
    muni_df1['municip_lon'] = muni_df1['centroid_municip'].apply(lambda x: x.x)
    muni_df1['municip_lat'] = muni_df1['centroid_municip'].apply(lambda x: x.y)

    if pop_df is not None:
        pop_df.columns = ['GEOCOD6','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022', '2022.1']
        pop_df = pop_df.drop(["2009", "2022.1"], axis=1)
        pop_df["GEOCOD6"] = pop_df["GEOCOD6"].apply(lambda x: x.split(" ")[0])
        muni_df1 = muni_df1.merge(pop_df, on="GEOCOD6", how="left")

    return muni_df1

if __name__=="__main__":
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    geopath = os.path.join(datapath, "shapefilesceqgis")

    cres_df = gpd.read_file( os.path.join(geopath, "CRES.shp") )
    muni_df = gpd.read_file( os.path.join(geopath, "Ceará MUN.shp") )
    macro_df = gpd.read_file( os.path.join(geopath, "Macro Ceará.shp") )
    pop_df = None
    # -- if pop file not available, comment line below
    pop_df = pd.read_excel(os.path.join(geopath, "POP. MUNICIPIO CEARA_ 2009_2023.xlsx"), header=4)[1:-3]
    muni_df1 = set_geodata(cres_df, macro_df, muni_df, pop_df=pop_df)

    # -- custom color for current macrorregions
    cmap_macro = {1: "#ef476f", 2: "#ffb300", 3: "#04ae81", 4: "#118ab2", 5: "#073b4c"}
    muni_df1["MACRO_COLOR"] = muni_df1["MACRO_ID"].map(cmap_macro)
    muni_df1.to_parquet( os.path.join(geopath, "ce_geodata.parquet") )