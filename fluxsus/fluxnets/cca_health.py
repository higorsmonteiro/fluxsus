'''
    CCA algorithm for health units.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''
import os
import pandas as pd
import geopandas as gpd


def cca_health(sector_rel, hospital_rel, lsector, lhos):
    '''
        City Clustering Algorithm (CCA) with health units as seeds.

        Args:
            sector_rel:
                geopandas.GeoDataFrame. Data containing two columns, an identifier 
                and a geometry column representing the centroid of the sector.
            hospital_rel:
                geopandas.GeoDataFrame. Data containing two columns, an identifier 
                and a geometry column representing the geolocation (latitude, longitude) 
                of the health unit.
            lsector:
                Float. Sector radius (in kilometers). Distance from a health unit in a given
                CCA cluster to define the sectors belonging to the cluster.
            lhos:
                Float. Hospital radius (in kilometers). CCA parameter representing the
                distance between health units.
    '''
    pass

CNES_FILE = "cnes_st_0801_2312.parquet"
SETORES_FILE = "censo2010_pop_setores.parquet"
NETWORK_FILE = "citytohospitalnet_agg_1801_2306.gml"

# -- paths
basepath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
cnespath = os.path.join(basepath, "opendatasus", "cnes")
geopath = os.path.join(basepath, "shapefilesceqgis")
gmlpath = os.path.join(basepath, "redes_aih")

# -- distance parameters (kilometers)
L_HOSPITAL = 10
L_SETORES = 20

# -- load and transform data
# ---- health units geolocation
cnes_df = pd.read_parquet(os.path.join(cnespath, "cnes_st_0801_2312.parquet"))[["CNES", "latitude", "longitude"]]

# ---- geolocation of census tract units (brazil)
pop_census_df = gpd.read_parquet(os.path.join(geopath, "censo2010_pop_setores.parquet"))[["CD_GEOCODI", "geometry"]]
pop_census_df1 = pop_census_df.copy()
pop_census_df1['centroid'] = pop_census_df1['geometry'].centroid
pop_census_df1 = pop_census_df1.drop('geometry', axis=1).rename({'centroid': 'geometry'}, axis=1).set_geometry('geometry')



# -- the city-hospital bipartite network can provide which health units are actually relevant for analysis (generated at least one AIH during the period chosen)
#graph = nx.read_gml(os.path.join(gmlpath, "novo_completo", "citytohospitalnet_agg_1801_2306.gml"))



