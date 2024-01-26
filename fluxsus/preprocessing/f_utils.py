
import os
import sys

import pandas as pd
import networkx as nx
import geopandas as gpd
from fluxsus.fluxnets.fluxnets import CityFlux, CityHospitalFlux

def create_citynet(cnes_df, geodata_df, inicial, final, sihpath, output):
    sih_df = []
    for year in range(int(inicial), int(final)+1):
        cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
        sih_df.append(cur_df)
    sih_df = pd.concat(sih_df)

    # -- generate network
    cityflux = CityFlux(cnes_df, geodata_df)
    cityflux.define_network().calculate_fluxes(sih_df).to_gml(output)