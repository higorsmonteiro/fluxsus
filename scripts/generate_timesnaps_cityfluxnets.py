import os
import sys
sys.path.append("..")

import pandas as pd
import datetime as dt
from datetime import timedelta

import geopandas as gpd
from fluxsus.fluxnets.fluxnets import CityFlux, CityHospitalFlux

def create_snapshots_citynet(cnes_df, geodata_df, dt_inicial, dt_final, sihpath, output):
    '''
    
    '''
    year_init = dt_inicial.year
    year_final = dt_final.year
    year_range = range(year_init-1, year_final+2, 1)

    sih_df = []
    for year in year_range:
        try:
            cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
            cur_df = cur_df[(cur_df["DT_INTER"]>=dt_inicial) & (cur_df["DT_INTER"]<=dt_final)]
            sih_df.append( cur_df )
        except:
            pass
    sih_df = pd.concat(sih_df)

    # -- generate network
    cityflux = CityFlux(cnes_df, geodata_df)
    cityflux.define_network().calculate_fluxes(sih_df).to_gml(output)

# -------------- network creation --------------

# ---- open the three main datasets
    
basepath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
cnespath = os.path.join(basepath, "opendatasus", "cnes")
sihpath = os.path.join(basepath, "opendatasus", "sihsus", "PARQUET")
geopath = os.path.join(basepath, "shapefilesceqgis")
output = os.path.join(basepath, "redes_aih", "snapshots")

cnes_df = pd.read_parquet(os.path.join(cnespath, "cnes_st_1001_2312.parquet"))
geodata_df = gpd.read_parquet(os.path.join(geopath, "ce_geodata.parquet"))

# -- partition year

start = dt.datetime(2008, 1, 1)
end = dt.datetime(2022, 12, 31)
delta = 90 # aggregated network over 90 days

last = start
timelst = []
while last<end:
    timelst.append(last)
    last += timedelta(days=delta)
if last!=end: timelst.append(end)

# -- create networks for the periods defined
for time_index in range(len(timelst)-1):
    dt_inicial = timelst[time_index]
    dt_final = timelst[time_index+1]
    print(dt_inicial, dt_final)

    fname = os.path.join(output, f"citynet_allicd_{dt_inicial.strftime('%Y-%m-%d')}_{dt_final.strftime('%Y-%m-%d')}.gml")
    create_snapshots_citynet(cnes_df, geodata_df, dt_inicial, dt_final, sihpath, fname)






