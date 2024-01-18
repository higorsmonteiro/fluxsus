import os
import sys
sys.path.append("..")

import pandas as pd
import networkx as nx
import geopandas as gpd
from fluxsus.fluxnets.fluxnets import CityFlux, CityHospitalFlux

def create_citynet(cnes_df, geodata_df, inicial, final, output):
    sih_df = []
    for year in range(int(inicial), int(final)+1):
        cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
        sih_df.append(cur_df)
    sih_df = pd.concat(sih_df)

    # -- generate network
    cityflux = CityFlux(cnes_df, geodata_df)
    cityflux.define_network().calculate_fluxes(sih_df).to_gml(output)

def create_cityhospitalnet(cnes_df, geodata_df, inicial, final, output):
    sih_df = []
    for year in range(int(inicial), int(final)+1):
        cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
        sih_df.append(cur_df)
    sih_df = pd.concat(sih_df)

    cityhospitalflux = CityHospitalFlux(cnes_df, geodata_df)
    cityhospitalflux.define_network().calculate_fluxes(sih_df).to_gml(output)

# -------------- network creation --------------

# -- open the three main datasets: cnes, ceará geodata, sihsus.
basepath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
cnespath = os.path.join(basepath, "opendatasus", "cnes")
sihpath = os.path.join(basepath, "opendatasus", "sihsus", "PARQUET")
geopath = os.path.join(basepath, "shapefilesceqgis")
output = os.path.join(basepath, "redes_aih", "novo")

cnes_df = pd.read_parquet(os.path.join(cnespath, "cnes_st_1001_2312.parquet"))
geodata_df = gpd.read_parquet(os.path.join(geopath, "ce_geodata.parquet"))

# -- list
print("Cities ... ", end='')
create_citynet(cnes_df, geodata_df, 2010, 2010, output=os.path.join(output, "CITYFLUX_2010_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2011, 2011, output=os.path.join(output, "CITYFLUX_2011_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2012, 2012, output=os.path.join(output, "CITYFLUX_2012_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2013, 2013, output=os.path.join(output, "CITYFLUX_2013_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2014, 2014, output=os.path.join(output, "CITYFLUX_2014_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2015, 2015, output=os.path.join(output, "CITYFLUX_2015_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2016, 2016, output=os.path.join(output, "CITYFLUX_2016_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2017, 2017, output=os.path.join(output, "CITYFLUX_2017_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2018, 2018, output=os.path.join(output, "CITYFLUX_2018_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2019, 2019, output=os.path.join(output, "CITYFLUX_2019_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2020, 2020, output=os.path.join(output, "CITYFLUX_2020_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2021, 2021, output=os.path.join(output, "CITYFLUX_2021_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2022, 2022, output=os.path.join(output, "CITYFLUX_2022_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2013, 2022, output=os.path.join(output, "CITYFLUX_2013_2022_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2018, 2022, output=os.path.join(output, "CITYFLUX_2018_2022_ALL.gml"))
create_citynet(cnes_df, geodata_df, 2020, 2022, output=os.path.join(output, "CITYFLUX_2020_2022_ALL.gml"))
print("done.")

print("Hospitals ... ", end='')
create_cityhospitalnet(cnes_df, geodata_df, 2010, 2010, output=os.path.join(output, "CITYHOSPITALFLUX_2010_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2011, 2011, output=os.path.join(output, "CITYHOSPITALFLUX_2011_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2012, 2012, output=os.path.join(output, "CITYHOSPITALFLUX_2012_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2013, 2013, output=os.path.join(output, "CITYHOSPITALFLUX_2013_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2014, 2014, output=os.path.join(output, "CITYHOSPITALFLUX_2014_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2015, 2015, output=os.path.join(output, "CITYHOSPITALFLUX_2015_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2016, 2016, output=os.path.join(output, "CITYHOSPITALFLUX_2016_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2017, 2017, output=os.path.join(output, "CITYHOSPITALFLUX_2017_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2018, 2018, output=os.path.join(output, "CITYHOSPITALFLUX_2018_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2019, 2019, output=os.path.join(output, "CITYHOSPITALFLUX_2019_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2020, 2020, output=os.path.join(output, "CITYHOSPITALFLUX_2020_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2021, 2021, output=os.path.join(output, "CITYHOSPITALFLUX_2021_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2022, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2022_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2013, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2013_2022_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2018, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2018_2022_ALL.gml"))
create_cityhospitalnet(cnes_df, geodata_df, 2020, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2020_2022_ALL.gml"))
print("done.")
