import os
import sys
sys.path.append("..")

import pandas as pd
import networkx as nx
import geopandas as gpd
import fluxsus.fluxnets.fnets_utils as futils
from fluxsus.fluxnets.fluxnets import CityFlux, CityHospitalFlux

#def create_citynet(cnes_df, geodata_df, inicial, final, output):
#    sih_df = []
#    for year in range(int(inicial), int(final)+1):
#        cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
#        sih_df.append(cur_df)
#    sih_df = pd.concat(sih_df)
#
#    # -- generate network
#    cityflux = CityFlux(cnes_df, geodata_df)
#    cityflux.define_network().calculate_fluxes(sih_df).to_gml(output)

#def create_cityhospitalnet(cnes_df, geodata_df, inicial, final, output):
#    sih_df = []
#    for year in range(int(inicial), int(final)+1):
#        cur_df = pd.read_parquet(os.path.join(sihpath, f"RDCE_{year}.parquet"))
#        sih_df.append(cur_df)
#    sih_df = pd.concat(sih_df)
#
#    cityhospitalflux = CityHospitalFlux(cnes_df, geodata_df)
#    cityhospitalflux.define_network().calculate_fluxes(sih_df).to_gml(output)

# -------------- network creation --------------

# -- obs: need to convert DBFs to parquet before running this script.

# -- open the three main datasets: cnes, ceará geodata, sihsus.
basepath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
cnespath = os.path.join(basepath, "opendatasus", "cnes")
sihpath = os.path.join(basepath, "opendatasus", "sihsus", "PARQUET")
geopath = os.path.join(basepath, "shapefilesceqgis")
output = os.path.join(basepath, "redes_aih", "novo_completo")

cnes_df = pd.read_parquet(os.path.join(cnespath, "cnes_st_0801_2312.parquet"))
geodata_df = gpd.read_parquet(os.path.join(geopath, "ce_geodata.parquet"))

# -- city net

# ---- larger agg
futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE1301', f'RDCE2212', output=os.path.join(output, f"cityfluxnet_agg_1301_2212.gml"))
futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE1801', f'RDCE2212', output=os.path.join(output, f"cityfluxnet_agg_1801_2212.gml"))
futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE1801', f'RDCE2306', output=os.path.join(output, f"cityfluxnet_agg_1801_2306.gml"))
futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE2001', f'RDCE2212', output=os.path.join(output, f"cityfluxnet_agg_2001_2212.gml"))
futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE2001', f'RDCE2306', output=os.path.join(output, f"cityfluxnet_agg_2001_2306.gml"))

# ---- per year
for yy in [f'{n}' for n in range(10, 23+1)]:
    futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE{yy}01', f'RDCE{yy}12', output=os.path.join(output, f"cityfluxnet_agg_{yy}01_{yy}12.gml"))

# ---- temporal without overlap
for yy in [f'{n}' for n in range(10, 23+1)]:
    for mm_pair in [('01', '03'), ('04', '06'), ('07', '09'), ('10', '12')]:
        futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE{yy}{mm_pair[0]}', f'RDCE{yy}{mm_pair[1]}', output=os.path.join(output, f"cityfluxnet_noverlap_{yy}{mm_pair[0]}_{yy}{mm_pair[1]}.gml"))

years_ = [ f'{n:2.0f}'.replace(" ", "0") for n in range(10, 23+1) ]
months_ = [ f'{n:2.0f}'.replace(" ", "0") for n in range(1, 12+1) ]
yy_left, yy_right = 0, 0
mm_left, mm_right = 0, 0+2

init_left = f'{years_[yy_left]}{months_[mm_left]}'
init_right = f'{years_[yy_right]}{months_[mm_right]}'
while init_right!='2312':
    print(f'({init_left} - {init_right})')
    futils.create_citynet(sihpath, cnes_df, geodata_df, f'RDCE{init_left}', f'RDCE{init_right}', output=os.path.join(output, f"cityfluxnet_{init_left}_{init_right}.gml"))

    mm_left+=1
    mm_right+=1
    if mm_right>11:
        mm_right = 0
        yy_right+=1
    if mm_left>11:
        mm_left = 0
        yy_left+=1

    init_left = f'{years_[yy_left]}{months_[mm_left]}'
    init_right = f'{years_[yy_right]}{months_[mm_right]}'

# -- city to hospital net (bipartite)
    
# ---- larger agg
futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE1301', f'RDCE2212', output=os.path.join(output, f"citytohospitalnet_agg_1301_2212.gml"))
futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE1801', f'RDCE2212', output=os.path.join(output, f"citytohospitalnet_agg_1801_2212.gml"))
futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE1801', f'RDCE2306', output=os.path.join(output, f"citytohospitalnet_agg_1801_2306.gml"))
futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE2001', f'RDCE2212', output=os.path.join(output, f"citytohospitalnet_agg_2001_2212.gml"))
futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE2001', f'RDCE2306', output=os.path.join(output, f"citytohospitalnet_agg_2001_2306.gml"))

# ---- per year
for yy in [f'{n}' for n in range(10, 23+1)]:
    futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE{yy}01', f'RDCE{yy}12', output=os.path.join(output, f"citytohospitalnet_agg_{yy}01_{yy}12.gml"))

# ---- temporal without overlap
for yy in [f'{n}' for n in range(10, 23+1)]:
    for mm_pair in [('01', '03'), ('04', '06'), ('07', '09'), ('10', '12')]:
        futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE{yy}{mm_pair[0]}', f'RDCE{yy}{mm_pair[1]}', output=os.path.join(output, f"citytohospitalnet_noverlap_{yy}{mm_pair[0]}_{yy}{mm_pair[1]}.gml"))

years_ = [ f'{n:2.0f}'.replace(" ", "0") for n in range(10, 23+1) ]
months_ = [ f'{n:2.0f}'.replace(" ", "0") for n in range(1, 12+1) ]
yy_left, yy_right = 0, 0
mm_left, mm_right = 0, 0+2

init_left = f'{years_[yy_left]}{months_[mm_left]}'
init_right = f'{years_[yy_right]}{months_[mm_right]}'
while init_right!='2312':
    print(f'({init_left} - {init_right})')
    futils.create_cityhospitalnet(sihpath, cnes_df, geodata_df, f'RDCE{init_left}', f'RDCE{init_right}', output=os.path.join(output, f"citytohospitalnet_{init_left}_{init_right}.gml"))

    mm_left+=1
    mm_right+=1
    if mm_right>11:
        mm_right = 0
        yy_right+=1
    if mm_left>11:
        mm_left = 0
        yy_left+=1

    init_left = f'{years_[yy_left]}{months_[mm_left]}'
    init_right = f'{years_[yy_right]}{months_[mm_right]}'



#print("Hospitals ... ", end='')
#create_cityhospitalnet(cnes_df, geodata_df, 2010, 2010, output=os.path.join(output, "CITYHOSPITALFLUX_2010_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2011, 2011, output=os.path.join(output, "CITYHOSPITALFLUX_2011_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2012, 2012, output=os.path.join(output, "CITYHOSPITALFLUX_2012_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2013, 2013, output=os.path.join(output, "CITYHOSPITALFLUX_2013_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2014, 2014, output=os.path.join(output, "CITYHOSPITALFLUX_2014_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2015, 2015, output=os.path.join(output, "CITYHOSPITALFLUX_2015_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2016, 2016, output=os.path.join(output, "CITYHOSPITALFLUX_2016_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2017, 2017, output=os.path.join(output, "CITYHOSPITALFLUX_2017_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2018, 2018, output=os.path.join(output, "CITYHOSPITALFLUX_2018_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2019, 2019, output=os.path.join(output, "CITYHOSPITALFLUX_2019_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2020, 2020, output=os.path.join(output, "CITYHOSPITALFLUX_2020_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2021, 2021, output=os.path.join(output, "CITYHOSPITALFLUX_2021_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2022, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2022_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2013, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2013_2022_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2018, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2018_2022_ALL.gml"))
#create_cityhospitalnet(cnes_df, geodata_df, 2020, 2022, output=os.path.join(output, "CITYHOSPITALFLUX_2020_2022_ALL.gml"))
#print("done.")
