import os
import sys
sys.path.append("..")

import numpy as np
import pandas as pd
from collections import defaultdict
from fluxsus.DBFIX import DBFIX

def timeseries_hospitalbeds(cnespath, cnes_agg_df):
    '''
        Create a time series of the number of hospital beds
    '''
    basehosp = defaultdict(lambda: {"NUMLEITOS_PRINC": [], "NUMLEITOS_TODOS": []})
    cnes_list = cnes_agg_df["CNES"].tolist()
    [ basehosp[cnes] for cnes in cnes_list ] # -- init dict

    main_beds_cols = ["QTLEITP1", "QTLEITP2", "QTLEITP3"]
    all_beds_cols = [ col for col in cnes_agg_df.columns if 'QTL' in col ] 

    years_ = [ f'{n:2.0f}'.replace(' ','0') for n in range(8, 24) ]
    months_ = [ f'{n:2.0f}'.replace(' ', '0') for n in range(1, 13) ]
    period_index = []

    for yy in years_:
        for mm in months_:
            print(f"{yy}{mm} ... ", end='')
            period_index.append(f"{yy}{mm}")
            df = pd.read_parquet(os.path.join(cnespath, f"STCE{yy}{mm}.parquet"))

            df["NUMLEITOS_PRINC"] = df[main_beds_cols].sum(axis=1)
            df["NUMLEITOS_TODOS"] = df[all_beds_cols].sum(axis=1)
            ex = df.set_index("CNES")[["NUMLEITOS_PRINC", "NUMLEITOS_TODOS"]].to_dict('index')

            for cnes in cnes_list:
                if ex.__contains__(cnes):
                    basehosp[cnes]["NUMLEITOS_PRINC"].append(ex[cnes]["NUMLEITOS_PRINC"])
                    basehosp[cnes]["NUMLEITOS_TODOS"].append(ex[cnes]["NUMLEITOS_TODOS"])

                else:
                    basehosp[cnes]["NUMLEITOS_PRINC"].append(np.nan)
                    basehosp[cnes]["NUMLEITOS_TODOS"].append(np.nan)
            print(f'feito.')
    
    final_df = pd.concat([ pd.DataFrame(basehosp[cnes]) for cnes in cnes_list ], axis=1, keys=cnes_list)
    final_df.index = period_index
    return final_df

if __name__=="__main__":
    # -- change any path according to your own inputs and outputs.
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    cnespath = os.path.join(datapath, "opendatasus", "cnes")
    cnes_df = pd.read_parquet(os.path.join(cnespath, "cnes_st_0801_2312.parquet"))

    cnesparquet = os.path.join(cnespath, "PARQUET")
    ts_cnes = timeseries_hospitalbeds(cnesparquet, cnes_df)

    ts_cnes.to_parquet(os.path.join(os.path.join(cnespath, "cnes_leitos_timeserie_0801_2312.parquet")))