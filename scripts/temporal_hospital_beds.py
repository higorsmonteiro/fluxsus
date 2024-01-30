import os
import sys
sys.path.append("..")

import glob
from tqdm import tqdm
import pandas as pd
from fluxsus.DBFIX import DBFIX

def agg_cnes(init_period : str, final_period : str, input_folder : str):
    '''
        Create the time series for each CNES (health unit) of the number of
        hospital beds available.

        Args:
        -----
            init_period:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                CNES file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the beginning
                of the period.
            final_period:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                CNES file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the ending of
                the period.
            input folder:
                String. Path to the folder containing the DBF files.

        Return:
        -------
            cnes_df:
                pandas.DataFrame. Final dataframe containing one single and most 
                recent record for each CNES unit.
    '''
    preffix = init_period[:4]
    list_of_files = glob.glob(os.path.join(input_folder, f'{preffix}*'))

    init_index = [idx for idx, s in enumerate(list_of_files) if init_period in s][0]
    final_index = [idx for idx, s in enumerate(list_of_files) if final_period in s][0]
    if final_index is None:
        raise Exception(f"no file {final_period} was found.")
    list_of_files = list_of_files[init_index:final_index+1]
    
    cnes_df = []
    for fname in tqdm(list_of_files):
        cur_df = DBFIX(fname, codec='latin').to_dataframe()
        cnes_df.append( cur_df )
    cnes_df = pd.concat(cnes_df, axis=0)

    # -- keep the most recent record for each CNES
    min_comp = cnes_df.groupby("CNES")["COMPETEN"].min().reset_index()
    max_comp = cnes_df.groupby("CNES")["COMPETEN"].max().reset_index()
    cnes_min = dict(zip(min_comp["CNES"], min_comp["COMPETEN"]))
    cnes_max = dict(zip(max_comp["CNES"], max_comp["COMPETEN"]))
    cnes_df = cnes_df.sort_values(by=["CNES", "COMPETEN"], ascending=False).drop_duplicates(subset=["CNES"], keep='first')
    cnes_df["COMPETEN_MIN"] = cnes_df["CNES"].apply(lambda x: cnes_min[x])
    cnes_df["COMPETEN_MAX"] = cnes_df["CNES"].apply(lambda x: cnes_max[x])
    return cnes_df

if __name__=="__main__":
    # -- change any path according to your own inputs and outputs.
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    input_folder = os.path.join(datapath, "opendatasus", "cnes", "DBF")
    cnes_df = agg_cnes("STCE0801", "STCE2312", input_folder)

    cnespath = os.path.join(datapath, "opendatasus", "cnes")
    cnesgeo_df = pd.read_csv(os.path.join(cnespath, "cnes_202311091005.csv"), delimiter=";")
    cnesgeo_df['CNES'] = cnesgeo_df['cnes'].apply(lambda x: f"{x:7.0f}".replace(" ", "0"))

    cnes_df = cnes_df.merge(cnesgeo_df[["CNES", "latitude", "longitude"]], on="CNES", how="left")
    cnes_df.to_parquet(os.path.join(os.path.join(datapath, "opendatasus", "cnes", "cnes_st_0801_2312.parquet")))
    
