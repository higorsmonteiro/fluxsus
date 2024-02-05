import os
import sys
sys.path.append("..")

import pandas as pd
from fluxsus.DBFIX import DBFIX

def cnes_to_parquet(dbf_fname: str, path_to_file: str, output_fname: str, output_path: str):
    '''
        Convert CNES DBF file to parquet for fast throughput.

        Args:
        -----
            dbf_fname:
                String. Name of the DBF file (with extension).
            path_to_file:
                String. Complete path to the DBF file.
            output_path:
                ...
    '''
    cnes_df = DBFIX( os.path.join(path_to_file, dbf_fname), codec='latin' ).to_dataframe()
    cnes_df.to_parquet(os.path.join(output_path, output_fname))

if __name__=="__main__":
    # -- change any path according to your own inputs and outputs.
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    input_folder = os.path.join(datapath, "opendatasus", "cnes", "DBF")
    output_folder = os.path.join(datapath, "opendatasus", "cnes", "PARQUET")

    months_ = [ f'{n:2.0f}'.replace(' ', '0') for n in range(1,13) ]
    years_ = [ f'{n:2.0f}'.replace(' ', '0') for n in range(8,23+1) ]

    uf = 'CE'
    for year in years_:
        for month in months_:
            fname = f'ST{uf}{year}{month}'
            print(f'Arquivo {fname} ... ', end='')
            cnes_to_parquet(fname+'.DBF', input_folder, fname+'.parquet', output_folder)
            print(f'feito.')