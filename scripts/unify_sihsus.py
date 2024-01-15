import os
import sys
sys.path.append("..")

import pandas as pd
from fluxsus.DBFIX import DBFIX

def export_sihsus_year(uf : str, year : int, path_to_files : str):
    '''
        Aggregate the monthly data from SIHSUS into year data exported into 
        PARQUET files (higher throughput).

        Args:
        -----
            uf:
                String. State code which the records refer. 
            year:
                Integer. Year of the files. 
            path_to_files:
                String. Path to the folder containing the DBF files.

        Return:
        -------
            sih_df:
                pandas.DataFrame.
    '''
    # -- only a subset of the fields are chosen due to space requirements. All
    # -- variables can be stored considering higher memory requirements.
    variables = [
        'ESPEC', 'N_AIH', 'ANO_CMPT', 'CNES', 'IDENT', 'MUNIC_RES', 'MARCA_UTI', 
        'VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'DT_INTER', 'DT_SAIDA', 
        'DIAG_PRINC', 'DIAG_SECUN', 'COBRANCA', 'NATUREZA', 'GESTAO', 'MUNIC_MOV', 
        'MORTE', 'COMPLEX'
    ]
    year_str = f"{year}"[2:]
    month_lst = [ f'{n:2.0f}'.replace(' ', '0') for n in range(1,13) ]

    sih_df = []
    for month in month_lst:
        fname = f"RD{uf.upper()}{year_str}{month}.dbf"
        try:
            cur_df = DBFIX( os.path.join(path_to_files, fname), codec='latin' ).to_dataframe()
            sih_df.append( cur_df[variables].copy() )
        except:
            pass
    
    sih_df = pd.concat(sih_df, axis=0)
    return sih_df
    
if __name__=="__main__":
    # -- change any path according to your own inputs and outputs.
    datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
    uf = "CE"
    input_folder = os.path.join(datapath, "opendatasus", "sihsus", "DBF")
    output_folder = os.path.join(datapath, "opendatasus", "sihsus", "PARQUET")
    for year in range(2008, 2011+1):
        print(f'ano {year} ... ', end='')
        sih_df = export_sihsus_year(uf, year, input_folder)
        sih_df.to_parquet(os.path.join(output_folder, f'RD{uf}_{year}.parquet'))
        print(f'feito.')