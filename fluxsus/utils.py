import os 
import pandas as pd

from fluxsus.DBFIX import DBFIX

def export_sihsus_year(uf:str, year:int, path_to_files:str, path_to_output:str):
    '''
        Aggregate the monthly data from SIHSUS into year data 
        exported into PARQUET files.

        Args:
        -----
            uf:
                String.
            year:
                Integer.
            path_to_files:
                String.
            path_to_output:
                String.

        Return:
        -------
            None.
    '''
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
    sih_df.to_parquet(os.path.join(path_to_output, f'RD{uf.upper()}_{year}.parquet'))

