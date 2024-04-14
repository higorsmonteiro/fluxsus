import os
import sys
sys.path.append("..")

from pathlib import Path
import pandas as pd
from fluxsus.DBFIX import DBFIX

def sihsus_to_parquet(dbf_fname: str, path_to_file: str, output_fname: str, output_path: str):
    '''
        Convert SIHSUS DBF file to parquet for fast throughput.

        Args:
        -----
            dbf_fname:
                String. Name of the DBF file (with extension).
            path_to_file:
                String. Complete path to the DBF file.
            output_path:
                ...
    '''
    selected_fields = [
        'ESPEC', 'N_AIH', 'CNES', 'IDENT', 'MUNIC_RES', 'MARCA_UTI', 
        'VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'DT_INTER', 'DT_SAIDA', 
        'DIAG_PRINC', 'DIAG_SECUN', 'COBRANCA', 'NATUREZA', 'GESTAO', 'MUNIC_MOV', 
        'MORTE', 'COMPLEX', 'ANO_CMPT', 'MES_CMPT'
    ]
    sih_df = DBFIX( os.path.join(path_to_file, dbf_fname), codec='latin' ).to_dataframe()
    sih_df["DT_INTER"] = pd.to_datetime(sih_df["DT_INTER"], format="%Y%m%d", errors="coerce")
    sih_df["DT_SAIDA"] = pd.to_datetime(sih_df["DT_SAIDA"], format="%Y%m%d", errors="coerce")
    #sih_df[selected_fields].to_parquet(os.path.join(output_path, output_fname))
    sih_df.to_parquet(os.path.join(output_path, output_fname))

def siasus_to_parquet(dbf_fname: str, path_to_file: str, output_fname: str, output_path: str):
    '''
        Convert SIASUS DBF file to parquet for fast throughput.

        Args:
        -----
            dbf_fname:
                String. Name of the DBF file (with extension).
            path_to_file:
                String. Complete path to the DBF file.
            output_path:
                ...
    '''
    sih_df = DBFIX( os.path.join(path_to_file, dbf_fname), codec='latin' ).to_dataframe()
    #sih_df["DT_INTER"] = pd.to_datetime(sih_df["DT_INTER"], format="%Y%m%d", errors="coerce")
    #sih_df["DT_SAIDA"] = pd.to_datetime(sih_df["DT_SAIDA"], format="%Y%m%d", errors="coerce")
    sih_df.to_parquet(os.path.join(output_path, output_fname))

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
        'MORTE', 'COMPLEX', 'COMPETEN', 'IDADE'
    ]
    year_str = f"{year}"[2:]
    month_lst = [ f'{n:2.0f}'.replace(' ', '0') for n in range(1,13) ]

    sih_df = []
    for month in month_lst:
        fname = f"RD{uf.upper()}{year_str}{month}.dbf"
        try:
            cur_df = DBFIX( os.path.join(path_to_files, fname), codec='latin' ).to_dataframe()
            sih_df.append( cur_df )
            #sih_df.append( cur_df[variables].copy() )
        except:
            pass
    
    sih_df = pd.concat(sih_df, axis=0)
    sih_df["DT_INTER"] = pd.to_datetime(sih_df["DT_INTER"], format="%Y%m%d", errors="coerce")
    sih_df["DT_SAIDA"] = pd.to_datetime(sih_df["DT_SAIDA"], format="%Y%m%d", errors="coerce")
    return sih_df
    
if __name__=="__main__":
    # -- change any path according to your own inputs and outputs.
    datapath = Path.home().joinpath("Documents", "data")
    input_folder = datapath.joinpath("opendatasus", "sihsus", "DBF")
    output_folder = datapath.joinpath("opendatasus", "sihsus", "PARQUET")

    months_ = [ f'{n:2.0f}'.replace(' ', '0') for n in range(1,13) ]
    years_ = [ f'{n:2.0f}'.replace(' ', '0') for n in range(8,23+1) ]

    # -- sihsus
    ufs = ['AL', 'BA', 'MA', 'CE', 'RN', 'SE', 'PI', 'PB', 'PE']
    ufs = ['SC', 'PR', 'RS']
    for uf in ufs:
        for year in years_:
            for month in months_:
                fname = f'RD{uf}{year}{month}'
                print(f'Arquivo {fname} ... ', end='')
                sihsus_to_parquet(fname+'.DBF', input_folder, fname+'.parquet', output_folder)
                print(f'feito.')

    # -- siasus
    #uf = 'CE'
    #for year in years_:
    #    for month in months_:
    #        fname = f'ATD{uf}{year}{month}'
    #        print(f'Arquivo {fname} ... ', end='')
    #        siasus_to_parquet(fname+'.DBF', input_folder, fname+'.parquet', output_folder)
    #        print(f'feito.')


    #uf = "CE"
    #for year in range(2008, 2022+1):
    #    print(f'ano {year} ... ', end='')
    #    sih_df = export_sihsus_year(uf, year, input_folder)
    #    sih_df.to_parquet(os.path.join(output_folder, f'RD{uf}_{year}.parquet'))
    #    print(f'feito.')