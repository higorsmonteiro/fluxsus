import os
import glob
import numpy as np
import pandas as pd
import datetime as dt
import geopandas as gpd
from collections import defaultdict

from simpledbf import Dbf5
from DBFIX import DBFIX

import utils

import networkx as nx

class NetSUS:
    '''
        Generate healthcare flux network from AIH's SUS data.
    '''
    def __init__(self, cid_chapter=None) -> None:
        self.cid_chapter = cid_chapter
        self.cnes_df = None
        self.sih_df = None
        self.mun_df = None

        # network metadata
        self.nodes_metadata = None
        self.edges_metadata = None

    def load_cnes(self, fname, path_to_file):
        '''
            Load CNES data.

            Args:
            -----
                fname:
                    String.
                path_to_file:
                    String.
        '''
        self.cnes_df = DBFIX( os.path.join(path_to_file, fname), codec='latin' ).to_dataframe()
        return self

    def load_sih(self, uf:str, ano_inicio:int, ano_final:int, path_to_files:str):
        '''
            Load SIHSUS data (specifically AIH related).
        '''
        # filter chapter

        ano_range = range(ano_inicio, ano_final+1)
        files_lst = [ f"RD{uf.upper()}"+f"{ano}"[2:] for ano in ano_range ]

        SIH_FILES = glob.glob(os.path.join(path_to_files, "*.dbf"))
        SIH_FILES = [ n for n in SIH_FILES for subfname in files_lst if n.__contains__(subfname) ]
        
        subcols = ["N_AIH", "CNES", "MUNIC_RES", "DIAG_PRINC", "DIAG_SECUN"]

        sih_df = []
        for fname in SIH_FILES:
            # -- open current month data
            cur_df = DBFIX(fname, codec='latin').to_dataframe()

            if self.cid_chapter is None:
                pass
            else:
                chapters = utils.filter_chapter(self.cid_chapter)
                cur_df["DIAG_PRINC_3DIG"] = cur_df["DIAG_PRINC"].apply(lambda x: x[:3])
                cur_df = cur_df[ cur_df["DIAG_PRINC_3DIG"].isin(chapters) ]

            sih_df.append( cur_df[subcols] )
        
        self.sih_df = pd.concat(sih_df)
        sih_df = None
        return self

    def load_geo(self, path_to_files):
        '''
            Load geographic information.
        '''
        # -- load geo info
        mun_df = gpd.read_file( os.path.join(path_to_files, "Ceará MUN.shp") )
        macro_df = gpd.read_file( os.path.join(path_to_files, "Macro Ceará.shp") )

        macro_df["ID"] = macro_df["ID"].astype(int)
        mun_df["GEO6"] = mun_df["GEO6"].astype(str)
        
        self.mun_df = mun_df.merge(macro_df[["ID", "Macro Shee"]], left_on="MACRO", right_on="ID").drop("ID", axis=1).rename({"Macro Shee": "Macro Nome"}, axis=1)
        self.mun_df["centroid"] = self.mun_df.centroid
        self.mun_df['lon'] = self.mun_df['centroid'].apply(lambda x: x.x)
        self.mun_df['lat'] = self.mun_df['centroid'].apply(lambda x: x.y)
        self.mun_df = self.mun_df.drop('centroid', axis=1)

        # -- create hash
        self.hash_munin_nome = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["NM_MUNICIP"]))
        self.hash_macro_id = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["MACRO"].astype(str)))
        self.hash_macro_nome = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["Macro Nome"]))
        self.hash_cres_id = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["CRES"].astype(str)))
        
        self.hash_lat = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["lat"]))
        self.hash_lon = defaultdict(lambda: '', zip(self.mun_df["GEO6"], self.mun_df["lon"]))
        return self
    
    def build_network(self, export_to):
        '''
        
        '''
        # -- linkage of AIHs and CNES
        subset_cols = ["CNES", "CODUFMUN", "MUNIC_RES"]
        sih_df_2 = self.sih_df.merge(self.cnes_df[["CNES", "CODUFMUN"]], on="CNES", how="left")[subset_cols]
        sih_df_2 = sih_df_2.dropna(axis=0, how='any')

        mun_codes = np.unique(np.concatenate( (sih_df_2["MUNIC_RES"].unique(), sih_df_2["CODUFMUN"].unique()) ))
        # -- restrict to CEARÁ
        mun_codes = np.array( [ x for x in mun_codes if x[:2]=='23' ] )

        # -- create metadata
        mun_names = np.array( [ self.hash_munin_nome[mun_code] for mun_code in mun_codes] )
        macro_ids = np.array( [ self.hash_macro_id[mun_code] for mun_code in mun_codes] )
        macro_names = np.array( [ self.hash_macro_nome[mun_code] for mun_code in mun_codes] )
        cres_ids = np.array( [ self.hash_cres_id[mun_code] for mun_code in mun_codes] )
        lat_ = np.array( [ self.hash_lat[mun_code] for mun_code in mun_codes] )
        lon_ = np.array( [ self.hash_lon[mun_code] for mun_code in mun_codes] )

        # -- code 'mun_codes[i]' has node label 'i'.
        self.code_to_label = { mun_codes[x]: x for x in range(mun_codes.shape[0]) }

        # -- edgelist with format (i -> j)
        edges_with_codes_and_self = list( zip( sih_df_2["MUNIC_RES"], sih_df_2["CODUFMUN"] ) )

        edges_with_codes_and_self = [  x for x in edges_with_codes_and_self if x[0][:2]=='23' and x[1][:2]=='23' ]
        # -- remove self edges
        edges_with_codes_and_noself = [  x for x in edges_with_codes_and_self if x[0]!=x[1] ]

        # -- edgelist with node labels
        edges_with_labels_and_noself = [ (self.code_to_label[x[0]], self.code_to_label[x[1]]) for x in edges_with_codes_and_noself ]

        edges_with_labels_count = pd.DataFrame( pd.Series(edges_with_labels_and_noself).value_counts() )
        edges_with_labels_count['weight_normed'] = edges_with_labels_count['count']/edges_with_labels_count['count'].sum()
        edges_with_labels_count['left_ADS'] = [ macro_ids[i[0]] for i in edges_with_labels_count.index ]
        edges_with_labels_count['right_ADS'] = [ macro_ids[i[1]] for i in edges_with_labels_count.index ]
        edges_with_labels_count['same ADS'] = edges_with_labels_count[['left_ADS', 'right_ADS']].apply(lambda x: '-1' if x['left_ADS']==x['right_ADS'] else x['left_ADS'], axis=1)

        self.nodes_metadata = []
        for label, mun_code in enumerate(mun_codes):
            self.nodes_metadata.append(
                (label, {'municipio_code': mun_code, 
                         'municipio_name': mun_names[label],
                         'macro_id': macro_ids[label],
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )

        self.edges_metadata = []
        for edge, row in edges_with_labels_count.iterrows():
            self.edges_metadata.append(
                (edge[0], edge[1], {'count': row['count'], 'norm': row['weight_normed'], 'same_ads': row['same ADS']})
            )

        # -- 
        G = nx.DiGraph()
        G.add_nodes_from(self.nodes_metadata)
        G.add_edges_from(self.edges_metadata)

        nx.write_gml(G, os.path.join(export_to))