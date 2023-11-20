import os
import glob
import numpy as np
import pandas as pd
import datetime as dt
import geopandas as gpd
from collections import defaultdict
from simpledbf import Dbf5

import networkx as nx

from fluxsus.DBFIX import DBFIX
import fluxsus.utils as utils

class FluxNets:
    '''
        Flux networks.

        Args:
        -----
            sihsus_df:
                pandas.DataFrame.
            city_geo_df:
                geopandas.GeoDataFrame. Dataframe containing the polygons 
                of the cities and extra metadata.
    '''
    def __init__(self, sihsus_df : pd.DataFrame, city_geo_df: gpd.GeoDataFrame) -> None:
        
        self.G = None
        self.sihsus_df = sihsus_df.copy()
        self.sihsus_sub_df = self.sihsus_df
        self.city_geo_df = city_geo_df.copy()

        # -- network metadata
        self.nodes_metadata = None
        self.edges_metadata = None

        self.icd_chapters_codes = {
            "I": [ 'A'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]+[ 'B'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99) ],
            "II": [ 'C'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]+[ 'D'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 49) ],
            "III": [ 'D'+f'{n:2.0f}'.replace(' ', '0') for n in range(50, 90) ],
            "V": [ 'F'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ],
            "IX": [ 'I'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 100) ],
            "X": [ 'J'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 100) ],
            "XX": [ 'V'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 100) ]+[ 'Y'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 99) ]
        }

        self.cgeo_hash = {
            "MUNIN_NOME": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["NM_MUNICIP"])),
            "MACRO_ID": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["MACRO"].astype(str))),
            "MACRO_NOME": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["Macro Nome"])),
            "CRES_ID": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["CRES"].astype(str))),
            "LAT": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["lat"])),
            "LON": defaultdict(lambda: '', zip(self.city_geo_df["GEO6"], self.city_geo_df["lon"])),
        }

        
    def build_flux_network(self, icd_chapter=None, include_munin=['23']):
        '''
            ...

            Args:
            -----
                icd_chapter:
                    String.
                include_munin:
                    List of Strings. The municipalities' codes to be 
                    included in the analysis.
                export_to:
                    String.

            Return:
            -------
                ...
        '''
        
        self.G = None
        self.sihsus_sub_df = self.sihsus_df
        if icd_chapter is not None and icd_chapter in self.icd_chapters_codes.keys():
            CHAPTER_CODES = self.icd_chapters_codes[icd_chapter]
            self.sihsus_sub_df = self.sihsus_df[ (self.sihsus_df["DIAG_PRINC"].isin( CHAPTER_CODES )) ] 

        mun_codes = np.unique( np.concatenate( (self.sihsus_sub_df["MUNIC_RES"].unique(), self.sihsus_sub_df["MUNIC_MOV"].unique()) ))
        # -- restrict to CEARÁ
        mun_codes = np.array( [ x for x in mun_codes if x[:2] in include_munin ] )

        # -- create metadata 
        mun_names = np.array( [ self.cgeo_hash['MUNIN_NOME'][mun_code] for mun_code in mun_codes] )
        macro_ids = np.array( [ self.cgeo_hash['MACRO_ID'][mun_code] for mun_code in mun_codes] )
        macro_names = np.array( [self.cgeo_hash['MACRO_NOME'][mun_code] for mun_code in mun_codes] )
        cres_ids = np.array( [ self.cgeo_hash['CRES_ID'][mun_code] for mun_code in mun_codes] )
        lat_ = np.array( [ self.cgeo_hash['LAT'][mun_code] for mun_code in mun_codes] )
        lon_ = np.array( [ self.cgeo_hash['LON'][mun_code] for mun_code in mun_codes] )

        # -- code 'mun_codes[i]' has node label 'i'.
        self.code_to_label = { mun_codes[x]: x for x in range(mun_codes.shape[0]) }

        # -- edgelist with format (i -> j)
        edges_with_codes_and_self = list( zip( self.sihsus_sub_df["MUNIC_RES"], self.sihsus_sub_df["MUNIC_MOV"] ) )

        edges_with_codes_and_self = [  x for x in edges_with_codes_and_self if x[0][:2] in include_munin and x[1][:2] in include_munin ]
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
        self.G = nx.DiGraph()
        self.G.add_nodes_from(self.nodes_metadata)
        self.G.add_edges_from(self.edges_metadata)

        #if export_to is not None:
        #    nx.write_gml(G, os.path.join(export_to))

    def to_gml(self, output):
        if self.G is not None:
            nx.write_gml(self.G, os.path.join(output))