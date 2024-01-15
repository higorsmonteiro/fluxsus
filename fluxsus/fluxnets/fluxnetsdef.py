'''
    Interface to simplify operations of creation of the networks and manipulations.
    Library vocab: Networkx (include graph-tool later)

    Author: Higor S. Monteiro
    email: higor.monteiro@fisica.ufc.br
'''

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


class BaseFlux:
    def __init__(self, cnes_df, geodata_df):
        '''
            Interface to create flux networks referring to hospital admissions. A flux is defined when 
            an individual from a given city is admitted to a hospital in another city. Considering the data 
            source used, the information flowing within the network can be either individuals or the total 
            cost of procedures during an admission.

            Args:
            -----
                cnes_df:
                    pandas.DataFrame.
                geodata_df:
                    pandas.DataFrame.
        '''
        self.sih_df = None
        self.cnes_df = cnes_df.copy()
        self.geodata_df = geodata_df.copy()

        self.graph = None
        self.code_to_label = None
        self.nodes_metadata = None
        self.edges_metadata = None

        # -- geographical position of nodes (to be used for drawing)
        self.geopos_net = {}

    def define_network(self):
        pass

    def calculate_fluxes(self):
        pass


class CityFluxNet(BaseFlux):
    
    def define_network(self):
        '''
            Define the nodes of the network and their metadata. 
        '''
        self.graph = nx.DiGraph()

        municip_codes = self.geodata_df["GEOCOD6"].tolist()
        municip_names = self.geodata_df["NM_MUNICIP"].tolist()
        macro_ids = self.geodata_df["MACRO_ID"].tolist()
        macro_names = self.geodata_df["MACRO_NOME"].tolist()
        cres_ids = self.geodata_df["CRES_ID"].tolist()
        lat_, lon_ = self.geodata_df["municip_lat"].tolist(), self.geodata_df["municip_lon"].tolist()

        # -- return -1 if city code not included in the network
        self.code_to_label = defaultdict(lambda: -1, { municip_codes[n]: n for n in range(len(municip_codes)) })

        self.nodes_metadata = []
        for label, mun_code in enumerate(municip_codes):
            self.nodes_metadata.append(
                (label, {'municipio_code': mun_code, 
                         'municipio_name': municip_names[label],
                         'macro_id': macro_ids[label],
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )
        self.graph.add_nodes_from(self.nodes_metadata)
        for v in self.graph.nodes():
            self.geopos_net.update( {v: np.array([self.graph.nodes[v]['lon'], self.graph.nodes[v]['lat']])} )
        return self

    def calculate_fluxes(self, sih_df, icd_filter=None):
        '''
        
        '''
        if icd_filter is not None:
            sih_df["DIAG_PRINC3"] = sih_df["DIAG_PRINC"].apply(lambda x: x[:3])
            if type(icd_filter)==list:
                sih_df = sih_df[sih_df["DIAG_PRINC3"].isin(icd_filter)]
            elif type(icd_filter)==str:
                sih_df = sih_df[sih_df["DIAG_PRINC3"]==icd_filter]

        # -- total cost (slow way - find faster way - vectorize!)
        count_sum_edge_with_code = sih_df.groupby(["MUNIC_RES", "MUNIC_MOV"]).agg(['sum', 'count'])["VAL_TOT"].reset_index()
        count_sum_edge_with_code = count_sum_edge_with_code[count_sum_edge_with_code["MUNIC_RES"]!=count_sum_edge_with_code["MUNIC_MOV"]]
        # -- get source node info
        count_sum_edge_with_code = count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_RES", right_on="GEOCOD6", how="left")
        count_sum_edge_with_code = count_sum_edge_with_code.rename({"MACRO_ID": "source_macro", "CRES_ID": "source_cres", "MACRO_NOME": "source_macro_nome"}, axis=1)
        # -- get target node info
        count_sum_edge_with_code = count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_MOV", right_on="GEOCOD6", how="left")
        count_sum_edge_with_code = count_sum_edge_with_code.rename({"MACRO_ID": "target_macro", "CRES_ID": "target_cres", "MACRO_NOME": "target_macro_nome"}, axis=1)


        count_sum_edge_with_code["MUNIC_RES"] = count_sum_edge_with_code["MUNIC_RES"].apply(lambda x: self.code_to_label[x])
        count_sum_edge_with_code["MUNIC_MOV"] = count_sum_edge_with_code["MUNIC_MOV"].apply(lambda x: self.code_to_label[x])
        count_sum_edge_with_label = count_sum_edge_with_code[(count_sum_edge_with_code["MUNIC_RES"]!=-1) & (count_sum_edge_with_code["MUNIC_MOV"]!=-1)]

        # -- edgelist with format (i -> j)
        #edges_with_codes_and_self = list( zip( sih_df["MUNIC_RES"], sih_df["MUNIC_MOV"] ) )
        # -- remove self edges
        #edges_with_codes_and_noself = [ x for x in edges_with_codes_and_self if x[0]!=x[1] ]
        # -- edgelist with node labels
        #edges_with_labels_and_noself = [ (self.code_to_label[x[0]], self.code_to_label[x[1]]) for x in edges_with_codes_and_noself if self.code_to_label[x[0]]!=-1 and self.code_to_label[x[1]]!=-1 ]
        #edges_with_labels_count = pd.DataFrame( pd.Series(edges_with_labels_and_noself).value_counts() )
        
        # -- later
        #edges_with_labels_count['weight_normed'] = edges_with_labels_count['count']/edges_with_labels_count['count'].sum()
        #edges_with_labels_count['left_ADS'] = [ macro_ids[i[0]] for i in edges_with_labels_count.index ]
        #edges_with_labels_count['right_ADS'] = [ macro_ids[i[1]] for i in edges_with_labels_count.index ]
        #edges_with_labels_count['same ADS'] = edges_with_labels_count[['left_ADS', 'right_ADS']].apply(lambda x: '-1' if x['left_ADS']==x['right_ADS'] else x['left_ADS'], axis=1)

        self.edges_metadata = []
        #for edge, row in edges_with_labels_count.iterrows():
        for edge, row in count_sum_edge_with_label.iterrows():
            self.edges_metadata.append(
                #(edge[0], edge[1], {'count': row['count'], 'norm': row['weight_normed'], 'same_ads': row['same ADS']})
                #(edge[0], edge[1], {'admission_count': row['count']})
                (row["MUNIC_RES"], row["MUNIC_MOV"], {'admission_count': row['count'], 'total_cost': row['sum'], 
                                                      'source_macro': row['source_macro'], 'target_macro': row['target_macro']})
            )
        self.graph.add_edges_from(self.edges_metadata)
        return self
    
class HospitalFluxNet(BaseFlux):
    
    def define_network(self):
        '''
            Define the nodes of the network and their metadata. 
        '''
        self.graph = nx.DiGraph()
        pass

    def calculate_fluxes(self, sih_df, icd_filter=None):
        '''
        
        '''
        pass