'''
    Interface to simplify operations of creation of the networks and manipulations.
    Library vocab: Networkx (include graph-tool later)

    Author: Higor S. Monteiro
    email: higor.monteiro@fisica.ufc.br
'''

import os
import numpy as np
import pandas as pd
from collections import defaultdict

import networkx as nx


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
        self.count_sum_edge_with_code = None
        self.cnes_df = self.cnes_df.merge(self.geodata_df[["MACRO_ID", "CRES_ID", "MACRO_NOME", "GEOCOD6"]], left_on="CODUFMUN", right_on="GEOCOD6", how="left").drop("GEOCOD6", axis=1)
        self.cnes_df["NUMLEITOS"] = self.cnes_df[["QTLEITP1", "QTLEITP2", "QTLEITP3"]].sum(axis=1)

        leitos_aux = self.cnes_df.groupby(["CODUFMUN"])["NUMLEITOS"].sum().reset_index().rename({"CODUFMUN": "GEOCOD6"}, axis=1)
        self.geodata_df = self.geodata_df.merge(leitos_aux, on="GEOCOD6", how="left")
        self.geodata_df["NUMLEITOS"] = self.geodata_df["NUMLEITOS"].apply(lambda x: 1 if pd.isna(x) or x==0 else x)

        self.graph = None
        self.code_to_muni_label = None
        self.code_to_hosp_label = None
        self.nodes_metadata = None
        self.edges_metadata = None

        # -- geographical position of nodes (to be used for drawing)
        self.geopos_net = {}

    def define_network(self):
        pass

    def calculate_fluxes(self):
        pass


class CityFlux(BaseFlux):
    
    def define_network(self):
        '''
            Define the nodes (cities) of the network and their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 
        '''
        self.graph = nx.DiGraph()

        municip_codes = self.geodata_df["GEOCOD6"].tolist()
        municip_names = self.geodata_df["NM_MUNICIP"].tolist()
        macro_ids = self.geodata_df["MACRO_ID"].tolist()
        macro_names = self.geodata_df["MACRO_NOME"].tolist()
        cres_ids = self.geodata_df["CRES_ID"].tolist()
        lat_, lon_ = self.geodata_df["municip_lat"].tolist(), self.geodata_df["municip_lon"].tolist()
        numleitos = self.geodata_df["NUMLEITOS"].tolist()

        # -- return -1 if city code not included in the network
        self.code_to_muni_label = defaultdict(lambda: -1, { municip_codes[n]: n for n in range(len(municip_codes)) })

        self.nodes_metadata = []
        for label, mun_code in enumerate(municip_codes):
            self.nodes_metadata.append(
                (label, {'municipio_code': mun_code, 
                         'municipio_name': municip_names[label],
                         'macro_id': macro_ids[label],
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'numleitos': numleitos[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )
        self.graph.add_nodes_from(self.nodes_metadata)
        for v in self.graph.nodes():
            self.geopos_net.update( {v: np.array([self.graph.nodes[v]['lon'], self.graph.nodes[v]['lat']])} )
        return self

    def calculate_fluxes(self, sih_df, icd_filter=None):
        '''
            Define the directed edges (flux of people and money between cities) of the 
            network and their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 
        '''
        if icd_filter is not None:
            sih_df["DIAG_PRINC3"] = sih_df["DIAG_PRINC"].apply(lambda x: x[:3])
            if type(icd_filter)==list:
                sih_df = sih_df[sih_df["DIAG_PRINC3"].isin(icd_filter)]
            elif type(icd_filter)==str:
                sih_df = sih_df[sih_df["DIAG_PRINC3"]==icd_filter]

        self.count_sum_edge_with_code = sih_df.groupby(["MUNIC_RES", "MUNIC_MOV"])["VAL_TOT"].agg(['sum', 'count']).reset_index()
        self.count_sum_edge_with_code = self.count_sum_edge_with_code[self.count_sum_edge_with_code["MUNIC_RES"]!=self.count_sum_edge_with_code["MUNIC_MOV"]]
        # -- get source node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_RES", right_on="GEOCOD6", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "source_macro", "CRES_ID": "source_cres", "MACRO_NOME": "source_macro_nome"}, axis=1)
        # -- get target node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_MOV", right_on="GEOCOD6", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "target_macro", "CRES_ID": "target_cres", "MACRO_NOME": "target_macro_nome"}, axis=1)
        # -- auxiliary metadata (for drawing)
        self.count_sum_edge_with_code["same_micro"] = self.count_sum_edge_with_code[["source_cres", "target_cres"]].apply(lambda x: x["source_cres"] if x["source_cres"]==x["target_cres"] else -1, axis=1)
        self.count_sum_edge_with_code["same_macro"] = self.count_sum_edge_with_code[["source_macro", "target_macro"]].apply(lambda x: x["source_macro"] if x["source_macro"]==x["target_macro"] else -1, axis=1)

        self.count_sum_edge_with_code["MUNIC_RES"] = self.count_sum_edge_with_code["MUNIC_RES"].apply(lambda x: self.code_to_muni_label[x])
        self.count_sum_edge_with_code["MUNIC_MOV"] = self.count_sum_edge_with_code["MUNIC_MOV"].apply(lambda x: self.code_to_muni_label[x])
        count_sum_edge_with_label = self.count_sum_edge_with_code[(self.count_sum_edge_with_code["MUNIC_RES"]!=-1) & (self.count_sum_edge_with_code["MUNIC_MOV"]!=-1)]
        
        # -- later
        #edges_with_labels_count['weight_normed'] = edges_with_labels_count['count']/edges_with_labels_count['count'].sum()
        #edges_with_labels_count['left_ADS'] = [ macro_ids[i[0]] for i in edges_with_labels_count.index ]
        #edges_with_labels_count['right_ADS'] = [ macro_ids[i[1]] for i in edges_with_labels_count.index ]
        #edges_with_labels_count['same ADS'] = edges_with_labels_count[['left_ADS', 'right_ADS']].apply(lambda x: '-1' if x['left_ADS']==x['right_ADS'] else x['left_ADS'], axis=1)

        self.edges_metadata = []
        for edge, row in count_sum_edge_with_label.iterrows():
            self.edges_metadata.append(
                (row["MUNIC_RES"], row["MUNIC_MOV"], {'admission_count': row['count'], 'total_cost': row['sum'], 
                                                      'source_macro': row['source_macro'], 'target_macro': row['target_macro'],
                                                      'source_micro': row['source_cres'], 'target_micro': row['target_cres'],
                                                      'same_macro' : row['same_macro'], 'same_micro': row['same_micro'] })
            )
        self.graph.add_edges_from(self.edges_metadata)
        return self
    
    def to_gml(self, output):
        if self.graph is not None:
            nx.write_gml(self.graph, os.path.join(output))
    
class CityHospitalFlux(BaseFlux):
    
    def define_network(self):
        '''
            Define the nodes (cities and hospitals) of the bipartite network and 
            their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 
        '''
        self.graph = nx.Graph()

        # -- define nodes defining the cities
        municip_codes = self.geodata_df["GEOCOD6"].tolist()
        municip_names = self.geodata_df["NM_MUNICIP"].tolist()
        macro_ids = self.geodata_df["MACRO_ID"].tolist()
        macro_names = self.geodata_df["MACRO_NOME"].tolist()
        cres_ids = self.geodata_df["CRES_ID"].tolist()
        lat_, lon_ = self.geodata_df["municip_lat"].tolist(), self.geodata_df["municip_lon"].tolist()

        # -- return -1 if city code not included in the network
        self.code_to_muni_label = defaultdict(lambda: -1, { municip_codes[n]: n for n in range(len(municip_codes)) })

        self.nodes_metadata = []
        for label, mun_code in enumerate(municip_codes):
            self.nodes_metadata.append(
                (label, {'type': 'city',
                         'code': mun_code, 
                         'name': municip_names[label],
                         'municip_code': mun_code,
                         'macro_id': macro_ids[label],
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )
        
        # -- define nodes defining the hospitals
        hospital_codes = self.cnes_df["CNES"].tolist()
        hospital_municode = self.cnes_df["CODUFMUN"].tolist()
        macro_ids = self.cnes_df["MACRO_ID"].tolist()
        macro_names = self.cnes_df["MACRO_NOME"].tolist()
        cres_ids = self.cnes_df["CRES_ID"].tolist()
        lat_, lon_ = self.cnes_df["latitude"].tolist(), self.cnes_df["longitude"].tolist()

        # -- return -1 if hospital code not included in the network
        self.code_to_hosp_label = defaultdict(lambda: -1, { hospital_codes[n]: n+label for n in range(len(hospital_codes)) })

        for label_hosp, hosp_code in enumerate(hospital_codes):
            self.nodes_metadata.append(
                (label_hosp+label, {'type': 'hospital',
                                    'code': hosp_code, 
                                    'name': '',
                                    'municip_code': hospital_municode[label_hosp],
                                    'macro_id': macro_ids[label_hosp],
                                    'macro_name': macro_names[label_hosp], 
                                    'cres_id': cres_ids[label_hosp],
                                    'lat': lat_[label_hosp],
                                    'lon': lon_[label_hosp] } )
            )
        
        self.graph.add_nodes_from(self.nodes_metadata)
        return self

    def calculate_fluxes(self, sih_df, icd_filter=None):
        '''
            Define the directed edges (flux of people and money between cities and hospitals) 
            of the network and their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 
        '''
        if icd_filter is not None:
            sih_df["DIAG_PRINC3"] = sih_df["DIAG_PRINC"].apply(lambda x: x[:3])
            if type(icd_filter)==list:
                sih_df = sih_df[sih_df["DIAG_PRINC3"].isin(icd_filter)]
            elif type(icd_filter)==str:
                sih_df = sih_df[sih_df["DIAG_PRINC3"]==icd_filter]

        # -- total cost (slow way - find faster way - vectorize!)
        self.count_sum_edge_with_code = sih_df.groupby(["MUNIC_RES", "CNES"])["VAL_TOT"].agg(['sum', 'count']).reset_index()
        # -- in this case, there is no self-edges, only cities where the hospital is
        #self.count_sum_edge_with_code = self.count_sum_edge_with_code[self.count_sum_edge_with_code["MUNIC_RES"]!=self.count_sum_edge_with_code["MUNIC_MOV"]]
        # -- get source node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_RES", right_on="GEOCOD6", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "source_macro", "CRES_ID": "source_cres", "MACRO_NOME": "source_macro_nome"}, axis=1)
        # -- get target node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.cnes_df[["CNES", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="CNES", right_on="CNES", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "target_macro", "CRES_ID": "target_cres", "MACRO_NOME": "target_macro_nome"}, axis=1)
        # -- auxiliary metadata (for drawing)
        self.count_sum_edge_with_code["same_micro"] = self.count_sum_edge_with_code[["source_cres", "target_cres"]].apply(lambda x: x["source_cres"] if x["source_cres"]==x["target_cres"] else -1, axis=1)
        self.count_sum_edge_with_code["same_macro"] = self.count_sum_edge_with_code[["source_macro", "target_macro"]].apply(lambda x: x["source_macro"] if x["source_macro"]==x["target_macro"] else -1, axis=1)

        self.count_sum_edge_with_code["MUNIC_RES"] = self.count_sum_edge_with_code["MUNIC_RES"].apply(lambda x: self.code_to_muni_label[x])
        self.count_sum_edge_with_code["CNES"] = self.count_sum_edge_with_code["CNES"].apply(lambda x: self.code_to_hosp_label[x])
        count_sum_edge_with_label = self.count_sum_edge_with_code[(self.count_sum_edge_with_code["MUNIC_RES"]!=-1) & (self.count_sum_edge_with_code["CNES"]!=-1)]

        self.edges_metadata = []
        for edge, row in count_sum_edge_with_label.iterrows():
            self.edges_metadata.append(
                (row["MUNIC_RES"], row["CNES"], {'admission_count': row['count'], 'total_cost': row['sum'], 
                                                 'source_macro': row['source_macro'], 'target_macro': row['target_macro'],
                                                 'source_micro': row['source_cres'], 'target_micro': row['target_cres'],
                                                 'same_macro' : row['same_macro'], 'same_micro': row['same_micro'] })
            )
        self.graph.add_edges_from(self.edges_metadata)
        return self
    
    def to_gml(self, output):
        if self.graph is not None:
            nx.write_gml(self.graph, os.path.join(output))