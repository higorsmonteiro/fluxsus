'''
    Interface to simplify the creation of networks given the formatted data.
    Library vocab: Networkx (include graph-tool later)

    Author: Higor S. Monteiro
    email: higor.monteiro@fisica.ufc.br
'''

import os
import numpy as np
import pandas as pd
from collections import defaultdict
import networkx as nx

import fluxsus.fluxnets.fnets_utils as futils

class BaseFlux:
    def __init__(self, cnes_df, geodata_df):
        '''
            Interface to create flux networks referring to hospital admissions. A flux is defined when 
            an individual from a given city is admitted to a hospital in another city. Considering the data 
            source used, the information flowing within the network can be either individuals or the total 
            cost of procedures performed during the admissions.

            The input data follow specific formats and they had gone through a preprocessing stage before 
            their use in this class. The preprocessing stage is shown in the scripts that follow this class.

            Args:
            -----
                cnes_df:
                    pandas.DataFrame.
                geodata_df:
                    pandas.DataFrame.

            Attributes:
            -----------
                graph:
                    networkx.DiGraph or networkx.Graph.
        '''
        self.sih_df = None
        self.cnes_df = cnes_df.copy()
        self.geodata_df = geodata_df.copy()
        self.count_sum_edge_with_code = None

        self.cnes_df = self.cnes_df.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="CODUFMUN", right_on="GEOCOD6", how='left').drop("GEOCOD6", axis=1)

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
        macro_new_ids = self.geodata_df["MACRO_ID_PROPOSAL"].tolist() # -- optional
        macro_names = self.geodata_df["MACRO_NOME"].tolist()
        cres_ids = self.geodata_df["CRES_ID"].tolist()
        lat_, lon_ = self.geodata_df["municip_lat"].tolist(), self.geodata_df["municip_lon"].tolist()

        # -- return -1 if city code not included in the network
        self.code_to_muni_label = defaultdict(lambda: -1, { municip_codes[n]: n for n in range(len(municip_codes)) })

        self.nodes_metadata = []
        for label, mun_code in enumerate(municip_codes):
            self.nodes_metadata.append(
                (label, {'municipio_code': mun_code, 
                         'municipio_name': municip_names[label],
                         'macro_id': macro_ids[label],
                         'macro_new_id': macro_new_ids[label], # -- optional
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )
        self.graph.add_nodes_from(self.nodes_metadata)
        for v in self.graph.nodes():
            self.geopos_net.update( {v: np.array([self.graph.nodes[v]['lon'], self.graph.nodes[v]['lat']])} )
        return self

    def calculate_fluxes(self, sih_df, multilayer_icd=False):
        '''
            Define the directed edges (flux of people and money between cities) of the 
            network and their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 

            Args:
            -----
                sih_df:
                    pandas.DataFrame.
                multilayer_icd:
                    Bool. Whether we should include information of stratified fluxes
                    based on ICD-10 chapters.
        '''
        # -- define the period of the data that was used to define the fluxes.
        if 'COMPETEN' in sih_df.columns:
            self.graph.graph['init_period'] = sih_df["COMPETEN"].min()
            self.graph.graph['final_period'] = sih_df["COMPETEN"].max()

        if multilayer_icd:
            sih_df["DIAG_PRINC3"] = sih_df["DIAG_PRINC"].apply(lambda x: x[:3])

        self.count_sum_edge_with_code = sih_df.groupby(["MUNIC_RES", "MUNIC_MOV"])["VAL_TOT"].agg(['sum', 'count']).reset_index()
        self.count_sum_edge_with_code = self.count_sum_edge_with_code[self.count_sum_edge_with_code["MUNIC_RES"]!=self.count_sum_edge_with_code["MUNIC_MOV"]]
        # -- get source node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_RES", right_on="GEOCOD6", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "source_macro", "CRES_ID": "source_cres", "MACRO_NOME": "source_macro_nome"}, axis=1)
        # -- get target node info
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.merge(self.geodata_df[["GEOCOD6", "MACRO_ID", "CRES_ID", "MACRO_NOME"]], left_on="MUNIC_MOV", right_on="GEOCOD6", how="left")
        self.count_sum_edge_with_code = self.count_sum_edge_with_code.rename({"MACRO_ID": "target_macro", "CRES_ID": "target_cres", "MACRO_NOME": "target_macro_nome"}, axis=1)
        # -- auxiliary metadata (probably for drawing)
        self.count_sum_edge_with_code["same_micro"] = self.count_sum_edge_with_code[["source_cres", "target_cres"]].apply(lambda x: x["source_cres"] if x["source_cres"]==x["target_cres"] else -1, axis=1)
        self.count_sum_edge_with_code["same_macro"] = self.count_sum_edge_with_code[["source_macro", "target_macro"]].apply(lambda x: x["source_macro"] if x["source_macro"]==x["target_macro"] else -1, axis=1)

        self.count_sum_edge_with_code["MUNIC_RES"] = self.count_sum_edge_with_code["MUNIC_RES"].apply(lambda x: self.code_to_muni_label[x])
        self.count_sum_edge_with_code["MUNIC_MOV"] = self.count_sum_edge_with_code["MUNIC_MOV"].apply(lambda x: self.code_to_muni_label[x])
        count_sum_edge_with_label = self.count_sum_edge_with_code[(self.count_sum_edge_with_code["MUNIC_RES"]!=-1) & (self.count_sum_edge_with_code["MUNIC_MOV"]!=-1)]

        # -- include multilayered information on edges (fluxes stratified by ICD-10 chapter)
        if multilayer_icd:
            chapters = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 
                        'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI',
                        'XVII', 'XVIII', 'XIX', 'XX', 'XXI', 'XXII']
            for chapter in chapters:
                ch_codes = futils.filter_chapter(chapter)
                count_sum_edge_strat_icd = sih_df[sih_df["DIAG_PRINC3"].isin(ch_codes)].groupby(["MUNIC_RES", "MUNIC_MOV"])["VAL_TOT"].agg(['sum', 'count']).reset_index().rename({'sum': f'sum_ch{chapter}', 'count': f'count_ch{chapter}'}, axis=1)
                count_sum_edge_strat_icd = count_sum_edge_strat_icd[count_sum_edge_strat_icd["MUNIC_RES"]!=count_sum_edge_strat_icd["MUNIC_MOV"]]
                count_sum_edge_strat_icd["MUNIC_RES"] = count_sum_edge_strat_icd["MUNIC_RES"].apply(lambda x: self.code_to_muni_label[x])
                count_sum_edge_strat_icd["MUNIC_MOV"] = count_sum_edge_strat_icd["MUNIC_MOV"].apply(lambda x: self.code_to_muni_label[x])
                count_sum_edge_strat_icd = count_sum_edge_strat_icd[(count_sum_edge_strat_icd["MUNIC_RES"]!=-1) & (count_sum_edge_strat_icd["MUNIC_MOV"]!=-1)]
                # -- join with the main dataframe
                count_sum_edge_with_label = count_sum_edge_with_label.merge(count_sum_edge_strat_icd, on=["MUNIC_RES", "MUNIC_MOV"], how='left').fillna(0)

        self.edges_metadata = []
        for edge, row in count_sum_edge_with_label.iterrows():
            self.edges_metadata.append(
                (row["MUNIC_RES"], row["MUNIC_MOV"], {'admission_count': row['count'], 'total_cost': row['sum'], 
                                                      'source_macro': row['source_macro'], 'target_macro': row['target_macro'],
                                                      'source_micro': row['source_cres'], 'target_micro': row['target_cres'],
                                                      'same_macro' : row['same_macro'], 'same_micro': row['same_micro'],
                                                      'admission_count_ch1': row['count_chI'], 'total_cost_ch1': row['sum_chI'],
                                                      'admission_count_ch2': row['count_chII'], 'total_cost_ch2': row['sum_chII'],
                                                      'admission_count_ch3': row['count_chIII'], 'total_cost_ch3': row['sum_chIII'],
                                                      'admission_count_ch4': row['count_chIV'], 'total_cost_ch4': row['sum_chIV'],
                                                      'admission_count_ch5': row['count_chV'], 'total_cost_ch5': row['sum_chV'],
                                                      'admission_count_ch6': row['count_chVI'], 'total_cost_ch6': row['sum_chVI'],
                                                      'admission_count_ch7': row['count_chVII'], 'total_cost_ch7': row['sum_chVII'],
                                                      'admission_count_ch8': row['count_chVIII'], 'total_cost_ch8': row['sum_chVIII'],
                                                      'admission_count_ch9': row['count_chIX'], 'total_cost_ch9': row['sum_chIX'],
                                                      'admission_count_ch10': row['count_chX'], 'total_cost_ch10': row['sum_chX'],
                                                      'admission_count_ch11': row['count_chXI'], 'total_cost_ch11': row['sum_chXI'],
                                                      'admission_count_ch12': row['count_chXII'], 'total_cost_ch12': row['sum_chXII'],
                                                      'admission_count_ch13': row['count_chXIII'], 'total_cost_ch13': row['sum_chXIII'],
                                                      'admission_count_ch14': row['count_chXIV'], 'total_cost_ch14': row['sum_chXIV'],
                                                      'admission_count_ch15': row['count_chXV'], 'total_cost_ch15': row['sum_chXV'],
                                                      'admission_count_ch16': row['count_chXVI'], 'total_cost_ch16': row['sum_chXVI'],
                                                      'admission_count_ch17': row['count_chXVII'], 'total_cost_ch17': row['sum_chXVII'],
                                                      'admission_count_ch18': row['count_chXVIII'], 'total_cost_ch18': row['sum_chXVIII'],
                                                      'admission_count_ch19': row['count_chXIX'], 'total_cost_ch19': row['sum_chXIX'],
                                                      'admission_count_ch20': row['count_chXX'], 'total_cost_ch20': row['sum_chXX'],
                                                      'admission_count_ch21': row['count_chXXI'], 'total_cost_ch21': row['sum_chXXI'],
                                                      'admission_count_ch22': row['count_chXXII'], 'total_cost_ch22': row['sum_chXXII'], })
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
        macro_new_ids = self.geodata_df["MACRO_ID_PROPOSAL"].tolist() # -- optional
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
                         'macro_new_id': macro_new_ids[label], # -- optional
                         'macro_name': macro_names[label], 
                         'cres_id': cres_ids[label],
                         'lat': lat_[label],
                         'lon': lon_[label] } )
            )
        
        # -- define nodes defining the hospitals (do not include new macro proposal, since not in data)
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

    def calculate_fluxes(self, sih_df, multilayer_icd=False):
        '''
            Define the directed edges (flux of people and money between cities and hospitals) 
            of the network and their metadata.

            Metadata refers to the name and code of a city, and the ids of the
            micro/macro regions for which the city belongs to. 
        '''
        if 'COMPETEN' in sih_df.columns:
            self.graph.graph['init_period'] = sih_df["COMPETEN"].min()
            self.graph.graph['final_period'] = sih_df["COMPETEN"].max()

        if multilayer_icd:
            sih_df["DIAG_PRINC3"] = sih_df["DIAG_PRINC"].apply(lambda x: x[:3])


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

        # -- include multilayered information on edges (fluxes stratified by ICD-10 chapter)
        if multilayer_icd:
            chapters = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 
                        'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI',
                        'XVII', 'XVIII', 'XIX', 'XX', 'XXI', 'XXII']
            for chapter in chapters:
                ch_codes = futils.filter_chapter(chapter)
                count_sum_edge_strat_icd = sih_df[sih_df["DIAG_PRINC3"].isin(ch_codes)].groupby(["MUNIC_RES", "CNES"])["VAL_TOT"].agg(['sum', 'count']).reset_index().rename({'sum': f'sum_ch{chapter}', 'count': f'count_ch{chapter}'}, axis=1)
                count_sum_edge_strat_icd = count_sum_edge_strat_icd[count_sum_edge_strat_icd["MUNIC_RES"]!=count_sum_edge_strat_icd["CNES"]]
                count_sum_edge_strat_icd["MUNIC_RES"] = count_sum_edge_strat_icd["MUNIC_RES"].apply(lambda x: self.code_to_muni_label[x])
                count_sum_edge_strat_icd["CNES"] = count_sum_edge_strat_icd["CNES"].apply(lambda x: self.code_to_muni_label[x])
                count_sum_edge_strat_icd = count_sum_edge_strat_icd[(count_sum_edge_strat_icd["MUNIC_RES"]!=-1) & (count_sum_edge_strat_icd["CNES"]!=-1)]
                # -- join with the main dataframe
                count_sum_edge_with_label = count_sum_edge_with_label.merge(count_sum_edge_strat_icd, on=["MUNIC_RES", "CNES"], how='left').fillna(0)

        self.edges_metadata = []
        for edge, row in count_sum_edge_with_label.iterrows():
            self.edges_metadata.append(
                (row["MUNIC_RES"], row["CNES"], {'admission_count': row['count'], 'total_cost': row['sum'], 
                                                 'source_macro': row['source_macro'], 'target_macro': row['target_macro'],
                                                 'source_micro': row['source_cres'], 'target_micro': row['target_cres'],
                                                 'same_macro' : row['same_macro'], 'same_micro': row['same_micro'],
                                                 'admission_count_ch1': row['count_chI'], 'total_cost_ch1': row['sum_chI'],
                                                 'admission_count_ch2': row['count_chII'], 'total_cost_ch2': row['sum_chII'],
                                                 'admission_count_ch3': row['count_chIII'], 'total_cost_ch3': row['sum_chIII'],
                                                 'admission_count_ch4': row['count_chIV'], 'total_cost_ch4': row['sum_chIV'],
                                                 'admission_count_ch5': row['count_chV'], 'total_cost_ch5': row['sum_chV'],
                                                 'admission_count_ch6': row['count_chVI'], 'total_cost_ch6': row['sum_chVI'],
                                                 'admission_count_ch7': row['count_chVII'], 'total_cost_ch7': row['sum_chVII'],
                                                 'admission_count_ch8': row['count_chVIII'], 'total_cost_ch8': row['sum_chVIII'],
                                                 'admission_count_ch9': row['count_chIX'], 'total_cost_ch9': row['sum_chIX'],
                                                 'admission_count_ch10': row['count_chX'], 'total_cost_ch10': row['sum_chX'],
                                                 'admission_count_ch11': row['count_chXI'], 'total_cost_ch11': row['sum_chXI'],
                                                 'admission_count_ch12': row['count_chXII'], 'total_cost_ch12': row['sum_chXII'],
                                                 'admission_count_ch13': row['count_chXIII'], 'total_cost_ch13': row['sum_chXIII'],
                                                 'admission_count_ch14': row['count_chXIV'], 'total_cost_ch14': row['sum_chXIV'],
                                                 'admission_count_ch15': row['count_chXV'], 'total_cost_ch15': row['sum_chXV'],
                                                 'admission_count_ch16': row['count_chXVI'], 'total_cost_ch16': row['sum_chXVI'],
                                                 'admission_count_ch17': row['count_chXVII'], 'total_cost_ch17': row['sum_chXVII'],
                                                 'admission_count_ch18': row['count_chXVIII'], 'total_cost_ch18': row['sum_chXVIII'],
                                                 'admission_count_ch19': row['count_chXIX'], 'total_cost_ch19': row['sum_chXIX'],
                                                 'admission_count_ch20': row['count_chXX'], 'total_cost_ch20': row['sum_chXX'],
                                                 'admission_count_ch21': row['count_chXXI'], 'total_cost_ch21': row['sum_chXXI'],
                                                 'admission_count_ch22': row['count_chXXII'], 'total_cost_ch22': row['sum_chXXII'], })
            )
        self.graph.add_edges_from(self.edges_metadata)
        return self
    
    def to_gml(self, output):
        if self.graph is not None:
            nx.write_gml(self.graph, os.path.join(output))