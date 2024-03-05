import pandas as pd
import networkx as nx
from fluxsus.utils_ import f_infomap

class NetProperties:
    '''
        Interface for some calculations on the built networks (city flux and hospitals).

        Args:
        -----
            graph:
                networkx.DiGraph.
    '''
    def __init__(self, graph) -> None:
        self.graph = graph

    def calculate_in_flow(self, weight_people_col=None, weight_cost_col=None, 
                          people_property_name='incoming_people', 
                          cost_property_name='incoming_cost'):
        ''' 
            Given a directed graph, calculate the total incoming flow of
            individuals and/or costs per node.

            Args:
            -----
                graph:
                    networkx.DiGraph.
                weight_people_col:
                    String. Name of the edge property that should be used to 
                    calculate the flow of people.
                weight_cost_col:
                    String. Name of the edge property that should be used to 
                    calculate the flow of costs (BRL).
            Return:
            -------
                graph:
                    networkx.DiGraph. The input network augmented with new node
                    properties referring to the total flows calculated.
        '''
        if not nx.is_directed(self.graph):
            raise Exception('self.graph parsed is not directed.')

        # -- aggregate incoming information
        for u in self.graph.nodes():
            in_edges = [ e for e in self.graph.in_edges(u) ]
            self.graph.nodes[u][people_property_name] = 0
            self.graph.nodes[u][cost_property_name] = 0
            for e in in_edges:
                self.graph.nodes[u][people_property_name] += self.graph.edges[e][weight_people_col]
                self.graph.nodes[u][cost_property_name] += self.graph.edges[e][weight_cost_col]

        # -- create new weight based on the number of hospital beds
        #for u, v in self.graph.edges():
        #    numleitos = self.graph.nodes[u]['numleitos']
        #    self.graph.edges[(u,v)]['outflow_per_hospbed'] = self.graph.edges[(u,v)]['admission_count']/numleitos
        return self
    

    def calculate_out_flow(self, weight_people_col=None, weight_cost_col=None, 
                           people_property_name='out_people', 
                           cost_property_name='out_cost'):
        ''' 
            Given a directed graph, calculate the total outgoing flow of
            individuals and/or costs per node.

            Args:
            -----
                graph:
                    networkx.DiGraph.
                weight_people_col:
                    String. Name of the edge property that should be used to 
                    calculate the flow of people.
                weight_cost_col:
                    String. Name of the edge property that should be used to 
                    calculate the flow of costs (BRL).
            Return:
            -------
                graph:
                    networkx.DiGraph. The input network augmented with new node
                    properties referring to the total flows calculated.
        '''
        if not nx.is_directed(self.graph):
            raise Exception('self.graph parsed is not directed.')

        # -- aggregate outgoing information
        for u in self.graph.nodes():
            out_edges = [ e for e in self.graph.out_edges(u) ]
            self.graph.nodes[u][people_property_name] = 0
            self.graph.nodes[u][cost_property_name] = 0
            for e in out_edges:
                self.graph.nodes[u][people_property_name] += self.graph.edges[e][weight_people_col]
                self.graph.nodes[u][cost_property_name] += self.graph.edges[e][weight_cost_col]

        return self

    def get_hospitalbeds(self, cnes_df, geodata_df):
        '''
            Add a node property to the graph referring to the number of hospital beds
            available in the city.

            note: some complexities are not included - temporality not include (some hospitals 
            might exist only after a given date, therefore it might include bias)
        '''
        cnes_df = cnes_df.merge(self.geodata_df[["MACRO_ID", "CRES_ID", "MACRO_NOME", "GEOCOD6"]], left_on="CODUFMUN", right_on="GEOCOD6", how="left").drop("GEOCOD6", axis=1)
        cnes_df["NUMLEITOS"] = cnes_df[["QTLEITP1", "QTLEITP2", "QTLEITP3"]].sum(axis=1)
        leitos_aux = cnes_df.groupby(["CODUFMUN"])["NUMLEITOS"].sum().reset_index().rename({"CODUFMUN": "GEOCOD6"}, axis=1)
        geodata_df = geodata_df.merge(leitos_aux, on="GEOCOD6", how="left")
        geodata_df["NUMLEITOS"] = geodata_df["NUMLEITOS"].apply(lambda x: 1 if pd.isna(x) or x==0 else x)

        mun_to_numleitos = dict(zip(geodata_df["GEOCOD6"], geodata_df["NUMLEITOS"]))
        for u in self.graph.nodes():
            self.graph.nodes[u]['numleitos'] = mun_to_numleitos[self.graph.nodes[u]['municipio_code']]

        return self


    def process_infomap_graph(self, weight_people_col='admission_count', weight_cost_col='total_cost', 
                              people_property_name='infomap_admission_count_module_id', cost_property_name='infomap_cost_module_id'):
        ''' 
            return graph with new node metadata on infomap modules.
        '''
        # -- community algorithms
        infomap_admcount = f_infomap(self.graph, weight_col=weight_people_col)
        #infomap_perhospbed = f_infomap(self.graph, weight_col='outflow_per_hospbed')
        infomap_cost = f_infomap(self.graph, weight_col=weight_cost_col)

        for u in self.graph.nodes():
            self.graph.nodes[u][people_property_name] = infomap_admcount[int(u)]
            #self.graph.nodes[u]['infomap_count_per_leito_module_id'] = infomap_perhospbed[int(u)]
            self.graph.nodes[u][cost_property_name] = infomap_cost[int(u)]
        return self
    
    def process_louvain_graph(self, weight_people_col='admission_count', weight_cost_col='total_cost'):
        ''' 
            return graph with new node metadata on louvain communities.
        '''
        # -- create new weight based on the number of hospital beds

        # -- community algorithms
        louvain_modules_count = nx.community.louvain_communities(self.graph, weight=weight_people_col)
        #louvain_modules_hospbed = nx.community.louvain_communities(self.graph, weight='outflow_per_hospbed')
        louvain_modules_cost = nx.community.louvain_communities(self.graph, weight=weight_cost_col)

        for module_index, nodes in enumerate(louvain_modules_count): 
            for node in list(nodes): self.graph.nodes[node]['louvain_count_module_id'] = module_index+1
        #for module_index, nodes in enumerate(louvain_modules_hospbed): 
        #    for node in list(nodes): self.graph.nodes[node]['louvain_count_per_leito_module_id'] = module_index+1
        for module_index, nodes in enumerate(louvain_modules_cost): 
            for node in list(nodes): self.graph.nodes[node]['louvain_cost_module_id'] = module_index+1
        return self.graph
    
    def process_sbm_graph(self):
        '''
            It requires graph-tool
        '''
        pass