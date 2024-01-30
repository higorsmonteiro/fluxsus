import networkx as nx

def calculate_incoming_flow(graph, weight_people_col=None, weight_cost_col=None, 
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
    if not nx.is_directed(graph):
        raise Exception('graph parsed is not directed.')
    
    # -- aggregate incoming information
    for u in graph.nodes():
        in_edges = [ e for e in graph.in_edges(u) ]
        graph.nodes[u][people_property_name] = 0
        graph.nodes[u][cost_property_name] = 0
        for e in in_edges:
            graph.nodes[u][people_property_name] += graph.edges[e][weight_people_col]
            graph.nodes[u][cost_property_name] += graph.edges[e][weight_cost_col]

    # -- create new weight based on the number of hospital beds
    #for u, v in graph.edges():
    #    numleitos = graph.nodes[u]['numleitos']
    #    graph.edges[(u,v)]['outflow_per_hospbed'] = graph.edges[(u,v)]['admission_count']/numleitos
    return graph

def calculate_out_flow(graph, weight_people_col=None, weight_cost_col=None, 
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
    if not nx.is_directed(graph):
        raise Exception('graph parsed is not directed.')
    
    # -- aggregate outgoing information
    for u in graph.nodes():
        out_edges = [ e for e in graph.out_edges(u) ]
        graph.nodes[u][people_property_name] = 0
        graph.nodes[u][cost_property_name] = 0
        for e in out_edges:
            graph.nodes[u][people_property_name] += graph.edges[e][weight_people_col]
            graph.nodes[u][cost_property_name] += graph.edges[e][weight_cost_col]
    
    return graph