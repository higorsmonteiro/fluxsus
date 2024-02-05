import pandas as pd
from collections import defaultdict
from infomap import Infomap

from fluxsus.DBFIX import DBFIX

def f_infomap(graph, weight_col="admission_count"):
    '''
    
    '''
    # -- command line flags can be added as a string to Infomap
    im = Infomap(directed=True)

    # -- add nodes and links
    for v in range(graph.number_of_nodes()):
        im.add_node(v)

    for u, v in graph.edges():
        im.add_link(int(u),int(v), graph.edges[u,v][weight_col])

    im.run()
    print(f"found {im.num_top_modules} modules with codelength: {im.codelength}")

    # -- hash: node -> module
    node_module = {}
    for node in im.tree:
        node_module.update({node.node_id: node.module_id})
    return node_module