import pandas as pd
from collections import defaultdict

from infomap import Infomap


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

def filter_chapter(chapter: str):
    '''
        Gives a list of ICD-10 codes (up to the 3rd character) corresponding to 
        the parsed ICD-10 chapter. 

        Args:
        -----
            chapter:
                String. Roman number referring to a ICD-10 chapter.

        Return:
        -------
            List. 

    '''
    cid10_chapters = defaultdict(lambda: [])

    # -- certain infectious and parasitic diseases
    cid10_chapters['I'] = [ 'A'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    cid10_chapters['I'] += [ 'B'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- neoplasms     
    cid10_chapters['II'] = [ 'C'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    cid10_chapters['II'] += [ 'D'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 48+1) ]
    # -- diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism
    cid10_chapters['III'] = [ 'D'+f'{n:2.0f}'.replace(' ', '0') for n in range(50, 89+1) ]
    # -- endocrine, nutritional and metabolic diseases
    cid10_chapters['IV'] = [ 'E'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 90+1) ]
    # -- mental and behavioural disorders
    cid10_chapters['V'] = [ 'F'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the nervous system
    cid10_chapters['VI'] = [ 'G'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the eye and adnexa
    cid10_chapters['VII'] = [ 'H'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 59+1) ]
    # -- diseases of the ear and mastoid process
    cid10_chapters['VIII'] = [ 'H'+f'{n:2.0f}'.replace(' ', '0') for n in range(60, 95+1) ]
    # -- diseases of the circulatory system
    cid10_chapters['IX'] = [ 'I'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the respiratory system
    cid10_chapters['X'] = [ 'J'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the digestive system
    cid10_chapters['XI'] = [ 'K'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 93+1) ]
    # -- diseases of the skin and subcutaneous tissue
    cid10_chapters['XII'] = [ 'L'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the musculoskeletal system and connective tissue
    cid10_chapters['XIII'] = [ 'M'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- diseases of the genitourinary system
    cid10_chapters['XIV'] = [ 'N'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- pregnancy, childbirth and the puerperium
    cid10_chapters['XV'] = [ 'O'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- certain conditions originating in the perinatal period
    cid10_chapters['XVI'] = [ 'P'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 96+1) ]
    # -- congenital malformations, deformations and chromosomal abnormalities
    cid10_chapters['XVII'] = [ 'Q'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified
    cid10_chapters['XVIII'] = [ 'R'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- injury, poisoning and certain other consequences of external causes
    cid10_chapters['XIX'] = [ 'S'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    cid10_chapters['XIX'] += [ 'T'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 98+1) ]
    # -- external causes of morbidity and mortality
    cid10_chapters['XX'] = [ 'V'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 99+1) ]
    cid10_chapters['XX'] += [ 'Y'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 98+1) ]
    # -- factors influencing health status and contact with health services
    cid10_chapters['XXI'] = [ 'Z'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    # -- factors influencing health status and contact with health services
    cid10_chapters['XXII'] = [ 'U'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 99+1) ]
    

    if chapter not in cid10_chapters.keys():
        raise Exception('Chapter not included in the list.')
    
    return cid10_chapters[chapter]