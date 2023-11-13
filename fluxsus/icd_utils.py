from collections import defaultdict

def filter_chapter(chapter):
    '''
    
    '''
    cid10_chapters = {
        'Neoplasmas': 'C00-D48',
        'Doenças do aparelho circulatório': 'I00-I99',
        'Transtornos mentais e comportamentais': 'F00-F99',
        'Causas externas de morbidade e de mortalidade': 'V01-Y98',
        'Doenças do aparelho respiratório': 'J00-J99',
        'Algumas doenças infecciosas e parasitárias': 'A00-B99',
    }

    cid10_chapters = defaultdict(lambda: [])

    # -- neoplasmas     
    cid10_chapters['Neoplasmas'] = [ 'C'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]
    cid10_chapters['Neoplasmas'] += [ 'D'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 49) ]
    # -- aparelho circulatório
    cid10_chapters['Doenças do aparelho circulatório'] = [ 'I'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]
    # -- transtornos mentais e comportamentais
    cid10_chapters['Transtornos mentais e comportamentais'] = [ 'F'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]
    # -- causas externas de morbidade e de mortalidade
    cid10_chapters['Causas externas de morbidade e de mortalidade'] = [ 'V'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 100) ]
    cid10_chapters['Causas externas de morbidade e de mortalidade'] += [ 'Y'+f'{n:2.0f}'.replace(' ', '0') for n in range(1, 99) ]
    # -- doenças do aparelho respiratório
    cid10_chapters['Doenças do aparelho respiratório'] = [ 'J'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]
    # -- algumas doenças infecciosas e parasitárias
    cid10_chapters['Algumas doenças infecciosas e parasitárias'] = [ 'A'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]
    cid10_chapters['Algumas doenças infecciosas e parasitárias'] += [ 'B'+f'{n:2.0f}'.replace(' ', '0') for n in range(0, 100) ]

    if chapter not in cid10_chapters.keys():
        raise Exception('Chapter not included in the list.')
    
    return cid10_chapters[chapter]
    
    