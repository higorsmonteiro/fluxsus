import os
import glob
from tqdm import tqdm
import pandas as pd
from collections import defaultdict
from fluxsus.DBFIX import DBFIX
from fluxsus.fluxnets.fluxnets import CityFlux, CityHospitalFlux

def create_citynet(sihpath, cnes_df, geodata_df, init_period, final_period, output):
    '''
        Create a city flux network for a given period.

        Period is defined with the month and year of competence of the SIHSUS file.

        Args:
        -----
            sihpath:
                String.
            cnes_df:
                pandas.DataFrame.
            geodata_df:
                pandas.DataFrame.
            init:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                SIHSUS file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the beginning
                of the period.
            final:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                SIHSUS file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the beginning
                of the period.
            output:
                String.
    '''
    preffix = init_period[:4]
    list_of_files = glob.glob(os.path.join(sihpath, f'{preffix}*'))

    init_index = [idx for idx, s in enumerate(list_of_files) if init_period in s][0]
    final_index = [idx for idx, s in enumerate(list_of_files) if final_period in s][0]
    if final_index is None:
        raise Exception(f"no file {final_period} was found.")
    list_of_files = list_of_files[init_index:final_index+1]

    sih_df = []
    for fname in tqdm(list_of_files):
        cur_df = pd.read_parquet(fname)
        sih_df.append( cur_df )
    sih_df = pd.concat(sih_df, axis=0)
    sih_df["COMPETEN"] = sih_df["ANO_CMPT"].astype(str)+sih_df["MES_CMPT"].astype(str)

    # -- generate network
    cityflux = CityFlux(cnes_df, geodata_df)
    cityflux.define_network().calculate_fluxes(sih_df, multilayer_icd=True).to_gml(output)

def create_cityhospitalnet(sihpath, cnes_df, geodata_df, init_period, final_period, output):
    '''
        Create a city flux network for a given period.

        Period is defined with the month and year of competence of the SIHSUS file.

        Args:
        -----
            sihpath:
                String.
            cnes_df:
                pandas.DataFrame.
            geodata_df:
                pandas.DataFrame.
            init:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                SIHSUS file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the beginning
                of the period.
            final:
                String. Format "XXUFYYMM", where 'XX' stands for the preffix of the
                SIHSUS file (as described by DATASUS), 'UF' stands for state code to
                consider, and "YYMM" stands for the year and month of the beginning
                of the period.
            output:
                String.
    '''
    preffix = init_period[:4]
    list_of_files = glob.glob(os.path.join(sihpath, f'{preffix}*'))

    init_index = [idx for idx, s in enumerate(list_of_files) if init_period in s][0]
    final_index = [idx for idx, s in enumerate(list_of_files) if final_period in s][0]
    if final_index is None:
        raise Exception(f"no file {final_period} was found.")
    list_of_files = list_of_files[init_index:final_index+1]

    sih_df = []
    for fname in tqdm(list_of_files):
        cur_df = pd.read_parquet(fname)
        sih_df.append( cur_df )
    sih_df = pd.concat(sih_df, axis=0)
    sih_df["COMPETEN"] = sih_df["ANO_CMPT"].astype(str)+sih_df["MES_CMPT"].astype(str)

    # -- generate network
    cityhospitalflux = CityHospitalFlux(cnes_df, geodata_df)
    cityhospitalflux.define_network().calculate_fluxes(sih_df, multilayer_icd=True).to_gml(output)


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


# -- keep track of proposal
def macro_proposal():
    return {
        "ACARAPE": 1,
        "AQUIRAZ": 1,
        "ARACOIABA": 1,
        "ARATUBA": 1,
        "BARREIRA": 1,
        "BATURITÉ": 1,
        "CAPISTRANO": 1,
        "CASCAVEL": 1,
        "CAUCAIA": 1,
        "CHOROZINHO": 1,
        "EUSÉBIO": 1,
        "FORTALEZA": 1,
        "GUAIÚBA": 1,
        "GUARAMIRANGA": 1,
        "HORIZONTE": 1,
        "ITAITINGA": 1,
        "ITAPIÚNA": 1,
        "MARACANAÚ": 1,
        "MARANGUAPE": 1,
        "MULUNGU": 1,
        "OCARA": 1,
        "PACAJUS": 1,
        "PACATUBA": 1,
        "PACOTI": 1,
        "PALMÁCIA": 1,
        "PINDORETAMA": 1,
        "REDENÇÃO": 1,
        "ACARAÚ": 2,
        "AMONTADA": 2,
        "APUIARÉS": 2,
        "CAMOCIM": 2,
        "CHAVAL": 2,
        "CRUZ": 2,
        "BARROQUINHA": 2,
        "BELA CRUZ": 2,
        "GENERAL SAMPAIO": 2,
        "GRANJA": 2,
        "IRAUÇUBA": 2,
        "ITAPAGÉ": 2,
        "ITAPIPOCA": 2,
        "ITAREMA": 2,
        "JIJOCA DE JERICOACOARA": 2,
        "MARCO": 2,
        "MARTINÓPOLE": 2,
        "MIRAÍMA": 2,
        "MORRINHOS": 2,
        "PARACURU": 2,
        "PARAIPABA": 2,
        "PENTECOSTE": 2,
        "SÃO GONÇALO DO AMARANTE": 2,
        "SÃO LUÍS DO CURU": 2,
        "TEJUÇUOCA": 2,
        "TRAIRI": 2,
        "TURURU": 2,
        "UMIRIM": 2,
        "URUBURETAMA": 2,
        "ABAIARA": 3,
        "ALTANEIRA": 3,
        "ANTONINA DO NORTE": 3,
        "ARARIPE": 3,
        "ASSARÉ": 3,
        "AURORA": 3,
        "BARBALHA": 3,
        "BARRO": 3,
        "BREJO SANTO": 3,
        "CAMPOS SALES": 3,
        "CARIRIAÇU": 3,
        "CRATO": 3,
        "FARIAS BRITO": 3,
        "GRANJEIRO": 3,
        "JARDIM": 3,
        "JATI": 3,
        "JUAZEIRO DO NORTE": 3,
        "MAURITI": 3,
        "MILAGRES": 3,
        "MISSÃO VELHA": 3,
        "NOVA OLINDA": 3,
        "PENAFORTE": 3,
        "PORTEIRAS": 3,
        "POTENGI": 3,
        "SALITRE": 3,
        "SANTANA DO CARIRI": 3,
        "TARRAFAS": 3,
        "ACOPIARA": 4,
        "BAIXIO": 4,
        "CATARINA": 4,
        "CARIÚS": 4,
        "CEDRO": 4,
        "DEPUTADO IRAPUAN PINHEIRO": 4,
        "ICÓ": 4,
        "IGUATU": 4,
        "IPAUMIRIM": 4,
        "JUCÁS": 4,
        "LAVRAS DA MANGABEIRA": 4,
        "MOMBAÇA": 4,
        "ORÓS": 4,
        "PIQUET CARNEIRO": 4,
        "QUIXELÔ": 4,
        "SABOEIRO": 4,
        "UMARI": 4,
        "VÁRZEA ALEGRE": 4,
        "BANABUIÚ": 5,
        "BOA VIAGEM": 5,
        "CANINDÉ": 5,
        "CARIDADE": 5,
        "CHORÓ": 5,
        "IBARETAMA": 5,
        "IBICUITINGA": 5,
        "ITATIRA": 5,
        "MADALENA": 5,
        "MILHÃ": 5,
        "PARAMOTI": 5,
        "PEDRA BRANCA": 5,
        "QUIXADÁ": 5,
        "QUIXERAMOBIM": 5,
        "SENADOR POMPEU": 5,
        "SOLONÓPOLE": 5,
        "ALTO SANTO": 6,
        "ARACATI": 6,
        "BEBERIBE": 6,
        "ERERÊ": 6,
        "FORTIM": 6,
        "ICAPUÍ": 6,
        "IRACEMA": 6,
        "ITAIÇABA": 6,
        "JAGUARETAMA": 6,
        "JAGUARIBARA": 6,
        "JAGUARIBE": 6,
        "JAGUARUANA": 6,
        "LIMOEIRO DO NORTE": 6,
        "MORADA NOVA": 6,
        "PALHANO": 6,
        "PEREIRO": 6,
        "POTIRETAMA": 6,
        "QUIXERÉ": 6,
        "RUSSAS": 6,
        "SÃO JOÃO DO JAGUARIBE": 6,
        "TABULEIRO DO NORTE": 6,
        "AIUABA": 7,
        "ARARENDÁ": 7,
        "ARNEIROZ": 7,
        "CATUNDA": 7,
        "CRATEÚS": 7,
        "HIDROLÂNDIA": 7,
        "INDEPENDÊNCIA": 7,
        "IPAPORANGA": 7,
        "IPUEIRAS": 7,
        "MONSENHOR TABOSA": 7,
        "NOVA RUSSAS": 7,
        "NOVO ORIENTE": 7,
        "PARAMBU": 7,
        "PORANGA": 7,
        "QUITERIANÓPOLIS": 7,
        "SANTA QUITÉRIA": 7,
        "TAMBORIL": 7,
        "TAUÁ": 7,
        "ALCÂNTARAS": 8,
        "CARIRÉ": 8,
        "CARNAUBAL": 8,
        "COREAÚ": 8,
        "CROATÁ": 8,
        "FORQUILHA": 8,
        "FRECHEIRINHA": 8,
        "GRAÇA": 8,
        "GROAÍRAS": 8,
        "GUARACIABA DO NORTE": 8,
        "IBIAPINA": 8,
        "IPU": 8,
        "MASSAPÊ": 8,
        "MERUOCA": 8,
        "MORAÚJO": 8,
        "MUCAMBO": 8,
        "PACUJÁ": 8,
        "PIRES FERREIRA": 8,
        "RERIUTABA": 8,
        "SANTANA DO ACARAÚ": 8,
        "SÃO BENEDITO": 8,
        "SENADOR SÁ": 8,
        "SOBRAL": 8,
        "TIANGUÁ": 8,
        "UBAJARA": 8,
        "URUOCA": 8,
        "VARJOTA": 8,
        "VIÇOSA DO CEARÁ": 8,
    }


