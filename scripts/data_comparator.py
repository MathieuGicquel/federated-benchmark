import pandas as pd
import seaborn as sns
import glob
import os
from matplotlib import pyplot as plt
import networkx as nx
from pyvis.network import Network
import yaml
import logging
import coloredlogs
import re
import random
import colorcet as cc

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

#
BASE_PLOT_PATH = "./plot/"

def data_comparator():
    #
    rdf4j_files = glob.glob(f'./result/**/**/**/rdf4j/**/*.out')
    virtuoso_files = glob.glob(f'./result/**/**/**/virtuoso/*.out')

    #logger.debug(rdf4j_files)
    #logger.debug(virtuoso_files)

    all_queries = [item.split('/')[4] for item in virtuoso_files]
    all_sites = [item.split('/')[2] for item in virtuoso_files]

    #logger.debug(all_queries)

    rdf4j_list_default_result = []
    rdf4j_list_force_result = []
    virtuoso_list_result = []

    for site in all_sites:
        #logger.debug(site)
        for query in all_queries:
            #logger.debug(query)
            rdf4j_results = list(filter(lambda v: re.match(f'.+/{site}/.+/{query}.out', v), rdf4j_files))
            #logger.debug(rdf4j_results)
            virtuoso_results = list(filter(lambda v: re.match(f'.+/{site}/.+/{query}.out', v), virtuoso_files))
            #logger.debug(virtuoso_results)
            for rdf4j_result in rdf4j_results:
                with open(rdf4j_result) as rdf4j_f:
                    rdf4j_data = rdf4j_f.readlines()
                    if 'default' in rdf4j_result:
                        logger.debug(f'query : {query} rdf4j_default : {len(rdf4j_data)}')
                        rdf4j_list_default_result.append(len(rdf4j_data))
                    elif 'force' in rdf4j_result:
                        logger.debug(f'query : {query} rdf4j_force : {len(rdf4j_data)}')
                        rdf4j_list_force_result.append(len(rdf4j_data))
            for virtuoso_result in virtuoso_results:
                with open(virtuoso_result) as virtuoso_f:
                    virtuoso_data = virtuoso_f.readlines()
                    logger.debug(f'query : {query} virtuoso : {len(virtuoso_data)-1}')
                    virtuoso_list_result.append(len(virtuoso_data)-1)

        df = pd.DataFrame({'rdf4j_default': rdf4j_list_default_result,'rdf4j_force': rdf4j_list_force_result,'virtuoso': virtuoso_list_result}, index=all_queries)
        fig = df.plot.bar(rot=90).legend(loc='center left',bbox_to_anchor=(1.0, 0.5)).figure
        fig.set_size_inches(15,10)
        fig.savefig(BASE_PLOT_PATH + "compare_result_"+ site +".png")
        rdf4j_list_default_result = []
        rdf4j_list_force_result = []
        virtuoso_list_result = []

if __name__ == "__main__":
    data_comparator()