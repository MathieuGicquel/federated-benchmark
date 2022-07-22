# Import part

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

# Goal : create plot to understand the meaning of result we have 

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

# Folder where plot are save

BASE_PLOT_PATH = "./plot/"

# Get all result csv files

files = glob.glob(f'./result/*.csv')
dfs = []
for file in files:
    df = pd.read_csv(file)
    site = os.path.basename(file).split('_')[1].split('.')[0]
    df["site"] = site
    type_ = os.path.basename(file).split('_')[2].split('.')[0]
    if type_ == "rdf4j":
        extended_type = os.path.basename(file).split('_')[3].split('.')[0]
        type_ += "_" + extended_type

    df["type"] = type_
    dfs.append(df)

result = pd.concat(dfs)
result = result.reset_index().drop(["index"], axis=1)

logger.debug(str(result))

# Don't take failed query to avoid huge difference in plot

tmp = result.loc[result['exec_time'] == "failed"]['query']
tmp = tmp.tolist()
tmp = [os.path.basename(file) for file in tmp]
logger.debug(tmp)
selection = ""
logger.debug(selection)
i = 1
for elem in tmp:
    if i != len(tmp):
        selection += str(elem) + "|"
    else:
        selection += str(elem)
    i += 1
logger.debug(selection)

if len(tmp) > 0:
    result = result[~result['query'].str.contains(selection)]

# Fix data inside pandas DataFrame

result = result.replace("failed", None)
result["exec_time"] = pd.to_numeric(result["exec_time"])
result["query"] = result["query"].apply(lambda q: os.path.basename(q))
queries: pd.Series = result["query"].unique()
result = result[result['exec_time'].notna()]
valid_queries: pd.Series = result["query"].unique()
result["site"] = pd.to_numeric(result["site"])
result["nb_http_request"] = pd.to_numeric(result["nb_http_request"])
logger.debug(result)

# Prepare folders

def compute_query_folder(query: str) -> str:
    res = BASE_PLOT_PATH + query.split(".")[0] + "/"
    logger.debug(res)
    return res


for query in queries:
    os.makedirs(compute_query_folder(query), exist_ok=True)
    logger.debug(query)

# Plot function part

# Execution time for all queries

def plot_execution_time_all_queries():
    fig = sns.lineplot(data=result.groupby(by=["site", "type","run_id"]).sum(), x="site", y="exec_time", marker="o",
                 hue="type").figure
    plt.title("Execution time for all queries (ms)")
    plt.xlabel('Number of shops')
    plt.ylabel('Total execution time (ms)')
    plt.legend(['FedX default','FedX force','Virtuoso'])
    fig.savefig(
        BASE_PLOT_PATH + "total_exec_time.png")
    plt.clf()


# Number of source selection for all queries

def plot_number_source_selection_all_queries():
    fig = sns.lineplot(data=result.groupby(by=["site", "type","run_id"]).sum(), x="site", y="total_ss", marker="o",
                 hue="type").figure
    plt.title("Number of source selection for all queries")
    plt.xlabel('Number of shops')
    plt.ylabel('Number of source selection')
    plt.legend(['FedX default','FedX force','Virtuoso'])
    fig.savefig(
        BASE_PLOT_PATH + "total_ss.png")
    plt.clf()


# Number of HTTP request for all queries

def plot_number_http_request_all_queries():
    fig = sns.lineplot(data=result.groupby(by=["site", "type","run_id"]).sum(), x="site", y="nb_http_request",
                 marker="o", hue="type").figure
    plt.title("Number of HTTP request for all queries")
    plt.xlabel('Number of shops')
    plt.ylabel('Number of HTTP request')
    plt.legend(['FedX default','FedX force','Virtuoso'])
    fig.savefig(
        BASE_PLOT_PATH + "total_httpreq.png")
    plt.clf()


# Execution time for each query default ss

def plot_execution_time_default_source_selection_all_queries():
    for query in queries:
        current_df = result[(result["query"] == query) & (result["type"] == "rdf4j_default")]
        sns.lineplot(data=current_df, x="site", y="exec_time", hue='run_id', marker="o", ci=None).figure.savefig(
            compute_query_folder(query) + "exectimeperrun.png")
        plt.clf()
        logger.debug(query)


# Execution time for each query force ss

def plot_execution_time_force_source_selection_all_queries():
    for query in queries:
        current_df = result[(result["query"] == query) & (result["type"] == "rdf4j_force")]
        sns.lineplot(data=current_df, x="site", y="exec_time", hue='run_id', marker="o", ci=None).figure.savefig(
            compute_query_folder(query) + "exectimeperrunforce.png")
        plt.clf()
        logger.debug(query)


# Execution time for each query

def plot_execution_time_each_query():
    for query in queries:
        current_df = result[result["query"] == query]
        sns.lineplot(data=current_df.groupby(by=["site", "type", "run_id"]).mean(), x="site", y="exec_time", marker="o",
                     hue="type", ci=None).figure.savefig(compute_query_folder(query) + "exectime.png")
        plt.clf()
        logger.debug(query)


# Number of source selection for each query

def plot_source_selection_each_query():
    for query in queries:
        current_df = result[result["query"] == query]
        sns.lineplot(data=current_df.groupby(by=["site", "type", "run_id"]).mean(), x="site", y="total_ss", marker="o",
                     hue="type", ci=None).figure.savefig(compute_query_folder(query) + "totalss.png")
        plt.clf()
        logger.debug(query)


# Number of HTTP request for each query

def plot_http_request_each_query():
    for query in queries:
        current_df = result[(result["query"] == query) & (result["type"].str.startswith('rdf4j'))]
        sns.lineplot(data=current_df.groupby(by=["site", "type", "run_id"]).mean(), x="site", y="nb_http_request",
                     marker="o", hue="type", ci=None).figure.savefig(
            compute_query_folder(query) + "httpreq.png")
        plt.clf()
        logger.debug(query)


# Repartition of data per site

def plot_data_repartition():
    rdata = glob.glob(f'./result/**/data/data.nq')
    logger.debug(rdata)

    for rd in rdata:
        logger.debug(rd)

        df = pd.read_csv(rd, sep=" ", names=['s', 'p', 'o', 'g', 'dot'])
        df['g'] = df['g'].replace(to_replace=r'<http://example.org/(.+)>', value=r'\1', regex=True)
        df = df.sort_values(by=['g'])

        # Get number of constant's object in data

        nb_cst = df[~df['o'].str.contains('http://example.org', regex=True)].groupby(['g'])['o'].count()

        # Get number of object (no constant) in data

        nb_obj = df[(df['o'].str.contains('http://example.org', regex=True)) & (
            df['p'].str.contains('http://example.org', regex=True))].groupby(['g'])['o'].count()

        # Get number of owl:sameAs predicate in data

        nb_sameas = df[df['p'].str.contains('#sameAs', regex=True)].groupby(['g'])['p'].count()

        # Get number of rdf:type predicate in data

        nb_type = df[df['p'].str.contains('#type', regex=True)].groupby(['g'])['p'].count()

        list_site = list(df['g'].unique())

        logger.debug(list_site)

        df_f = pd.DataFrame(
            {'Nb of constant': nb_cst, 'Nb of object': nb_obj, 'Nb of sameAs': nb_sameas, 'Nb of type': nb_type},
            index=list_site)
        nb_sites = rd.split('/')[2]
        nb_sites = int(nb_sites.split('-')[1])
        fig = df_f.plot(kind='bar', stacked=True, color=['red', 'skyblue', 'green', 'gold']).legend(loc='center left',bbox_to_anchor=(1.0, 0.5),fontsize=str(min(2 * 300,nb_sites*2))).figure
        fig.set_size_inches(min(300,4 * nb_sites),min(150,2 * nb_sites))
        plt.xticks(fontsize=min(300,nb_sites),rotation=90)
        plt.yticks(fontsize=min(2 * 300,nb_sites*2))
        plt.xlabel('Shop', fontsize=min(4 * 300,nb_sites*4))
        plt.ylabel('Number of objects', fontsize=min(4 * 300,nb_sites*4))
        plt.title("Repartition of data on all shop", fontsize=min(5 * 300,nb_sites*5))
        fig.savefig(BASE_PLOT_PATH + "data_repartition_" + rd.split('/')[2] + ".png")
        plt.clf()

# Data compared

def data_comparator():
    
    rdf4j_files = glob.glob(f'./result/**/**/**/rdf4j/**/*.out')
    virtuoso_files = glob.glob(f'./result/**/**/**/virtuoso/*.out')

    logger.debug(rdf4j_files)
    logger.debug(virtuoso_files)

    all_queries = list(set([item.split('/')[4] for item in virtuoso_files]))
    all_sites = list(set([item.split('/')[2] for item in virtuoso_files]))
    all_runs = list(set([item.split('/')[3] for item in virtuoso_files]))

    logger.debug(all_queries)

    for site in all_sites:
        df_result = pd.DataFrame(columns=['query','run','type','result'])
        logger.debug(site)
        for query in all_queries:
            for run in all_runs:
                logger.debug(query)
                logger.debug(run)
                rdf4j_results = list(filter(lambda v: re.match(f'.+/{site}/{run}/.+/{query}.out', v), rdf4j_files))
                logger.debug(rdf4j_results)
                virtuoso_results = list(filter(lambda v: re.match(f'.+/{site}/{run}/.+/{query}.out', v), virtuoso_files))
                logger.debug(virtuoso_results)
                for rdf4j_result in rdf4j_results:
                    with open(rdf4j_result) as rdf4j_f:
                        rdf4j_data = rdf4j_f.readlines()
                        if 'default' in rdf4j_result:
                            logger.debug(f'query : {query} rdf4j_default : {len(rdf4j_data)}')
                            df_result.loc[len(df_result.index)] = [query,run,'rdf4j_default',len(rdf4j_data)]
                        elif 'force' in rdf4j_result:
                            logger.debug(f'query : {query} rdf4j_force : {len(rdf4j_data)}')
                            df_result.loc[len(df_result.index)] = [query,run,'rdf4j_force',len(rdf4j_data)]
                for virtuoso_result in virtuoso_results:
                    with open(virtuoso_result) as virtuoso_f:
                        virtuoso_data = virtuoso_f.readlines()
                        logger.debug(f'query : {query} virtuoso : {len(virtuoso_data)-1}')
                        df_result.loc[len(df_result.index)] = [query,run,'virtuoso',len(virtuoso_data)-1]

        fig = sns.barplot(x="query", y="result", hue="type", data=df_result).legend(loc='center left',bbox_to_anchor=(1.0, 0.5),fontsize=str(len(all_queries)*0.65)).figure
        fig.set_size_inches(len(all_queries),len(all_queries)*0.5)
        plt.xticks(fontsize=len(all_queries)*0.4,rotation=90)
        plt.yticks(fontsize=len(all_queries)*0.6)
        plt.xlabel('Queries', fontsize=len(all_queries)*0.7)
        plt.ylabel('Number of results', fontsize=len(all_queries)*0.7)
        plt.title("Number of result for each queries per method", fontsize=len(all_queries))
        fig.savefig(BASE_PLOT_PATH + "compare_result_"+ site +".png")
        logger.debug(df)
        plt.clf()


# Graph of link between site

def plot_graph_link_between_site():
    rdata = glob.glob(f'./result/*.yaml')
    for rd in rdata:

        nb_site = str(str(str(rd).split('/')[2]).split('_')[1]).split('.')[0]

        G = nx.MultiDiGraph()
        data = yaml.load(open(rd), Loader=yaml.FullLoader)
        logger.debug(rd)

        for site in data.keys():
            if site != "all_site":
                logger.debug(site)
                G.add_node(site)
                for predicate in data[site]["predicates"].keys():
                    if predicate in [
                        "#sameAs"
                    ] or True:
                        logger.debug(data[site]["predicates"][predicate][site])
                        for target_site in data[site]["predicates"][predicate].keys():
                            G.add_node(target_site)
                            in_: int = data[site]["predicates"][predicate][target_site]["in"]
                            out_: int = data[site]["predicates"][predicate][target_site]["out"]
                            logger.debug(data[site]["predicates"][predicate][target_site])
                            if in_ > 0:
                                if target_site != site:
                                    logger.info(f"Adding edge from {target_site} to {site}. ({predicate} : {in_})")
                                    if predicate in ["#sameAs"]:
                                        G.add_edge(target_site, site, title=f"{predicate} : {in_}",color='black', value=200)
                                    else:
                                        G.add_edge(target_site, site, title=f"{predicate} : {in_}")
        nt = Network('1000px', '1000px', directed=True, layout=False)
        nt.show_buttons()
        nt.from_nx(G, default_node_size=100)
        nt.barnes_hut(spring_length=5000, overlap=1)

        nt.save_graph(f'{BASE_PLOT_PATH}repartition-{nb_site}.graph.html')
        nx.write_adjlist(G, f'{BASE_PLOT_PATH}repartition-{nb_site}.graph.adjlist')


# Data ontology

def plot_graph_ontology():
    rdata = glob.glob(f'./result/**/data/data.nq')
    for rd in rdata:
        nb_sites = rd.split('/')[2]
        nb_sites = int(nb_sites.split('-')[1])

        df: pd.DataFrame = pd.read_csv(rd, sep=" ", names=['s', 'p', 'o', 'g', 'dot'])
        
        df["s_type"] = df["s"].apply(lambda s: s.split("/")[-1].split("_")[-2] if len(s.split("/")[-1].split("_")) > 1 else None)
        df["o_type"] = df["o"].apply(lambda s: s.split("/")[-1].split("_")[-2] if len(s.split("/")[-1].split("_")) > 1 else None)

        G: nx.DiGraph() = nx.DiGraph()    

        for idx, row in df.iterrows():
            logger.debug(str(row))
            if "http://example.org" in row["p"] and row["s_type"] is not None and row["o_type"] is not None:
                logger.debug(f"Adding edge {row['p']} from {row['s_type']} ({row['s']}) to {row['o_type']} ({row['o']})")
                G.add_node(row["s_type"])
                G.add_node(row["o_type"])
                G.add_edge(row["s_type"], row["o_type"], title=row["p"])

        nt = Network('1000px', '1000px', directed=True, layout=False)
        nt.show_buttons()
        nt.from_nx(G)

        nt.save_graph(f'{BASE_PLOT_PATH}ontology-{nb_sites}.graph.html')
        nx.write_adjlist(G, f'{BASE_PLOT_PATH}ontology-{nb_sites}.graph.adjlist')

# Entity graph

def plot_graph_entity():
    rdata = glob.glob(f'./result/**/data/data.nq')
    logger.debug(rdata)

    for rd in rdata:
        df = pd.read_csv(rd, sep=" ", names=['s', 'p', 'o', 'g', 'dot'])
        

        nb_sites = rd.split('/')[2]
        nb_sites = int(nb_sites.split('-')[1])
        output_path = f"{BASE_PLOT_PATH}data-{nb_sites}.html"

        nt = Network('1000px', '1000px', directed=True, layout=False)

        palette = sns.color_palette(cc.glasbey, n_colors=nb_sites+1).as_hex()
        color_dict = dict()
        for idx, row in df.iterrows():
            s = row["s"]
            o = row["o"]
            g = row["g"]
            p = row["p"]

            regex_extract_id = r"http:\/\/.*\/(\S+)\/"
            print(f"idx = {idx}, s = {s}, o = {o}")
            s_site_id = re.search(regex_extract_id, s).group(1)
            if "^^" in o or "string" in o:

                # Literal

                o_site_id = s_site_id
            else:
                o_site_id = re.search(regex_extract_id, o).group(1)

            if s_site_id not in color_dict.keys():
                color_dict[s_site_id] = palette[len(color_dict)]
            if o_site_id not in color_dict.keys():
                color_dict[o_site_id] = palette[len(color_dict)]

            nt.add_node(s, color=color_dict.get(s_site_id))
            nt.add_node(o, color=color_dict.get(o_site_id))

            if s_site_id != o_site_id:
                logger.debug(f"Found outgoing link : {s} {p} {o} {g}")
                nt.add_edge(s, o, color='black', title=p, value=200)
            else:
                nt.add_edge(s, o, color=color_dict.get(s_site_id), title=p)

        logger.debug(color_dict)
        nt.show_buttons()
        nt.barnes_hut(spring_length=5000, overlap=1)
        nt.save_graph(output_path)

# Plotting part
# You can decide which plot you want we you execute this scripts after running the whole project

# Plot for all queries

#plot_execution_time_all_queries()
#plot_number_source_selection_all_queries()
#plot_number_http_request_all_queries()
#plot_execution_time_default_source_selection_all_queries()
#plot_execution_time_force_source_selection_all_queries()

# Plot for each queries

#plot_execution_time_each_query()
#plot_source_selection_each_query()
#plot_http_request_each_query()

# Plot for data repartition

plot_data_repartition()

# Plot for result comparaison

#data_comparator()

# Plot for graph

#plot_graph_ontology()
#plot_graph_link_between_site()
#plot_graph_entity()