import pandas as pd
import seaborn as sns
import glob
import os
from matplotlib import pyplot as plt
import networkx as nx
from pyvis.network import Network
import yaml

#
BASE_PLOT_PATH = "./plot/"

#
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

tmp = result.loc[result['exec_time'] == "failed"]['query']
tmp = tmp.tolist()
tmp = [os.path.basename(file) for file in tmp]
selection = ""
print(selection)
i=1
for elem in tmp:
    if i != len(tmp):
        selection+=str(elem) + "|"
    else:
        selection+=str(elem)
    i+=1
result = result[~result['query'].str.contains(selection)]

result = result.replace("failed", None)
result["exec_time"] = pd.to_numeric(result["exec_time"])
result["query"] = result["query"].apply(lambda q: os.path.basename(q))
queries: pd.Series = result["query"].unique()
result = result[result['exec_time'].notna()]
valid_queries: pd.Series = result["query"].unique()
result["site"] = pd.to_numeric(result["site"])
result["nb_http_request"] = pd.to_numeric(result["nb_http_request"])
print(result)


# prepare folders
def compute_query_folder(query: str) -> str:
    res = BASE_PLOT_PATH + query.split(".")[0] + "/"
    print(res)
    return res


for query in queries:
    os.makedirs(compute_query_folder(query),exist_ok=True)
    print(query)



# Execution time for all queries
sns.lineplot(data=result.groupby(by=["site","type","query","run_id"]).sum(), x="site", y="exec_time", marker="o", hue="type",ci=None).figure.savefig(
    BASE_PLOT_PATH + "total_exec_time.png")
plt.clf()

# Number of source selection for all queries
sns.lineplot(data=result.groupby(by=["site","type","query","run_id"]).sum(), x="site", y="total_ss", marker="o", hue="type",ci=None).figure.savefig(
    BASE_PLOT_PATH + "total_ss.png")
plt.clf()

# Number of HTTP request for all queries
sns.lineplot(data=result.groupby(by=["site","type","query","run_id"]).sum(), x="site", y="nb_http_request", marker="o", hue="type",ci=None).figure.savefig(
    BASE_PLOT_PATH + "total_httpreq.png")
plt.clf()

# Execution time for each query default ss
for query in queries:
    current_df = result[(result["query"] == query) & (result["type"] == "rdf4j_default")]
    sns.lineplot(data=current_df, x="site", y="exec_time",hue='run_id', marker="o",ci=None).figure.savefig(
        compute_query_folder(query) + "exectimeperrun.png")
    plt.clf()
    print(query)

# Execution time for each query force ss
for query in queries:
    current_df = result[(result["query"] == query) & (result["type"] == "rdf4j_force")]
    sns.lineplot(data=current_df, x="site", y="exec_time",hue='run_id', marker="o",ci=None).figure.savefig(
        compute_query_folder(query) + "exectimeperrunforce.png")
    plt.clf()
    print(query)

# Execution time for each query
for query in queries:
    current_df = result[result["query"] == query]
    sns.lineplot(data=current_df.groupby(by=["site", "type","run_id"]).mean(), x="site", y="exec_time", marker="o",
                 hue="type",ci=None).figure.savefig(compute_query_folder(query) + "exectime.png")
    plt.clf()
    print(query)

# Number of source selection for each query
for query in queries:
    current_df = result[result["query"] == query]
    sns.lineplot(data=current_df.groupby(by=["site", "type", "run_id"]).mean(), x="site", y="total_ss", marker="o",
                 hue="type",ci=None).figure.savefig(compute_query_folder(query) + "totalss.png")
    plt.clf()
    print(query)

# Number of HTTP request for each query
for query in queries:
    current_df = result[(result["query"] == query) & (result["type"].str.startswith('rdf4j'))]
    sns.lineplot(data=current_df.groupby(by=["site","type","run_id"]).mean(), x="site", y="nb_http_request", marker="o", hue="type",ci=None).figure.savefig(
        compute_query_folder(query) + "httpreq.png")
    plt.clf()
    print(query)



# Repartition of data per site
rdata = glob.glob(f'./result/**/data/data.nq')
print(rdata)

for rd in rdata:
    print(rd)

    df = pd.read_csv(rd, sep=" ", names=['s', 'p', 'o', 'g', 'dot'])
    df['g'] = df['g'].replace(to_replace=r'<http://example.org/(s[0-9]+)>',value=r'\1',regex=True)
    df = df.sort_values(by=['g'])

    # repartition 
    nb_cst = df[df['o'].str.contains('string|integer|date',regex=True)].groupby(['g'])['o'].count()
    nb_obj = df[(df['o'].str.contains('http://example.org',regex=True)) & (df['p'].str.contains('http://example.org',regex=True))].groupby(['g'])['o'].count()
    nb_sameas = df[df['p'].str.contains('sameAs',regex=True)].groupby(['g'])['p'].count()

    list_site = list(df['g'].unique())

    print(list_site)

    df_f = pd.DataFrame({'Nb of constant (p0 to p50)':nb_cst, 'Nb of object (p51 to p81)':nb_obj, 'Nb of sameAs':nb_sameas},index=list_site)
    fig = df_f.plot(kind='bar', stacked=True, color=['red', 'skyblue', 'green']).figure
    nb_sites = rd.split('/')[2]
    nb_sites = int(nb_sites.split('-')[1])
    fig.set_size_inches(min(100,nb_sites),min(100,0.5*nb_sites))
    fig.savefig(BASE_PLOT_PATH + "data_repartition_" + rd.split('/')[2] + ".png")
    plt.clf()

    # graph
    G = nx.MultiDiGraph()
    
rdata = glob.glob(f'./result/*.yaml')
for rd in rdata:

    nb_site = str(str(str(rd).split('/')[2]).split('_')[1]).split('.')[0]

    #G = Network('500px', '500px', directed=True)
    G = nx.MultiDiGraph()
    data = yaml.load(open(rd), Loader=yaml.FullLoader)
    print(rd)
    #G = nx.MultiDiGraph()
    for site in data.keys():
        if site != "all_site":
            print(site)
            G.add_node(site)
            for predicate in data[site]["predicates"].keys():
                if predicate in ["psameAs","phomepage"]:
                    print(data[site]["predicates"][predicate][site])
                    for target_site in data[site]["predicates"][predicate].keys():
                        G.add_node(target_site)
                        in_ : int = data[site]["predicates"][predicate][target_site]["in"]
                        out_ : int = data[site]["predicates"][predicate][target_site]["out"]
                        print(data[site]["predicates"][predicate][target_site])
                        if in_ > 0:
                            G.add_edge(target_site, site,title=f"{predicate} : {in_}")
                        if out_ > 0:
                            G.add_edge(site,target_site,title=f"{predicate} : {out_}")

    nt = Network('500px', '500px',directed=True)
    nt.from_nx(G)
    nt.show(f'{BASE_PLOT_PATH}graph-{nb_site}.html')