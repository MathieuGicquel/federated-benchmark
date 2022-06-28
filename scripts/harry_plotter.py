import pandas as pd
import seaborn as sns
import glob
import os
from matplotlib import pyplot as plt

#
BASE_PLOT_PATH = "./plot/"

#
files = glob.glob(f'./result/*.csv')
dfs = []
for file in files:
    df = pd.read_csv(file)
    site = os.path.basename(file).split('_')[1].split('.')[0]
    df["site"] = site
    type = os.path.basename(file).split('_')[2].split('.')[0]
    if type == "rdf4j":
        extended_type = os.path.basename(file).split('_')[3].split('.')[0]
        type += "_" + extended_type

    df["type"] = type
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
    with open(rd) as rd_file:
        rd_f = rd_file.readlines()
        nb_cst = dict()
        nb_obj = dict()
        nb_sameas = dict()
        site_list = []
        for tp in rd_f:
            predicate = tp.split(' ')[1]
            origin_site = tp.split(' ')[3]
            origin_site = origin_site.split('/')[3]
            origin_site = origin_site.replace('>','')
            site_list.append(origin_site)
            if "sameAs" in predicate:
                nb_sameas[str(origin_site)] = nb_sameas.get(str(origin_site),0) + 1
            else:
                for i in list(range(0,50+1)):
                    p_t = "p" + str(i) + ">"
                    if p_t in predicate:
                        nb_cst[str(origin_site)] = nb_cst.get(str(origin_site),0) + 1
                for i in list(range(51,81+1)):
                    p_t = "p" + str(i) + ">"
                    if p_t in predicate:
                        nb_obj[str(origin_site)] = nb_obj.get(str(origin_site),0) + 1
        print("nb_cst", len(nb_cst))
        print("nb_obj",len(nb_obj))
        print("nb_sameas",len(nb_sameas))

        list_cst = []
        list_obj = []
        list_sameas = []
        list_site = []

        for key in sorted(nb_cst):
            list_cst.append(nb_cst[key])
            list_site.append(key)

        for key in sorted(nb_obj):
            list_obj.append(nb_obj[key])

        for key in sorted(nb_sameas):
            list_sameas.append(nb_sameas[key])

        df_f = pd.DataFrame({'Nb of constant (p0 to p50)':list_cst, 'Nb of object (p51 to p81)':list_obj, 'Nb of sameAs':list_sameas},index=list_site)
        fig = df_f.plot(kind='bar', stacked=True, color=['red', 'skyblue', 'green']).figure
        nb_sites = rd.split('/')[2]
        nb_sites = int(nb_sites.split('-')[1])
        fig.set_size_inches(min(100,nb_sites),min(100,0.5*nb_sites))
        fig.savefig(BASE_PLOT_PATH + "data_repartition_" + rd.split('/')[2] + ".png")
        plt.clf()