import pandas as pd
import seaborn as sns
import glob
import os
from matplotlib import pyplot as plt

#
BASE_PLOT_PATH = "../plot/"

#
files = glob.glob(f'../result/*.csv')
dfs = []
for file in files:
    df = pd.read_csv(file)
    df["site"] = os.path.basename(file).split('_')[1].split('.')[0]
    df["type"] = os.path.basename(file).split('_')[2].split('.')[0]
    dfs.append(df)

result = pd.concat(dfs)
result = result.reset_index().drop(["index"], axis=1)

result = result.replace("failed", None)
result["exec_time"] = pd.to_numeric(result["exec_time"])
result["query"] = result["query"].apply(lambda q: os.path.basename(q))
queries: pd.Series = result["query"].unique()
result = result[result['exec_time'].notna()]
valid_queries: pd.Series = result["query"].unique()
result["site"] = pd.to_numeric(result["site"])
result["nb_http_request"] = pd.to_numeric(result["nb_http_request"])



# prepare folders
def compute_query_folder(query: str) -> str:
    res = BASE_PLOT_PATH + query.split(".")[0] + "/"
    print(res)
    return res


for query in queries:
    os.makedirs(compute_query_folder(query),exist_ok=True)

#
sns.lineplot(data=result.groupby(by=["site"]).sum(), x="site", y="exec_time", marker="o").figure.savefig(
    BASE_PLOT_PATH + "total_exec_time.png")
plt.clf()


#
for query in queries:
    print(query)
    current_df = result[(result["query"] == query) & (result["type"] == "rdf4j")]
    print(current_df)
    sns.lineplot(data=current_df, x="site", y="exec_time",hue='run_id', marker="o").figure.savefig(
        compute_query_folder(query) + "exectimeperrun.png")
    plt.clf()

# Execution time for each query
for query in queries:
    current_df = result[result["query"] == query]
    sns.lineplot(data=current_df.groupby(by=["site", "type"]).mean(), x="site", y="exec_time", marker="o",
                 hue="type").figure.savefig(compute_query_folder(query) + "exectime.png")
    plt.clf()

# Number of HTTP request for each query
for query in queries:
    current_df = result[(result["query"] == query) & (result["type"] == "rdf4j")]
    sns.lineplot(data=current_df.groupby(by=["site"]).mean(), x="site", y="nb_http_request", marker="o").figure.savefig(
        compute_query_folder(query) + "httpreq.png")
    plt.clf()
