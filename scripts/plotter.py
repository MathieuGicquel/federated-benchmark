import pandas as pd
import seaborn as sns
import glob
import os

files = glob.glob(f'../result/*.csv')

dfs = []

for file in files:
    df = pd.read_csv(file)
    df["site"] = os.path.basename(file).split('_')[1].split('.')[0]
    dfs.append(df)

result = pd.concat(dfs)
result["exec_time"] = result["exec_time"].replace("failed", "0")
result["exec_time"] = pd.to_numeric(result["exec_time"])
result["site"] = pd.to_numeric(result["site"])
result = result.groupby(by=["site"]).sum()
sns.lineplot(data=result, x="site", y="exec_time", marker="o").figure.savefig("../result/total_exec_time.png")