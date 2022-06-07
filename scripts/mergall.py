import glob
import csv
import click
from pathlib import Path
import pandas as pd
import re

@click.command()
@click.argument("input_path")
@click.argument("output_path")
def merge(input_path, output_path):
    #Select files
    files = Path(input_path).rglob('*.csv')
    dfs = dict()
    for file in files:
        print(str(file.absolute()))
        if re.search(r"(.*)/query-\d+\.csv",str(file.absolute())):
            type = re.search(r".*/(.*)/query-\d+\.csv",str(file.absolute())).group(1)
            df = pd.read_csv(file)
            df["run_id"] = file.parent.parent.parent.name

            if dfs.get(type) is None:
                dfs[type] = []

            dfs.get(type).append(df)

    for type in dfs.keys():
        result : pd.DataFrame = pd.concat(dfs.get(type))
        result.to_csv(output_path + "all_"+ Path(input_path).name +"_" + type + ".csv", index=False)

if __name__ == "__main__":
    merge()
