# Import part

import glob
import csv
import click
from pathlib import Path
import pandas as pd
import re

# Goal : merge all statistic file into one statistic file to do some plot on it

@click.command()
@click.argument("input_path")
@click.argument("output_path")
def merge(input_path, output_path):

    #Select files

    files = Path(input_path).rglob('run-*/**/*.csv')
    dfs = dict()
    for file in files:

        # Do RDF4J with default source selection statistic files

        if re.search(r".*/rdf4j/default/query-\d+(-\d+)?\.csv",str(file.absolute())):
            print(str(file.absolute()))
            type = 'rdf4j_default'
            df = pd.read_csv(file)
            df["run_id"] = (file.parent.parent.parent.parent.name).replace("run-","")

            if re.match(r".*/query-\d+(-\d+)\.csv",str(file.absolute())):
                variation_id = re.match(r".*/rdf4j/default/query-\d+(-\d+)\.csv",str(file.absolute())).group(1).replace('-','')
                df["variation_id"] = variation_id

            if dfs.get(type) is None:
                dfs[type] = []

            dfs.get(type).append(df)

        # Do RDF4J with forced source selection statistic files

        if re.search(r".*/rdf4j/force/query-\d+(-\d+)?\.csv",str(file.absolute())):
            type = 'rdf4j_force'
            df = pd.read_csv(file)
            df["run_id"] = (file.parent.parent.parent.parent.name).replace("run-","")

            if re.match(r".*/query-\d+(-\d+)\.csv",str(file.absolute())):
                variation_id = re.match(r".*/query-\d+(-\d+)\.csv",str(file.absolute())).group(1).replace('-','')
                df["variation_id"] = variation_id

            if dfs.get(type) is None:
                dfs[type] = []

            dfs.get(type).append(df)

        # Do Virtuoso statistic files

        if re.search(r".*/virtuoso/query-\d+(-\d+)?\.csv",str(file.absolute())):
            type = "virtuoso"
            df = pd.read_csv(file)
            df["run_id"] = (file.parent.parent.parent.name).replace("run-","")

            if dfs.get(type) is None:
                dfs[type] = []

            if re.match(r".*/query-\d+(-\d+)\.csv",str(file.absolute())):
                variation_id = re.match(r".*/query-\d+(-\d+)\.csv",str(file.absolute())).group(1).replace('-','')
                df["variation_id"] = variation_id

            dfs.get(type).append(df)

    print(dfs.keys())
    for type in dfs.keys():
        result : pd.DataFrame = pd.concat(dfs.get(type))
        result.to_csv(output_path + "all_"+ (Path(input_path).name).replace("site-","") +"_" + type + ".csv", index=False)

if __name__ == "__main__":
    merge()