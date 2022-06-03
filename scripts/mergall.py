import glob
import csv
import click
from pathlib import Path
import pandas as pd
import re

@click.command()
@click.argument("input_path")
@click.argument("output_file")

def merge(input_path, output_file):
    #Select files
    files = Path(input_path).rglob('*.csv')
    dfs = []
    for file in files:
        print(str(file.absolute()))
        if re.compile(r".+[0-9].csv").match(str(file.absolute())):
            df = pd.read_csv(file)
            df["run_id"] = re.search(r'(\d+)\/query-\d+.csv',str(file.absolute())).group(1)
            dfs.append(df)

    result : pd.DataFrame = pd.concat(dfs)
    result.to_csv(output_file,index=False)

if __name__ == "__main__":
    merge()
