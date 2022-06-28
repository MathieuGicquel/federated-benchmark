
# Examples of use :
#   python3 ./scripts/constantin_first.py /GDD/federated-benchmark/repo/prepa/filter_queries --output /GDD/federated-benchmark/repo/result/site-5/queries
import click
import logging
import coloredlogs
import urllib
import urllib.parse
import urllib.request
from time import time
from pathlib import Path
from urllib.error import HTTPError
import os
from glob import glob
import random
import re
import pandas as pd
import io
import sys

coloredlogs.install(level='DEBUG', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# text/csv
# text/tsv
# application/json
# voir http://vos.openlinksw.com/owiki/wiki/VOS/VOSSparqlProtocol
def sparqlQuery(query, baseURL, format="text/csv",default_graph_uri=""):
    params={
        "default-graph-uri": default_graph_uri,
        "should-sponge": "soft",
        "query": query,
        "debug": "on",
        "timeout": "",
        "format": format,
        "save": "display",
        "fname": ""
        }
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(baseURL)
    response=None
    exception=None
    df=None
    try:
        with urllib.request.urlopen(req,data=data) as f:
            response = f.read()
            df = pd.read_csv(io.StringIO(response.decode('utf-8')),quotechar="'",)
            columns = df.columns
            new_columns = [s.replace('"','') for s in columns]
            df.set_axis(new_columns, axis=1,inplace=True)

            for c in new_columns:
                df[c] = df[c].apply(lambda s: (s.replace('"','') if str(s).startswith('"http://') else s ),)


            # for each column
            #   create a new column containing the last number : \d+(?!.*\d)
            regex_columns = []
            for c in df.columns:
                new_c = "regex_" + c
                df[new_c] = df[c].apply(lambda s: int(re.search(r"(\d+)(?!.*\d)",str(s)).group(1)))
                regex_columns.append(new_c)

            df = df.sort_values(regex_columns)
            df = df.drop(regex_columns,axis=1)
    except HTTPError as e:
        exception = e.read()
    return(response,exception,df)



@click.command()
@click.argument("queries")
@click.option("--format", type=click.Choice(["text/csv"]), default="text/csv",
    help="Format of the results set, see http://vos.openlinksw.com/owiki/wiki/VOS/VOSSparqlProtocol")
@click.option("--output", type=str, default=None,
    help="The folder in which the query result will be stored.")
@click.option("--entrypoint", type=str, default="http://localhost:8890/sparql/",
    help="URL of the Virtuoso SPARQL endpoint")
@click.option("--seed", type=int, default=5,
    help="Seed for random function")
@click.option("--variation", type=int, default=1,
    help="Number of variation for each query")
def virtuoso(queries,format,output,entrypoint,seed,variation):
    output = os.path.abspath(output)
    random.seed(int(seed))
    variations = range(1,variation + 1)
    queries_files = sorted(glob(f"{queries}/*.noask.sparql"))
    for query_file in queries_files:
        for variation_id in variations:
            seed = random.randint(0, 1000000) 
            logger.debug(f"New seed : {seed}")
            random.seed(int(seed))
            with open(query_file) as query:
                query_absolute_path = os.path.abspath(query_file)
                query_base_name = os.path.basename(query_file).split('.')[0]

                output_query_path = f'{output}/{query_base_name}-{variation_id}.noask.cst.sparql'
                output_query_ss_path = f'{output}/{query_base_name}-{variation_id}.ss.cst.sparql'

                logger.debug(f"Query : {query_absolute_path}")
                query = query.read()
                replacement_df = get_replacement_df(query,entrypoint, format)
                new_query = add_cst_to_query(query,replacement_df,seed)
                new_query_ss = get_ss_query(new_query)

                new_query = f"# Variation {variation_id} from {query_absolute_path} - seed {seed}\n\n" + new_query
                new_query_ss = f"# Source Selection Query of {output}/{query_base_name}-{variation_id}.noask.cst.sparql\n\n" + new_query_ss

                logger.debug(f"Old query :\n {query}")
                logger.debug(f"New query :\n {new_query}")
                logger.debug(f"New Source Selection query :\n {new_query_ss}")

                with open(output_query_path, 'w') as ffile:
                    ffile.write(new_query)


                with open(output_query_ss_path, 'w') as ffile:
                    ffile.write(new_query_ss)


def get_replacement_df(query: str,entrypoint: str, format: str):
    query_virtuoso = re.sub(r'SELECT (.+) WHERE', r'SELECT * WHERE', query)
    result, exception, df=sparqlQuery(query_virtuoso, entrypoint, format)

    if exception is not None:
            df = pd.DataFrame()
    return df


def add_cst_to_query(query: str, df: pd.DataFrame,seed) -> str:
    triples = get_triples_without_cst(query)
    var_0 = [tp[0] for tp in triples]
    var_1 = [tp[2] for tp in triples]
    var = set(var_1) - set(var_0)
    var = var | {'?x0'}
    var = sorted(list(var))
    logger.debug(f"var = {var}")
    x_c = random.sample(var,k=1)[0]
    logger.debug(f"x_c = {x_c}")
    x_c = x_c.replace('\'','')
    x_c = x_c.replace('?','')


    #column = str(random.sample(list(sorted(df[str(x_c)])),k=1)[0])
    column = str(df[x_c].sample(n=1,random_state=seed).iloc[0])
    print(column)

    column = re.sub(r"\[([0-9]+)\]",r'\1', column)
    column = re.sub(r'\[([0-9]+)"\]',r'"\1"', column)
    column = re.sub(r"(http://example.org/s[0-9]+/[0-9]+)",r'<\1>', column)

    prepa = query.split('WHERE {')
    prepa[0] = prepa[0].replace(f'?{x_c}', '')
    if '?x' not in prepa[0] and '*' not in prepa[0]:
        prepa[0] = prepa[0] + ' * '
    prepa[1] = prepa[1].replace(f'?{x_c}', f'{column}')
    result = prepa[0] + 'WHERE {' + prepa[1]

    return result

def get_triples_without_cst(query: str):
    triples = re.findall(r"(\?x[0-9]+) (\S+) (\?x[0-9]+) \.", query)
    return triples

def get_triples(query:str):
    triples = re.findall(r"(\?x[0-9]+|<http://example.org/s[0-9]+/[0-9]+>|\"[0-9]+\"|[0-9]+) (\S+) (\?x[0-9]+|<http://example.org/s[0-9]+/[0-9]+>|\"[0-9]+\"|[0-9]+) \.", query)
    return triples

def get_ss_query(query: str) -> str:
    triples = get_triples(query)
    prefixes = query.split("SELECT")[0]
    source_selection_query = prefixes + "\n" + "SELECT DISTINCT "
    for i in range(0,len(triples)):
        source_selection_query += "?tp" + str(i) + " "
    source_selection_query += "{"

    for i in range(0,len(triples)):
        source_selection_query += "GRAPH ?tp" + str(i) + " { " + " ".join(triples[i]) + " } . \n"
    source_selection_query += "}"     

    return source_selection_query              


if __name__ == "__main__":
    virtuoso()