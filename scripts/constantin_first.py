# Import part

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

# Examples of use :
# python3 ./scripts/constantin_first.py /GDD/federated-benchmark/repo/prepa/filter_queries --output /GDD/federated-benchmark/repo/result/site-5/queries

# Goal : Add constant in gMark/WatDiv queries from their own result

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

def sparqlQuery(query, baseURL, format="text/csv",default_graph_uri=""):

    # Set HTTP request's parameters

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

    # Create HTTP request

    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(baseURL)

    response=None
    exception=None
    df=None

    try:

        with urllib.request.urlopen(req,data=data) as f:

            # Read HTTP request's result and convert it as pandas DataFrame

            response = f.read()
            df = pd.read_csv(io.StringIO(response.decode('utf-8')),quotechar="'",)
            
            columns = df.columns
            new_columns = [s.replace('"','') for s in columns]
            df.set_axis(new_columns, axis=1,inplace=True)

            for c in new_columns:
                df[c] = df[c].apply(lambda s: (s.replace('"','') if str(s).startswith('"http://') else s ),)


            # For each column
            # Create a new column containing the last number : \d+(?!.*\d)

            regex_columns = []

            def get_entity_id(s: str) -> str:
                groups = re.search(r"(\d+)(?!.*\d)",str(s))
                if groups is not None:
                    return groups.group(1)
                else:
                    return s


            for c in df.columns:
                new_c = "regex_" + c
                
                df[new_c] = df[c].apply(lambda s: get_entity_id(s))
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

    # Set seed to control random function

    random.seed(int(seed))

    # Take a number of variation for a single query (so with different constant)

    variations = range(1,variation + 1)

    # Select all queries files

    queries_files = sorted(glob(f"{queries}/*.noask.sparql"))
    for query_file in queries_files:
        for variation_id in variations:
            seed = random.randint(0, 1000000) 
            logger.debug(f"New seed : {seed}")
            random.seed(int(seed))
            with open(query_file) as query:

                # Get different informations about the query

                query_absolute_path = os.path.abspath(query_file)
                query_base_name = os.path.basename(query_file).split('.')[0]

                # Construct link for futur generated query

                output_query_path = f'{output}/{query_base_name}-{variation_id}.noask.cst.sparql'
                output_query_ss_path = f'{output}/{query_base_name}-{variation_id}.ss.cst.sparql'

                logger.debug(f"Query : {query_absolute_path}")

                # Read and launch query to have all his result

                query = query.read()
                replacement_df = get_replacement_df(query,entrypoint, format)

                # Add constant to query from query's result

                if "template" in query_file:
                    new_query = add_fixed_cst_to_query(query,replacement_df,seed)
                else:
                    new_query = add_random_cst_to_query(query,replacement_df,seed)

                # Get source selection query from the query with constant

                new_query_ss = get_ss_query(new_query)

                # Add some informations about the query

                new_query = f"# Variation {variation_id} from {query_absolute_path} - seed {seed}\n\n" + new_query
                new_query_ss = f"# Source Selection Query of {output}/{query_base_name}-{variation_id}.noask.cst.sparql\n\n" + new_query_ss

                logger.debug(f"Old query :\n {query}")
                logger.debug(f"New query :\n {new_query}")
                logger.debug(f"New Source Selection query :\n {new_query_ss}")

                # Write the newly generated query

                with open(output_query_path, 'w') as ffile:
                    ffile.write(new_query)

                with open(output_query_ss_path, 'w') as ffile:
                    ffile.write(new_query_ss)

# Goal : Replace SELECT ?x0 ... ?xn WHERE { ... } by SELECT * WHERE { ... } and execute it

def get_replacement_df(query: str,entrypoint: str, format: str):
    query_virtuoso = re.sub(r'SELECT (.+) WHERE', r'SELECT * WHERE', query)
    result, exception, df=sparqlQuery(query_virtuoso, entrypoint, format)

    if exception is not None:
            df = pd.DataFrame()
    return df

# Goal : Add constant to the query from his result

def add_fixed_cst_to_query(query:str, df: pd.DataFrame, seed) -> str:

    # We already decide what variable to replace by constant because of following WatDiv queries template (here ?c[0-9]+)

    for m in re.finditer(r"(\?c[0-9]+)", str(query)):
        var = str(m.group(1))
        x_c = var.replace("?","")
        logger.debug(var)
        logger.debug(df[x_c])

        if len(df[x_c]) > 0:
            column = str(df[x_c].sample(n=1,random_state=seed).iloc[0])

            # Correct result :
            # [string_[0-9]+] -> "string_[0-9]+"
            # http://example.org/s[0-9]/Retailer_[0-9]+ -> <http://example.org/s[0-9]/Retailer_[0-9]+>
            # http://example.org/federated_shop/Topic_[0-9]+ -> <http://example.org/federated_shop/Topic_[0-9]+>

            column = re.sub(r"\[([A-Za-z]+_[0-9]+)\]",r'\1', column)
            column = re.sub(r'\[([A-Za-z]+_[0-9]+)"\]',r'"\1"', column)
            column = re.sub(r"(http://example.org/(s[0-9]+|federated_shop)/[A-Za-z]+_[0-9]+)",r'<\1>', column)

            # Correct SELECT :
            # SELECT ?x[0-9]+ WHERE { ... } -> SELECT * WHERE { ... } where ?x[0-9] are transformed into a constant
            # SELECT ?x[0-9]+ ?x[10-19] WHERE { ... } -> SELECT ?x[10-19]+ WHERE { ... } where ?x[0-9] are transformed into a constant

            prepa = query.split('WHERE {')
            prepa[0] = prepa[0].replace(f'?{x_c}', '')
            if '?x' not in prepa[0] and '*' not in prepa[0]:
                prepa[0] = prepa[0] + ' * '
            prepa[1] = prepa[1].replace(f'?{x_c}', f'{column}')
            result = prepa[0] + 'WHERE {' + prepa[1]
            query = result
        else:
            logger.debug(f"Empty df[{x_c}]")

    return query

# Goal : Add constant to the query from his result

def add_random_cst_to_query(query: str, df: pd.DataFrame,seed) -> str:

    # Choose which variable become a constant

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

    logger.debug(f"df = {df}")
    logger.debug(f"df[x_c] = {df[x_c]}")
    column = str(df[x_c].sample(n=1,random_state=seed).iloc[0])
    logger.debug(str(column))

    # Correct result :
    # [string_[0-9]+] -> "string_[0-9]+"
    # http://example.org/s[0-9]/Retailer_[0-9]+ -> <http://example.org/s[0-9]/Retailer_[0-9]+>
    # http://example.org/federated_shop/Topic_[0-9]+ -> <http://example.org/federated_shop/Topic_[0-9]+>

    column = re.sub(r"\[([A-Za-z]+_[0-9]+)\]",r'\1', column)
    column = re.sub(r'\[([A-Za-z]+_[0-9]+)"\]',r'"\1"', column)
    column = re.sub(r"(http://example.org/(s[0-9]+|federated_shop)/[A-Za-z]+_[0-9]+)",r'<\1>', column)

    # Correct SELECT :
    # SELECT ?x[0-9]+ WHERE { ... } -> SELECT * WHERE { ... } where ?x[0-9] are transformed into a constant
    # SELECT ?x[0-9]+ ?x[10-19] WHERE { ... } -> SELECT ?x[10-19]+ WHERE { ... } where ?x[0-9] are transformed into a constant

    prepa = query.split('WHERE {')
    prepa[0] = prepa[0].replace(f'?{x_c}', '')
    if '?x' not in prepa[0] and '*' not in prepa[0]:
        prepa[0] = prepa[0] + ' * '
    prepa[1] = prepa[1].replace(f'?{x_c}', f'{column}')
    result = prepa[0] + 'WHERE {' + prepa[1]

    return result

# Goal : Get all triples with no constant in it

def get_triples_without_cst(query: str):
    triples = re.findall(r"(\?x[0-9]+) (\S+) (\?x[0-9]+) \.", query)
    return triples

# Goal : Get all triples

def get_triples(query:str):
    triples = re.findall(r"(\?x[0-9]+|<http://example.org/s[0-9]+/[A-Za-z]+_[0-9]+>|<http://example.org/federated_shop/[A-Za-z]+_[0-9]+>|<http://example.org/s[0-9]+/[A-Za-z]+>|<http://example.org/federated_shop/[A-Za-z]+>) (\S+) (\?x[0-9]+|<http://example.org/s[0-9]+/[A-Za-z]+_[0-9]+>|<http://example.org/federated_shop/[A-Za-z]+_[0-9]+>|<http://example.org/s[0-9]+/[A-Za-z]+>|<http://example.org/federated_shop/[A-Za-z]+>|\"string_[0-9]+\"|[0-9]+) \.", query)
    return triples

# Goal : Get the corresponding source selection query from a given query

def get_ss_query(query: str) -> str:

    # Get all triples of the query

    triples = get_triples(query)

    # SELECT only named graph of each triples

    prefixes = query.split("SELECT")[0]
    source_selection_query = prefixes + "\n" + "SELECT DISTINCT "
    for i in range(0,len(triples)):
        source_selection_query += "?tp" + str(i) + " "
    source_selection_query += "{"

    # Add a GRAPH clause for each triples to get their named graph

    for i in range(0,len(triples)):
        source_selection_query += "GRAPH ?tp" + str(i) + " { " + " ".join(triples[i]) + " } . \n"
    source_selection_query += "}"     

    return source_selection_query              


if __name__ == "__main__":
    virtuoso()