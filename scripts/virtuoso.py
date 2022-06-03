import re
import click
import logging
import coloredlogs
import urllib
import urllib.parse
import urllib.request
import json
import pandas as pd
from glom import glom
from pathlib import Path
from time import time

from tabulate import tabulate

#headers = ['Name', 'Code']
#data = sorted([(v,k) for k,v in d.items()]) # flip the code and name and sort
#print(tabulate(data, headers=headers))


coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
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
    with urllib.request.urlopen(req,data=data) as f:
        #resp=pd.read_csv(f)
        resp = f.read()
        return resp
#        return json.loads(resp)


@click.command()
@click.argument("query")
@click.option("--format", type=click.Choice(["text/csv"]), default="text/csv",
    help="Format of the results set, see http://vos.openlinksw.com/owiki/wiki/VOS/VOSSparqlProtocol")
@click.option("--measures", type=str, default=None,
    help="The file in which query execution statistics will be stored.")
@click.option("--output", type=str, default=None,
    help="The file in which the query result will be stored.")
@click.option("--entrypoint", type=str, default="http://localhost:8890/sparql/",
    help="URL of the Virtuoso SPARQL endpoint")

def virtuoso(query,format,measures,output,entrypoint):

    engine="virtuoso"
    execution_time=0
    projs=[]
    with open(query) as query_file:
        querys=query_file.read()
        query_name=Path(query).stem

        logger.info(f'{engine} processing query:{query_name}')
        start_time = time()

        data=sparqlQuery(querys, entrypoint, format)
        execution_time = time() - start_time

        report=f'{query_name},{engine},{execution_time}\n'
        if measures is not None:
            with open(measures, 'w') as measures_file:
                measures_file.write(report)
        logger.info(f'Query {query_name} complete in {execution_time}s')

        if output is not None:
            with open(output, 'w') as output_file:
                output_file.write(data.decode())
        else:
            print(data.decode())
#        print("Retrieved data:\n" + json.dumps(data, sort_keys=True, indent=4))


if __name__ == "__main__":
    virtuoso()
