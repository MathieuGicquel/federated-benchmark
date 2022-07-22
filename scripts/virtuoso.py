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

# Goal : Execute query on Virtuoso

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

    try:

        with urllib.request.urlopen(req,data=data) as f:

            # Read HTTP request's result

            response = f.read()

    except HTTPError as e:

        exception = e.read()

    return(response,exception)

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

    execution_time=0
    projs=[]
    with open(query) as query_file:
        querys=query_file.read()
        query_name=os.path.abspath(query)

        logger.info(f'Virtuoso processing query:{query_name}')
        start_time = time()

        data=sparqlQuery(querys, entrypoint, format)
        execution_time = round((time() - start_time) * 1000)

        # If the query failed, we set his execution time to failed, else we set the execution time by the time it takes

        if data[1]==None:

            report=f'query,exec_time\n{query_name},{execution_time}\n'
            if measures is not None:
                with open(measures, 'w') as measures_file:
                    measures_file.write(report)
            logger.info(f'Query {query_name} complete in {execution_time}ms')

            if output is not None:
                with open(output, 'w') as output_file:
                    output_file.write(data[0].decode())
            else:
                print(data[0].decode())
        else:

            report=f'query,exec_time\n{query_name},"failed"\n'
            if measures is not None:
                with open(measures, 'w') as measures_file:
                    measures_file.write(report)
            logger.info(f'Query {query_name} complete in {execution_time}s')

            if output is not None:
                with open(output, 'w') as output_file:
                    output_file.write("failed")
            else:
                print("")

if __name__ == "__main__":
    virtuoso()
