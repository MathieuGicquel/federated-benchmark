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

# Goal : Take only queries who return result

coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

def sparqlQuery(query, baseURL, format="text/csv",default_graph_uri=""):

    # Set HTTP request's parameters

    params={
        "default-graph-uri": default_graph_uri,
        "should-sponge": "soft",
        "query": query,
        "debug": "on",
        "timeout": "3000",
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
@click.argument("queries")
@click.option("--format", type=click.Choice(["text/csv"]), default="text/csv",
    help="Format of the results set, see http://vos.openlinksw.com/owiki/wiki/VOS/VOSSparqlProtocol")
@click.option("--output", type=str, default=None,
    help="The folder in which the query result will be stored.")
@click.option("--entrypoint", type=str, default="http://localhost:8890/sparql/",
    help="URL of the Virtuoso SPARQL endpoint")
@click.argument("nb_query")

def filter_queries(queries,format,output,entrypoint,nb_query):

    # Get all queries files

    queries_files = glob(f"{queries}/*.sparql")
    logger.debug(str(queries_files))
    i_query = 0

    for query in queries_files:
        execution_time=0
        projs=[]
        with open(query) as query_file:

            # Get different informations about the query

            querys=query_file.read()
            query_name=os.path.basename(query)
            query_ext="".join(Path(query).suffixes)

            logger.debug(f'Virtuoso processing query:{query_name}')
            start_time = time()

            data=sparqlQuery(querys, entrypoint, format)
            execution_time = round((time() - start_time) * 1000)

            if data[1]==None:
                logger.debug(f'Query {query_name} complete in {execution_time}ms')

                # If the query return at least 1 result, we select it

                if output is not None:
                    j = 0
                    for line in (data[0].decode()).split('\n'):
                        if j <= 2:
                            j+=1
                        elif j == 3:
                            break
                    if j == 3:
                        logger.debug(f'Query {query} has result')
                        logger.debug(data)

                        with open(f'{output}/query-{i_query}{query_ext}', 'w') as output_file:
                            output_file.write(f"# {query} \n{querys}")
                        i_query+=1
                    else:
                        logger.info(f'Query {query} has no result')
                else:
                    print(data[0].decode())

        # If we have enough query, we not continue

        if i_query == int(nb_query):
            break

if __name__ == "__main__":
    filter_queries()