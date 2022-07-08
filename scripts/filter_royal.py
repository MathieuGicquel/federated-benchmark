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

#headers = ['Name', 'Code']
#data = sorted([(v,k) for k,v in d.items()]) # flip the code and name and sort
#print(tabulate(data, headers=headers))


coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
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
        "timeout": "3000",
        "format": format,
        "save": "display",
        "fname": ""
        }
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(baseURL)
    response=None
    exception=None
    try:
        with urllib.request.urlopen(req,data=data) as f:
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

def virtuoso(queries,format,output,entrypoint,nb_query):

    queries_files = glob(f"{queries}/*.noask.sparql")

    i_query = 0

    for query in queries_files:
        execution_time=0
        projs=[]
        with open(query) as query_file:
            querys=query_file.read()
            query_name=os.path.basename(query)

            logger.info(f'Virtuoso processing query:{query_name}')
            start_time = time()

            data=sparqlQuery(querys, entrypoint, format)
            execution_time = round((time() - start_time) * 1000)

            if data[1]==None:
                logger.info(f'Query {query_name} complete in {execution_time}ms')

                if output is not None:
                    j = 0
                    for line in (data[0].decode()).split('\n'):
                        if j <= 2:
                            j+=1
                        elif j == 3:
                            break
                    if j == 3:
                        logger.info(f'Query {query_name} has result')
                        logger.debug(data)
                        with open(f'{output}/query-{i_query}.ss.sparql', 'a') as output_file:
                            query_ss_name = query_name.split('.')[0]
                            with open(f'{queries}/{query_ss_name}.ss.sparql', 'r') as ss_file:
                                output_file.write(ss_file.read())

                        with open(f'{output}/query-{i_query}.noask.sparql', 'a') as output_file:
                            output_file.write(querys)
                        i_query+=1
                else:
                    print(data[0].decode())

        if i_query == int(nb_query):
            break


if __name__ == "__main__":
    virtuoso()