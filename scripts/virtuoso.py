import click
import logging
import coloredlogs
import urllib
import urllib.parse
import urllib.request
from time import time
from pathlib import Path
from urllib.error import HTTPError

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
    response=None
    exception=None
    try:
        with urllib.request.urlopen(req,data=data) as f:
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
        query_name=query

        logger.info(f'Virtuoso processing query:{query_name}')
        start_time = time()

        data=sparqlQuery(querys, entrypoint, format)
        execution_time = (time() - start_time) * 1000

        if data[1]==None:

            report=f'query,exec_time\n{query_name},{execution_time}\n'
            if measures is not None:
                with open(measures, 'w') as measures_file:
                    measures_file.write(report)
            logger.info(f'Query {query_name} complete in {execution_time}s')

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
