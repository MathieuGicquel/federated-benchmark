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
import re
import requests
import logging
import http.client
import glob
from rdflib import Dataset
from rdflib.namespace import RDF


coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

def sparqlQuery(query, baseURL, format="application/x-trig",default_graph_uri=""):
    params={
        "query": query,
        "format":format
        }
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(baseURL )
    response=None
    exception=None
    try:
        with urllib.request.urlopen(req,data=data) as f:
            response = f.read()
    except HTTPError as e:
        exception = e.read()
    return(response,exception)



@click.command()
@click.argument("folder")
@click.argument("output_file")
@click.option("--format", type=str, default="application/x-trig",
    help="Format of the results set, see http://vos.openlinksw.com/owiki/wiki/VOS/VOSSparqlProtocol")
@click.option("--entrypoint", type=str, default="http://localhost:8890/sparql/",
    help="URL of the Virtuoso SPARQL endpoint")

def virtuoso(folder,output_file,format,entrypoint):

    queries = glob.glob(f"{folder}/*.sparql")
    logger.debug(str(queries))

    i = 0

    with open(output_file,'w') as output:
        output.write("")

    for query in queries:

        logger.debug(str(query))

        all_prefix = []
    
        with open(query,'r') as query_file:
            querys=query_file.read()
            query_name=os.path.abspath(query)

            data=sparqlQuery(querys, entrypoint, format)

            if data[1]==None:
                #pass
                with open(output_file,'a') as output:
                    lines = data[0].decode()
                    logger.debug(lines)
                    for line in str(lines).split('\n'):
                        if '@' in line:
                            print(line)
                            pre = line.split(' ')[1]
                            pre = pre.split('\t')[0]
                            print(pre)
                            all_prefix.append(str(pre))
                    for pre in all_prefix:
                        # attention : https://www.w3.org/1999/02/22-rdf4-syntax-ns#type !!
                        pre_t = pre.split(':')[0]
                        #lines = lines.replace(pre_t,pre_t+str(i))
                        lines = re.sub(pre_t + ":", pre_t+str(i) + ":", lines)

                    # convert trig to nq
                    g = Dataset()
                    g.parse(data=lines, format="trig")
                    v = g.serialize(format="nquads")

                    v = str(re.sub(r"\n+", r"\n", str(v)))
                    v = str(re.sub(r"^\n", r"", str(v)))
                    output.write(v)


                    

            else:
                
                print("")

        i += 1
        
        print(all_prefix)


if __name__ == "__main__":
    virtuoso()
