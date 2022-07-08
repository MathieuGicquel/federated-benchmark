import logging
import warnings
from collections import defaultdict
import re
from bs4 import BeautifulSoup 

import click
import coloredlogs
import pandas as pd
import yaml
from yaml.representer import Representer

import pandasql as ps

warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

@click.command()
@click.argument("input_file")
@click.argument("nb_retailer")
@click.argument("output_file")

def retailer_evolution(input_file,nb_retailer,output_file):
    nb_ret = int(nb_retailer)
    with open(input_file, 'r') as xmlfile:
        xmldata = xmlfile.read()
    
    xmldata = re.sub(r"(<fixed type=\"15\")>([0-9]+)<(\/fixed>)",rf"\1>{nb_ret}<\3", xmldata)

    with open(output_file, 'w') as out:
        out.write(xmldata)

if __name__ == "__main__":
    retailer_evolution()