# Import part

import click
import shutil
from bs4 import BeautifulSoup
import yaml
import logging
import coloredlogs
import numpy as np
from html import escape
from html.parser import HTMLParser
from lxml.html.soupparser import fromstring
from lxml.etree import tostring
import re

# Example of use :
# python3 ./scripts/generate_use_cases.py ./use-case/shop.xml ./prepa/use-cases

# Goal : Take a shop.xml file template and apply the data distribution from configuration.yaml on it to have a variation of number of nodes

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("shop_template")
@click.argument("output")

def generate_use_cases(shop_template,output):
    nb_shop = str(configuration.get("site"))
    with open(shop_template, 'r') as xmlfile:
        xmldata = xmlfile.read()
        xmldata = re.sub(r"<!--.*-->","",xmldata)
    bs_data = BeautifulSoup(xmldata,'xml')

    # Data distribution

    tag = bs_data.find('nodes')
    logger.debug(str(tag.string))

    data_distrib = configuration["data_distribution"]
    logger.debug(str(data_distrib))
    typ_str = data_distrib.get("choosen_type")
    typ = data_distrib["types"].get(typ_str)
    logger.debug(str(typ))

    if typ_str == "uniform":
        new_values = np.random.uniform(typ.get("min"),typ.get("max"), int(nb_shop))
    elif typ_str == "gaussian":
        new_values = np.random.normal(typ.get("mu"),typ.get("sigma"), int(nb_shop))
    elif typ_str == "zipfian":
        new_values = np.random.zipf(typ.get("alpha"), int(nb_shop))
        new_values = new_values * typ.get("multiplicator")
    else:
        logger.error("Unknown " + typ_str)
        raise Exception()
    logger.debug(new_values)

    for i, val in enumerate(new_values):
        tag.string.replace_with(str(int(val)))
        logger.debug(str(tag.string))
        with open(f'{output}/shop-{i}.xml','w') as outfile:
            str_xml = str(bs_data.prettify())
            str_xml = re.sub(r"(\s*<.+>)\n\s*([A-Za-z0-9\.]+)\n\s*(</.+>)\n",r"\1\2\3\n",str_xml)
            outfile.write(str_xml)

if __name__ == "__main__":
    generate_use_cases()