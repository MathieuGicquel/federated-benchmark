import click
import shutil
from bs4 import BeautifulSoup
import yaml
import logging
import coloredlogs
import numpy as np

coloredlogs.install(level='DEBUG', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

#python3 ./scripts/shopanhour.py ./use-case/shop.xml ./prepa/use-cases

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("shop_template")
@click.argument("output")
# Add option for repartition

def shopanhour(shop_template,output):
    nb_shop = str(configuration.get("site"))
    with open(shop_template, 'r') as xmlfile:
        xmldata = xmlfile.read()
    bs_data = BeautifulSoup(xmldata,'html.parser')

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
            outfile.write(bs_data.prettify())

    # sameAs distribution

    sameas_distrib = configuration["sameas_distribution"]
    logger.debug(str(sameas_distrib))
    typ_str_sameas = sameas_distrib.get("choosen_type")
    typ_sameas = sameas_distrib["types"].get(typ_str_sameas)
    logger.debug(str(typ))

    if typ_str_sameas == "uniform":
        new_values_sameas = np.random.uniform(typ_sameas.get("min"),typ_sameas.get("max"), int(nb_shop))
    elif typ_str_sameas == "gaussian":
        new_values_sameas = np.random.normal(typ_sameas.get("mu"),typ_sameas.get("sigma"), int(nb_shop))
    elif typ_str_sameas == "zipfian":
        new_values_sameas = np.random.zipf(typ_sameas.get("alpha"), int(nb_shop))
        new_values_sameas = new_values_sameas * typ_sameas.get("multiplicator")
    else:
        logger.error("Unknown " + typ_str_sameas)
        raise Exception()
    logger.debug(new_values_sameas)

    for i, val in enumerate(new_values_sameas):

        with open(f'{output}/shop-{i}.xml','r') as xmlfile:
            xmldata = xmlfile.read()
        bs_data = BeautifulSoup(xmldata,'html.parser')

        total_sameAs = 0
        for tag_sameas in bs_data.find_all('target', {'symbol':'82'}):
            total_sameAs += 1
        nb_sameAs = int(val / total_sameAs)
        for tag_sameas in bs_data.find_all('target', {'symbol':'82'}):
            out_distrib = bs_data.new_tag("outdistribution", type="uniform")
            min_t = bs_data.new_tag("min")
            min_t.string = str(nb_sameAs)
            max_t = bs_data.new_tag("max")
            max_t.string = str(nb_sameAs)
            out_distrib.append(min_t)
            out_distrib.append(max_t)
            tag_sameas.append(out_distrib)
            #tag_sameas.append(str(f"<outdistribution type=\"uniform\"><min>{nb_sameAs}</min><max>{nb_sameAs}</max></outdistribution>"))
            #logger.debug(str(tag_sameas))
            with open(f'{output}/shop-{i}.xml','w') as outfile:
                outfile.write(bs_data.prettify())

if __name__ == "__main__":
    shopanhour()