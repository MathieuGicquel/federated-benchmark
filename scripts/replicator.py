# Import part

import logging
import warnings
from collections import defaultdict
import click
import coloredlogs
import pandas as pd
import yaml
from yaml.representer import Representer
import pandasql as ps
import random
import re

# Goal : Duplicate an entity and all object who arrived to it to another site and add a sameAs predicate between them

warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def replicator(input_file,output_file):

    df = pd.read_csv(input_file, sep=" ", names=['s','p','o','g','dot'],quotechar="'")

    sameas_proba = configuration["sameas_proba"]

    df_g = set(df['g'].unique())

    logger.debug(type(sameas_proba))

    for index, row in df.iterrows():
        type_s = (row['o'].split('_')[0]).split('/')[-1]
        logger.debug(type_s)
        if type_s in sameas_proba.keys():
            sameas_proba_product = sameas_proba.get(str(type_s))
            random_float = random.random()
            if random_float <= sameas_proba_product:
                row_g = set()
                row_g.add(row['g'])
                df_g_other = df_g - row_g
                logger.debug(df_g)
                logger.debug(row_g)
                logger.debug(df_g_other)
                new_g = random.sample(df_g_other,1)[0]
                logger.debug(new_g)
                duplicator(df,row,new_g)

    with open(f"{output_file}","w") as wfile:
        wfile.write(generate_nq(df))

def generate_nq(df: pd.DataFrame) -> str:
    data = df[["s","p","o","g"]]
   
    rdf = ""
    len_df = len(df)
    for index, row in data.iterrows():
        rdf += f'{row["s"]} {row["p"]} {row["o"]} {row["g"]} .\n'
        logger.debug(f"{index}/{len_df}")
        
   
    return rdf

def duplicator(df: pd.DataFrame,row: pd.core.series.Series,other_site: str) -> pd.DataFrame:
    duplicate_product = row['o']
    logger.debug(duplicate_product)
    origin_site = row['g']
    logger.debug(origin_site)
    offers = df[(df['o'] == duplicate_product) & (df['g'] == origin_site) & (df['p'].str.contains("includes"))]
    logger.debug(list(offers['s']))

    for idx, row in offers.iterrows():

        # Select all offers from a given product from a line and duplicate it on other_site (graph) :
        # new_offer includes new_product other_site .
        # And add sameAs between the original product and the duplicate product :
        # new_product sameAs original_product other_site .

        s_origin = (other_site.split('/')[-1]).replace(">","")
        new_offer = re.sub(r"/s[0-9]+/",f"/{s_origin}/",(row['s']))
        new_product = re.sub(r"/s[0-9]+/",f"/{s_origin}/",(row['o']))
        df.loc[len(df)] = [new_product, "<http://www.w3.org/2002/07/owl#sameAs>", row['o'], other_site, "."]
        logger.info(f"Cloning row from {row} to {df.loc[len(df) - 1]}")

        df.loc[len(df)] = [new_offer, row["p"], new_product, other_site, "."]

    retailers = df[(df['o'].isin(list(offers['s']))) & (df['g'] == origin_site) & (df['p'].str.contains("offers"))]
    logger.debug(retailers)
    for idx, row in retailers.iterrows():

        # Select all retailer from given offers from a line and duplicate it on other_site (graph) :
        # new_retailer offers new_offer other_site .

        s_origin = (other_site.split('/')[-1]).replace(">","")
        new_retailer = re.sub(r"/s[0-9]+/",f"/{s_origin}/",(row['s']))
        new_offer = re.sub(r"/s[0-9]+/",f"/{s_origin}/",(row['o']))
        df.loc[len(df)] = [new_retailer, row["p"], new_offer, other_site, "."]
        logger.info(f"Cloning row from {row} to {df.loc[len(df) - 1]}")

if __name__ == "__main__":
    replicator()