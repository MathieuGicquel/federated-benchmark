# Import part

import click
import pandas as pd
import glob
import os
import random
import numpy as np
import yaml
import logging
import coloredlogs
import warnings

# Goal : correctly convert txt data into ttl data

warnings.simplefilter(action='ignore', category=FutureWarning)

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("txt_file")
@click.argument("output")

def convert(txt_file, output):
    nb_site = int(configuration["site"])

    df = pd.read_csv(txt_file, sep=" ", names=['s', 'p', 'o'])

    with open(f'{output}', 'w') as ffile:
        df["sNq"] = df.apply(lambda l: subject(l, nb_site), axis=1)
        df["pNq"] = df.apply(lambda l: predicate(l), axis=1)
        df["oNq"] = df.apply(lambda l: objecte(l, nb_site), axis=1)
        df["gNq"] = df.apply(lambda l: graph(l, nb_site), axis=1)

        ffile.write(generate_nq(df))

def subject(line, nb_site):
    subject_ = str(line['s'])
    site = int(subject_.split('_')[1]) % nb_site

    return "<http://example.org/s" + str(int(site)) + "/" + str(subject_) + ">"

def predicate(line):
    predicate_ = line['p']
    logger.debug(predicate_)
    if "sameAs" in predicate_:
        return "<http://www.w3.org/2002/07/owl#sameAs>"
    else:
        return "<http://example.org/p" + str(predicate_) + ">"

def objecte(line, nb_site):
    objecte_ = str(line['o'])
    site = int(objecte_.split('_')[1]) % nb_site

    if str(objecte_).split('_')[0] in ["string", "integer", "date"]:
        if str(objecte_).split('_')[0] in ["string"]:
            objecte_ = "\"" + str(objecte_) + "\""
        else:
            objecte_ = str(objecte_)
    else:
        objecte_ = "<http://example.org/s" + str(int(site)) + "/" + str(objecte_) + ">"
    return objecte_

def graph(line, nb_site):
    graph_ = str(line['s'])
    site = int(graph_.split('_')[1]) % nb_site

    return "<http://example.org/s" + str(int(site)) + ">"

def generate_nq(df: pd.DataFrame) -> str:
    data = df[["sNq", "pNq", "oNq", "gNq"]]

    rdf = ""
    for index, row in data.iterrows():
        rdf += f'{row["sNq"]} {row["pNq"]} {row["oNq"]} {row["gNq"]} .\n'

    return rdf

if __name__ == "__main__":
    convert()