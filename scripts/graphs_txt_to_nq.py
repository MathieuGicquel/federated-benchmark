# Import part

import click
import pandas as pd
import glob
import os
import numpy as np
import yaml
import logging
import coloredlogs
import warnings
from pyspark.sql import SparkSession
import re

# Goal : correctly convert txt data into nq data

warnings.simplefilter(action='ignore', category=FutureWarning)

coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("./configuration.yaml"), Loader=yaml.FullLoader)
spark = SparkSession.builder \
    .master("local") \
    .appName("federated-benchmark") \
    .config("spark.ui.port", '4050') \
    .setMaster("local['*']")
if os.environ["TMPDIR"] is not None:
    print(os.environ["TMPDIR"])
    spark = spark.config("spark.local.dir", os.environ["TMPDIR"])
    spark = spark.config("spark.driver.defaultJavaOptions","-Djava.io.tmpdir=" + os.environ["TMPDIR"])

spark = spark.getOrCreate()


@click.command()
@click.argument("input_folder")
@click.argument("output")
def graphs_txt_to_nq(input_folder, output):
    logger.debug(input_folder)
    files = glob.glob(f'{input_folder}/*.txt0.fixed.txt')
    triples = spark.sparkContext.emptyRDD()

    def parseQuads(line, src):
        parts = re.split(r'\s+', line)
        return parts[0], parts[1], parts[2], src

    for file in files:
        rdd = spark.read.text(file).rdd.map(lambda x: x[0]).map(lambda x: parseQuads(x, file))
        print("Sample = ", rdd.take(1))
        triples = triples.union(rdd)

    triples = triples.map(to_federated_shop)
    print(triples.take(1))
    object_type = triples.filter(lambda x: not x[0].startswith("integer") and  not x[0].startswith("string") and not x[0].startswith("date")).map(
        lambda x: add_type(x[0], x[3])).distinct()
    subject_type = triples.filter(lambda x: not x[2].startswith("integer") and  not x[2].startswith("string") and not x[2].startswith("date")).map(
        lambda x: add_type(x[2], x[3])).distinct()
    types = object_type.union(subject_type).distinct()
    triples = triples.union(types)
    quads = triples.map(tuple_to_quads)

    workdir = os.path.dirname(output)
    workdir_spark = workdir + "/spark"

    import shutil
    shutil.rmtree(workdir_spark, ignore_errors=True)

    quads.saveAsTextFile(workdir_spark)

    # concatenate
    import shutil
    with open(output, 'wb') as wfd:
        for f in glob.glob(f'{workdir_spark}/*'):
            with open(f, 'rb') as fd:
                shutil.copyfileobj(fd, wfd)



def to_federated_shop(x: str):
    s = x[0]
    p = x[1]
    o = x[2]
    g = x[3]

    if (s.split("_")[0] in configuration["shared_types"]):
        s = "<http://example.org/federated_shop/" + s + ">"

    if (o.split("_")[0] in configuration["shared_types"]):
        o = "<http://example.org/federated_shop/" + o + ">"

    if (s.startswith("<http://example.org/federated_shop/")):
        g = "<http://example.org/federated_shop>"
    return (s, p, o, g)


def add_type(entity, file):
    if file.startswith("<http://example.org/federated_shop") or entity.startswith("<http://example.org/federated_shop"):
        site = "<http://example.org/federated_shop>"
        entity_nq = entity
    else:
        search = re.search(r"http://example.org/s([0-9]+)", entity)
        if search is not None:
            site = f"<http://example.org/s{search.group(1)}>"
            entity_nq = entity
        else:
            try:
                match = re.search(r"/data-([0-9]+).txt0.fixed.txt", file).group(1)
            except:
                print(file)
                raise Exception()
            site = f"<http://example.org/s{match}>"
            subject = str(entity.split("_")[0] + "_s" + match + "_" + entity.split("_")[1])
            entity_nq = f"<http://example.org/s{match}/" + str(subject) + ">"

    new_q = (entity_nq,
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://example.org/federated_shop/" + entity.split("/")[-1].split("_")[0] + ">",
            f"{site}"
            )
    return new_q


def tuple_to_quads(t):        
    match = None
    if(t[3].startswith("<http://")):
        g = t[3]
    else:
        search = re.search(r"/data-([0-9]+).txt0.fixed.txt", t[3])
        if search != None:
            match = search.group(1)
            g = f"<http://example.org/s{match}>"
        else:
            g = "<http://example.org/federated_shop>"
            
    q = subject_to_uri(t[0], match) + " " + predicate_to_uri(t[1]) + " " + objecte(t[2], match) + " " + g + " ."
    return q

# TODO
def subject_to_uri(s, site):
    if s.startswith('<'):
        return s

    if site == None:
        return "<http://example.org/federated_shop/" + str(s) + ">"
    
    search = re.search(r"<http://example.org/s([0-9]+)", site)
    if search != None:
        site = search.group(1)

    subject_split = s.split("_")
    subject = str(subject_split[0] + "_s" + site + "_" + subject_split[1])
    return "<http://example.org/s" + str(int(site)) + "/" + str(subject) + ">"


def predicate_to_uri(p):
    if p.startswith('<http://'):
        return p
    else:
        predicate = p
        predicate = "<http://example.org/" + str(predicate) + ">"
        return predicate


def objecte(o, site):
    object_type = o.split('_')[0]
    objecte = o

    if objecte.startswith('<'):
        return objecte

    if site == None:
        return "<http://example.org/federated_shop/" + str(objecte) + ">"

    if object_type in ["string", "integer", "date"]:
        if str(object_type) in ["string"]:
            objecte = "\"" + str(objecte) + "\""
            logger.debug(f"String found {objecte}")
        else:
            if str(object_type) in ["integer", "date"]:
                logger.debug(str(object_type))
                objecte = str(objecte).replace(object_type + "_", "")
            else:
                objecte = str(objecte)
    else:
        objecte_split = str(objecte).split("_")
        objecte = str(objecte_split[0] + "_s" + site + "_" + objecte_split[1])
        logger.debug(objecte)
        objecte = "<http://example.org/s" + str(int(site)) + "/" + str(objecte) + ">"
    return objecte


if __name__ == "__main__":
    graphs_txt_to_nq()
