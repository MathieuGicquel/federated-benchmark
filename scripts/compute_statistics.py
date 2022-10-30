# Import part

import logging
import warnings
from collections import defaultdict
import click
import coloredlogs
import yaml
from pyspark.sql import SparkSession
from yaml.representer import Representer
from pyspark.sql import Row
import re
# Goal : Create a yaml statistic files to do some plot on it

yaml.add_representer(defaultdict, Representer.represent_dict)
warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

class Quad:
    def __init__(self, s: str, p: str, o: str, g: str):
        self.s: str = s
        self.o: str = o
        self.p: str = p
        self.g: str = g

    def __str__(self):
        return ("%s %s %s %s") % (self.s, self.p, self.o, self.g)

    def get_site_of_entity(entity : str):
        """<http://example.org/s2/MusicAlbum_s0_1749> -> should return <http://example.org/s2>
            "string_2515" -> should return LITERALS
        """
        matched = re.match("<http://example.org/(.+)/.+>", entity)
        if matched == None:
            return "LITERALS"

        extract = matched.group(1)
        return "<http://example.org/%s>" % extract

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def compute_statistics(input_file, output_file):
    spark = SparkSession.builder.master("local").appName("Replicator").config("spark.ui.port", '4050').getOrCreate()

    def parseQuads(line):
        parts = re.split(r'\s+', line)
        return Quad(parts[0], parts[1], parts[2], parts[3])

    lines = spark.read.text(input_file).rdd.map(lambda row: row[0])
    quads = lines.map(lambda line: parseQuads(line))

    quads_row=quads.map(lambda x: Row(subject=x.s,predicate=x.p,object=x.o,graph=x.g, origin_subject=Quad.get_site_of_entity(x.s), origin_object=Quad.get_site_of_entity(x.o)))
    df=spark.createDataFrame(quads_row)
    df.createOrReplaceTempView("quads")


    entites_per_site = spark.sql("""
        SELECT graph, object, count(subject) 
        FROM quads 
        WHERE predicate ='<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' 
        GROUP BY graph, object
        """)

    entities_all_sites = spark.sql("""
        SELECT object, count(subject) 
        FROM quads 
        WHERE predicate ='<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' 
        GROUP BY object
        """)

    links_out_per_site = spark.sql("""
        SELECT graph, predicate, origin_subject, origin_object, count(object) 
        FROM quads 
        WHERE origin_subject != origin_object AND origin_object!='LITERALS' 
        GROUP BY graph, predicate, origin_subject, origin_object 
        """)

    graphs = list(map(lambda x: x[0],spark.sql("""SELECT DISTINCT graph FROM quads""").collect()))

    # messing with data...
    output_dict = dict()
    output_dict["perSite"] = dict()
    for graph in graphs:
        output_dict["perSite"][graph] = dict()

        # entities per site
        output_dict["perSite"][graph]["entites_per_site"] = dict()
        entitesCurrentSite = entites_per_site.rdd.filter(lambda x: x["graph"] == graph).collect()
        for entity in entitesCurrentSite:
            output_dict["perSite"][graph]["entites_per_site"][entity[1]] = entity[2]

        # links out per site
        output_dict["perSite"][graph]["linksOut"] = dict()
        linksOutCurrentSite = links_out_per_site.rdd.filter(lambda x: x["graph"] == graph).collect()
        for linkOut in linksOutCurrentSite:
            if linkOut[1] not in dict(output_dict["perSite"][graph]["linksOut"]).keys():
                output_dict["perSite"][graph]["linksOut"][linkOut[1]] = dict()
            output_dict["perSite"][graph]["linksOut"][linkOut[1]][linkOut[3]] = linkOut[4]

    # entities_all_sites
    output_dict["allSites"] = dict()
    output_dict["allSites"]["entites_per_site"] = dict()
    for entity in entities_all_sites.collect():
        output_dict["allSites"]["entites_per_site"][entity[0]] = entity[1]


    with open(f"{output_file}", 'w') as yaml_file:
        logger.debug("Writing yaml")
        yaml.dump(output_dict, yaml_file)

if __name__ == "__main__":
    compute_statistics()
