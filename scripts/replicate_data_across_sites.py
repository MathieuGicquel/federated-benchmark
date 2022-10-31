# Import part

import logging
import warnings
import click
import coloredlogs
import yaml
import random
import re
from pyspark.sql import SparkSession
import os

# Goal : Duplicate an entity and all object who arrived to it to another site and add a sameAs predicate between them

warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("./configuration.yaml"), Loader=yaml.FullLoader)

spark = SparkSession.builder \
    .master("local") \
    .appName("federated-benchmark") \
    .config("spark.ui.port", '4050') \
    .master("local[*]")
if os.environ["TMPDIR"] is not None:
    print(os.environ["TMPDIR"])
    spark = spark.config("spark.local.dir", os.environ["TMPDIR"])
    spark = spark.config("spark.driver.defaultJavaOptions","-Djava.io.tmpdir=" + os.environ["TMPDIR"])

spark = spark.getOrCreate()

class Quad:
    def __init__(self, s: str, p: str, o: str, g: str):
        self.s: str = s
        self.o: str = o
        self.p: str = p
        self.g: str = g

    def __str__(self):
        return ("%s %s %s %s") % (self.s, self.p, self.o, self.g)

    def move_to_site(self, site: int):
        return Quad(
            re.sub(r"/s[0-9]+/", "/s%s/" % site, self.s),
            re.sub(r"/s[0-9]+/", "/s%s/" % site, self.p),
            re.sub(r"/s[0-9]+/", "/s%s/" % site, self.o),
            re.sub(r"/s[0-9]+", "/s%s" % site, self.g),
        )

    def move_entity_to_site(entity: str, site: int) -> str:
        return re.sub(r"/s[0-9]+/", "/s%s/" % site, entity)

    def move_named_graph_to_site(name: str, site: int) -> str:
        return re.sub(r"/s[0-9]+", "/s%s" % site, name)

    def toNq(self):
        return ("%s %s %s %s .\n") % (self.s, self.p, self.o, self.g)


@click.command()
@click.argument("input_file")
@click.argument("output_file")
@click.argument("number_of_site")
def replicate_data_across_sites(input_file, output_file, number_of_site):
    nsite = int(number_of_site)

    lines = spark.read.text(input_file).rdd.map(lambda row: row[0])

    def parseQuads(line):
        parts = re.split(r'\s+', line)
        return Quad(parts[0], parts[1], parts[2], parts[3])

    triples = lines.map(lambda line: parseQuads(line))
    sameas_proba = configuration["sameas_proba"]

    toAddSameAs = triples \
        .filter(
        lambda quad: quad.p == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" and quad.s.split('/')[-1].startswith(
            tuple(sameas_proba.keys()))) \
        .filter(lambda quad: random.random() < sameas_proba.get(quad.s.split('/')[-1].split('_')[0]))

    # select * where {
    #   ?product a Product .
    #   ?offer includes ?product .
    #   ?retailer offers ?offer .
    #   ?product ?p ?productInfo
    #   }
    tp1 = toAddSameAs.keyBy(lambda t: t.s)
    tp2 = triples.filter(lambda t: t.p == '<http://example.org/includes>').keyBy(lambda t: t.o)

    join1 = tp1.join(tp2).keyBy(lambda b: b[1][1].s)

    tp3 = triples.filter(lambda quad: quad.p == "<http://example.org/offers>").keyBy(lambda quad: quad.o)

    joined2 = join1.join(tp3).keyBy(lambda b: b[1][0][0])

    tp4 = triples.keyBy(lambda t: t.s)

    joined3 = joined2.join(tp4)

    groupBy = joined3.keyBy(lambda b: b[0]).groupByKey()  # keyBy product

    def flatten_triples(x):
        quads = set()
        for data in x[1]:
            q1 = data[1][0][1][1]
            q2 = data[1][0][1][0][1][0]
            q3 = data[1][0][1][0][1][1]
            q4 = data[1][1]
            quads.add(q1)
            quads.add(q2)
            quads.add(q3)
            quads.add(q4)

        return (x[0], quads)

    flatten = groupBy.map(flatten_triples)

    def moveToOtherSite(x):
        key = x[0]
        quads = x[1]

        current_site = int(re.search(r"/s([0-9]+)/", key).group(1))
        random_site = current_site
        while random_site == current_site:
            random_site = random.randrange(nsite)

        new_quads = set()
        for quad in quads:
            new_quad = quad.move_to_site(random_site)
            new_quads.add(new_quad)

        new_quad = Quad(Quad.move_entity_to_site(key, random_site), "<http://www.w3.org/2002/07/owl#sameAs>", key,
                        "<http://example.org/s%s>" % random_site)
        new_quads.add(new_quad)
        return (key, new_quads)

    #todo remove collect : concat newSameas together then concat all newSameAs and data
    moves = flatten.map(moveToOtherSite).collect()  # moved : (key, set)

    import shutil
    shutil.copyfile(input_file, output_file)
    with open(f"{output_file}", "a") as wfile:
        for move in moves:
            key = move[0]
            quads = move[1]
            for quad in quads:
                wfile.write(quad.toNq())


if __name__ == "__main__":
    replicate_data_across_sites()
