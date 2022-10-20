# Import part

import logging
import warnings
from collections import defaultdict
import click
import coloredlogs
import pandas as pd
import yaml
from yaml.representer import Representer
import random
from time import time
from progress.bar import IncrementalBar
import numpy as np

import ray
ray.init()
#import modin.pandas as pd
import os
#os.environ["MODIN_CPUS"] = "8"
#os.environ["MODIN_ENGINE"] = "ray"
#os.environ["MODIN_NPARTITIONS"] = "8"
from ray.util.multiprocessing import Pool

# Example of use :
# python3 ./scripts/improve_data_coherency.py ./lib/gmark/demo/shop-a/shop-a-graph.txt0.txt ./tmp/tmp.txt

# Goal : Fix gMark data to have a logical data schema

warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='INFO', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def improve_data_coherency(input_file,output_file):


    pool = Pool(8,ray_address="auto")

    start_time = time()
    df = pd.read_csv(input_file, sep=" ", names=['s','p','o'])

    # TODO : Need to fix problem for partial part (when object are in subject, but is bad construct) [Appaer few time]

    # Fix SubGenre problem

    #subgenre_df1 = ps.sqldf("select * from df where p == 'hasGenre'")
    subgenre_df1 = df[df['p'] == 'hasGenre']
    #logger.debug(subgenre_df1)

    subgenre_df1_o_index = subgenre_df1
    subgenre_df1_o_index['id'] = subgenre_df1_o_index['o']
    subgenre_df1_o_index = subgenre_df1_o_index.set_index('id')
    df_s_index = df
    df_s_index['id'] = df_s_index['s']
    df_s_index = df_s_index.set_index('id')
    df_s_index = df_s_index[df_s_index['s'].str.contains("SubGenre",regex=True)]
    df1_join = df_s_index
    df2_join = subgenre_df1_o_index
    subgenre_df2 = df2_join.join(df1_join,how="left",lsuffix="_caller",rsuffix="_other")
    subgenre_df2 = subgenre_df2[subgenre_df2['s_other'].isnull()]
    subgenre_df2 = subgenre_df2.drop(columns=["s_other","p_other","o_other"])
    subgenre_df2 = subgenre_df2.rename(columns={"s_caller": "s", "p_caller": "p", "o_caller": "o"})
    #logger.debug(subgenre_df2)
    subgenre_df2 = subgenre_df2.drop_duplicates(subset=['o'])
    #logger.debug(subgenre_df2)

    genre_df = df[df['o'].str.contains('^Genre',regex=True)]
    #logger.debug(genre_df)

    topic_df = df[df['o'].str.contains('^Topic',regex=True)]
    #logger.debug(topic_df)

    line2add = ""

    bar = IncrementalBar('SubGenre', max = subgenre_df2['s'].size)

    def parallel_subgenre(row_line):
        row = row_line[1]
        sbj = row['o']

        obj_genre = list(genre_df['o'].sample(n=1))[0]
        obj_topic = list(topic_df['o'].sample(n=1))[0]
        return f"{sbj} type {obj_genre}\n{sbj} tag {obj_topic}\n"

    for line in pool.map(parallel_subgenre, subgenre_df2.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} type {obj_genre}")
        #logger.info(f"{sbj} tag {obj_topic}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'w') as wfile:
        with open(input_file, 'r') as rfile:
            rrfile = rfile.read()
            wfile.write(rrfile + line2add)

    df = pd.read_csv(output_file, sep=" ", names=['s','p','o'])

    # Fix Website problem

    #websitep_df = ps.sqldf("select * from df where p == 'homepage' or p == 'trailer' or p == 'subscribes'") # Add subscribes to solve User problem earlier
    websitep_df = df[(df['p'] == 'homepage') | (df['p'] == 'trailer') | (df['p'] == 'subscribes')]
    #logger.debug(websitep_df)

    websitep_df_o_index = websitep_df
    websitep_df_o_index['id'] = websitep_df_o_index['o']
    websitep_df_o_index = websitep_df_o_index.set_index('id')
    df_s_index = df
    df_s_index['id'] = df_s_index['s']
    df_s_index = df_s_index.set_index('id')
    df_s_index = df_s_index[df_s_index['s'].str.contains("Website",regex=True)]
    df1_join = df_s_index
    df2_join = websitep_df_o_index
    websitep_df2 = df2_join.join(df1_join,how="left",lsuffix="_caller",rsuffix="_other")
    websitep_df2 = websitep_df2[websitep_df2['s_other'].isnull()]
    websitep_df2 = websitep_df2.drop(columns=["s_other","p_other","o_other"])
    websitep_df2 = websitep_df2.rename(columns={"s_caller": "s", "p_caller": "p", "o_caller": "o"})
    websitep_df2 = websitep_df2.drop_duplicates(subset=['o'])
    #logger.debug(websitep_df2)

    string_df = df[df['o'].str.contains('^string',regex=True)]
    #logger.debug(string_df)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    #logger.debug(integer_df)

    language_df = df[df['o'].str.contains('^Language',regex=True)]
    #logger.debug(language_df)

    line2add = ""

    bar = IncrementalBar('Website', max = websitep_df2['s'].size)

    def parallel_website(row_line):
        row = row_line[1]
        sbj = row['o']
        obj_string = list(string_df['o'].sample(n=1))[0]
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_language = list(language_df['o'].sample(n=1))[0]
        return f"{sbj} url {obj_string}\n{sbj} hits {obj_integer}\n{sbj} language {obj_language}\n"

    for line in pool.map(parallel_website,websitep_df2.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} url {obj_string}")
        #logger.info(f"{sbj} hits {obj_integer}")
        #logger.info(f"{sbj} language {obj_language}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix User problem

    #userp_df = ps.sqldf("select * from df where p == 'conductor' or p == 'artist' or p == 'actor' or p == 'director' or p == 'author' or p == 'editor' or p == 'contactPoint' or p == 'employee' or p == 'reviewer'") # Add contactPoint, employee and reviewer to solve Retailer and Review problem earlier
    userp_df = df[(df['p'] == 'conductor') | (df['p'] == 'actor') | (df['p'] == 'director') | (df['p'] == 'author') | (df['p'] == 'editor') | (df['p'] == 'contactPoint') | (df['p'] == 'employee') | (df['p'] == 'reviewer')]
    #logger.debug(userp_df)

    userp_df_o_index = userp_df
    userp_df_o_index['id'] = userp_df_o_index['o']
    userp_df_o_index = userp_df_o_index.set_index('id')
    df_s_index = df
    df_s_index['id'] = df_s_index['s']
    df_s_index = df_s_index.set_index('id')
    df_s_index = df_s_index[df_s_index['s'].str.contains("User",regex=True)]
    df1_join = df_s_index
    df2_join = userp_df_o_index
    userp_df2 = df2_join.join(df1_join,how="left",lsuffix="_caller",rsuffix="_other")
    userp_df2 = userp_df2[userp_df2['s_other'].isnull()]
    userp_df2 = userp_df2.drop(columns=["s_other","p_other","o_other"])
    userp_df2 = userp_df2.rename(columns={"s_caller": "s", "p_caller": "p", "o_caller": "o"})
    userp_df2 = userp_df2.drop_duplicates(subset=['o'])
    #logger.debug(userp_df2)

    website_df = df[df['o'].str.contains('^Website',regex=True)]
    #logger.debug(website_df)

    city_df = df[df['o'].str.contains('^City',regex=True)]
    #logger.debug(city_df)

    agegroup_df = df[df['o'].str.contains('^AgeGroup',regex=True)]
    #logger.debug(agegroup_df)

    gender_df = df[df['o'].str.contains('^Gender',regex=True)]
    #logger.debug(gender_df)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    #logger.debug(country_df)

    line2add = ""

    bar = IncrementalBar('User', max = userp_df2['s'].size)

    def parallel_user_1(row_line):
        row = row_line[1]
        sbj = row['o']
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_website = list(website_df['o'].sample(n=1))[0]
        obj_city = list(city_df['o'].sample(n=1))[0]
        line = ""
        if not agegroup_df.empty:
            obj_agegroup = list(agegroup_df['o'].sample(n=1))[0]
            line += f"{sbj} age {obj_agegroup}\n"
            #logger.info(f"{sbj} age {obj_agegroup}")
        if not gender_df.empty:
            obj_gender = list(gender_df['o'].sample(n=1))[0]
            line += f"{sbj} gender {obj_gender}\n"
            #logger.info(f"{sbj} gender {obj_gender}")
        obj_country = list(country_df['o'].sample(n=1))[0]
        line += f"{sbj} userId {obj_integer}\n{sbj} subscribes {obj_website}\n{sbj} location {obj_city}\n{sbj} nationality {obj_country}\n"
        return line

    for line in pool.map(parallel_user_1,userp_df2.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} userId {obj_integer}")
        #logger.info(f"{sbj} subscribes {obj_website}")
        #logger.info(f"{sbj} location {obj_city}")
        #logger.info(f"{sbj} nationality {obj_country}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix City problem

    #location_perf_df = ps.sqldf("select * from df where p == 'location' or p == 'performedIn'") # Add performedIn to solve Product problem earlier
    location_perf_df = df[(df['p'] == 'location') | (df['p'] == 'performedIn')]
    #logger.debug(location_perf_df)

    location_perf_df_o_index = location_perf_df
    location_perf_df_o_index['id'] = location_perf_df_o_index['o']
    location_perf_df_o_index = location_perf_df_o_index.set_index('id')
    df_s_index = df
    df_s_index['id'] = df_s_index['s']
    df_s_index = df_s_index.set_index('id')
    df_s_index = df_s_index[df_s_index['s'].str.contains("City",regex=True)]
    df1_join = df_s_index
    df2_join = location_perf_df_o_index
    location_perf_df2 = df2_join.join(df1_join,how="left",lsuffix="_caller",rsuffix="_other")
    location_perf_df2 = location_perf_df2[location_perf_df2['s_other'].isnull()]
    location_perf_df2 = location_perf_df2.drop(columns=["s_other","p_other","o_other"])
    location_perf_df2 = location_perf_df2.rename(columns={"s_caller": "s", "p_caller": "p", "o_caller": "o"})
    location_perf_df2 = location_perf_df2.drop_duplicates(subset=['o'])
    #logger.debug(location_perf_df2)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    #logger.debug(integer_df)

    line2add = ""

    bar = IncrementalBar('City', max = location_perf_df2['s'].size)

    def parallel_city(row_line):
        row = row_line[1]
        sbj = row['o']
        obj_country = list(country_df['o'].sample(n=1))[0]
        return f"{sbj} parentCountry {obj_country}\n"

    for line in pool.map(parallel_city,location_perf_df2.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} parentCountry {obj_country}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Review problem

    #hasreview_df = ps.sqldf("select * from df where p == 'hasReview'")
    hasreview_df = df[df['p'] == 'hasReview']
    #logger.debug(hasreview_df)

    hasreview_df_o_index = hasreview_df
    hasreview_df_o_index['id'] = hasreview_df_o_index['o']
    hasreview_df_o_index = hasreview_df_o_index.set_index('id')
    df_s_index = df
    df_s_index['id'] = df_s_index['s']
    df_s_index = df_s_index.set_index('id')
    df_s_index = df_s_index[df_s_index['s'].str.contains("Review",regex=True)]
    df1_join = df_s_index
    df2_join = hasreview_df_o_index
    hasreview_df2 = df2_join.join(df1_join,how="left",lsuffix="_caller",rsuffix="_other")
    hasreview_df2 = hasreview_df2[hasreview_df2['s_other'].isnull()]
    hasreview_df2 = hasreview_df2.drop(columns=["s_other","p_other","o_other"])
    hasreview_df2 = hasreview_df2.rename(columns={"s_caller": "s", "p_caller": "p", "o_caller": "o"})
    hasreview_df2 = hasreview_df2.drop_duplicates(subset=['o'])
    #logger.debug(hasreview_df2)

    user_df = df[df['o'].str.contains('^User',regex=True)]
    #logger.debug(user_df)

    line2add = ""

    bar = IncrementalBar('Review', max = hasreview_df2['s'].size)

    def parallel_review(row_line):
        row = row_line[1]
        sbj = row['o']
        obj_rating = list(integer_df['o'].sample(n=1))[0]
        obj_title = list(string_df['o'].sample(n=1))[0]
        obj_text = list(string_df['o'].sample(n=1))[0]
        obj_totalvotes = list(integer_df['o'].sample(n=1))[0]
        obj_user = list(user_df['o'].sample(n=1))[0]
        return f"{sbj} rating {obj_rating}\n{sbj} title {obj_title}\n{sbj} text {obj_text}\n{sbj} totalVotes {obj_totalvotes}\n{sbj} reviewer {obj_user}\n"

    for line in pool.map(parallel_review,hasreview_df2.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} rating {obj_rating}")
        #logger.info(f"{sbj} title {obj_title}")
        #logger.info(f"{sbj} text {obj_text}")
        #logger.info(f"{sbj} totalVotes {obj_totalvotes}")
        #logger.info(f"{sbj} reviewer {obj_user}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Offer problem

    #includes_df = ps.sqldf("select * from df where p == 'includes'")
    includes_df = df[df['p'] == 'includes']
    #logger.debug(includes_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "includes" not in line.strip("\n"):
                output.write(line)

    product_df = df[df['s'].str.contains("Concert|Album|Movie|Book|Product",regex=True)]
    product_df = product_df.drop_duplicates(subset=['s'])
    #logger.debug(product_df)

    line2add = ""

    bar = IncrementalBar('Offer', max = product_df['s'].size)

    def parallel_product_offer(row_line):
        row_product = row_line[1]
        sbj = list(includes_df['s'].sample(n=1))[0]
        obj_product = row_product['s']
        return f"{sbj} includes {obj_product}\n"

    for line in pool.map(parallel_product_offer,product_df.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} includes {obj_product}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    #offers_df = ps.sqldf("select * from df where p == 'offers'")
    offers_df = df[df['p'] == 'offers']
    #logger.debug(offers_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "offers Offer_" not in line.strip("\n"):
                output.write(line)

    offer_df = includes_df[includes_df['s'].str.contains('^Offer',regex=True)]
    offer_df = offer_df.drop_duplicates(subset=['s'])
    #logger.debug(offer_df)

    line2add = ""

    bar = IncrementalBar('Offer', max = offer_df['s'].size)

    def parallel_offer_offer(row_line):
        row_offer = row_line[1]
        line = ""
        if len(list(offers_df['s'])) > 0:
            sbj = list(offers_df['s'].sample(n=1))[0]
            obj_offer = row_offer['s']
            line += f"{sbj} offers {obj_offer}\n"
        return line

    for line in pool.map(parallel_offer_offer,offer_df.iterrows()):
        line2add += line
        #logger.info(f"{sbj} offers {obj_offer}")
        bar.next()

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Purchase problem

    #makespurchase_df = ps.sqldf("select * from df where p == 'makesPurchase'")
    makespurchase_df = df[df['p'] == 'makesPurchase']
    #logger.debug(makespurchase_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "makesPurchase" not in line.strip("\n"):
                output.write(line)

    purchase_df = df[df['s'].str.contains('^Purchase',regex=True)]
    purchase_df = purchase_df.drop_duplicates(subset=['s'])
    #logger.debug(purchase_df)

    line2add = ""

    bar = IncrementalBar('Purchase', max = purchase_df['s'].size)

    def parallel_purchase_purchase(row_line):
        row_purchase = row_line[1]
        line = ""
        if len(list(makespurchase_df['s'])) > 0:
            sbj = list(makespurchase_df['s'].sample(n=1))[0]
            obj_purchase = row_purchase['s']
            line += f"{sbj} makesPurchase {obj_purchase}\n"
        return line

    for line in pool.map(parallel_purchase_purchase,purchase_df.iterrows()):
        line2add += line
        #logger.info(f"{sbj} makesPurchase {obj_purchase}")
        bar.next()

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    #purchasefor_df = ps.sqldf("select * from df where p == 'purchaseFor'")
    purchasefor_df = df[df['p'] == 'purchaseFor']
    #logger.debug(purchasefor_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "purchaseFor" not in line.strip("\n"):
                output.write(line)

    purchaseproduct_df = product_df
    #logger.debug(purchaseproduct_df)

    line2add = ""

    bar = IncrementalBar('Purchase', max = purchasefor_df['s'].size)

    def parallel_purchase(row_line):
        row = row_line[1]
        sbj = row['s']
        obj_purchase = list(purchaseproduct_df['s'].sample(n=1))[0]
        return f"{sbj} purchaseFor {obj_purchase}\n"

    for line in pool.map(parallel_purchase,purchasefor_df.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} purchaseFor {obj_purchase}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Product problem

    #like_df = ps.sqldf("select * from df where p == 'like'")
    like_df = df[df['p'] == 'like']
    #logger.debug(like_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "like" not in line.strip("\n"):
                output.write(line)

    likeproduct_df = product_df
    #logger.debug(likeproduct_df)

    line2add = ""

    bar = IncrementalBar('Product', max = like_df.size)

    def parallel_product(row_line):
        row = row_line[1]
        sbj = row['s']
        obj_like = list(likeproduct_df['s'].sample(n=1))[0]
        return f"{sbj} like {obj_like}\n"

    for line in pool.map(parallel_product,like_df.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} like {obj_like}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix other User problem

    #follows_foaf_df = ps.sqldf("select * from df where p == 'follows' or p == 'friendOf'")
    follows_foaf_df = df[(df['p'] == 'follows') | (df['p'] == 'friendOf')]
    #logger.debug(follows_foaf_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "follows" not in line.strip("\n"):
                output.write(line)

    fuser_df = df[df['s'].str.contains('^User',regex=True)]
    #logger.debug(fuser_df)

    line2add = ""

    bar = IncrementalBar('User', max = follows_foaf_df['s'].size)

    def parallel_user_2(row_line):
        row = row_line[1]
        sbj = row['s']
        obj_follows = list(fuser_df['s'].sample(n=1))[0]
        obj_friends = list(fuser_df['s'].sample(n=1))[0]
        while obj_follows == sbj:
            obj_follows = list(fuser_df['s'].sample(n=1))[0]
        while obj_friends == sbj:
            obj_friends = list(fuser_df['s'].sample(n=1))[0]
        return f"{sbj} follows {obj_follows}\n{sbj} friendOf {obj_friends}\n"

    for line in pool.map(parallel_user_2,follows_foaf_df.iterrows()):
        line2add += line
        bar.next()
        #logger.info(f"{sbj} follows {obj_follows}")
        #logger.info(f"{sbj} friendOf {obj_friends}")

    #logger.info(line2add)

    bar.finish()

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    execution_time = round((time() - start_time) * 1000)
    pool.close()
    logger.info(f"fixator take {execution_time}ms")

if __name__ == "__main__":
    improve_data_coherency()
