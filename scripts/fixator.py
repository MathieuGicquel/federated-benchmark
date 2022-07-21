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

warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def fixator(input_file,output_file):
    df = pd.read_csv(input_file, sep=" ", names=['s','p','o'])

    # TODO : Need to fix problem for partial part (when object are in subject, but is bad construct) [Appaer few time]

    # RUN FIXATOR : python3 ./scripts/fixator.py ./lib/gmark/demo/shop-a/shop-a-graph.txt0.txt ./tmp/tmp.txt

    # Fix SubGenre problem

    # Force

    subgenre_df1 = ps.sqldf("select * from df where p == 'hasGenre'")
    logger.debug(subgenre_df1)

    subgenre_df2 = (subgenre_df1[~subgenre_df1['o'].isin(df['s'])]).dropna()
    subgenre_df2 = subgenre_df2.drop_duplicates(subset=['o'])
    logger.debug(subgenre_df2)

    genre_df = df[df['o'].str.contains('^Genre',regex=True)]
    logger.debug(genre_df)

    topic_df = df[df['o'].str.contains('^Topic',regex=True)]
    logger.debug(topic_df)

    line2add = ""

    for index, row in subgenre_df2.iterrows():
        sbj = row['o']
        obj_genre = list(genre_df['o'].sample(n=1))[0]
        obj_topic = list(topic_df['o'].sample(n=1))[0]
        line2add += f"{sbj} type {obj_genre}\n{sbj} tag {obj_topic}\n"
        logger.info(f"{sbj} type {obj_genre}")
        logger.info(f"{sbj} tag {obj_topic}")

    with open(output_file, 'w') as wfile:
        with open(input_file, 'r') as rfile:
            rrfile = rfile.read()
            wfile.write(rrfile + line2add)

    df = pd.read_csv(output_file, sep=" ", names=['s','p','o'])

    # Fix Website problem

    # Force 

    websitep_df = ps.sqldf("select * from df where p == 'homepage' or p == 'trailer' or p == 'subscribes'") # Add subscribes to solve User problem earlier
    logger.debug(websitep_df)

    websitep_df2 = (websitep_df[~websitep_df['o'].isin(df['s'])]).dropna()
    websitep_df2 = websitep_df2.drop_duplicates(subset=['o'])
    logger.debug(websitep_df2)

    string_df = df[df['o'].str.contains('^string',regex=True)]
    logger.debug(string_df)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug(integer_df)

    language_df = df[df['o'].str.contains('^Language',regex=True)]
    logger.debug(language_df)

    line2add = ""

    for index, row in websitep_df2.iterrows():
        sbj = row['o']
        obj_string = list(string_df['o'].sample(n=1))[0]
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_language = list(language_df['o'].sample(n=1))[0]
        line2add += f"{sbj} url {obj_string}\n{sbj} hits {obj_integer}\n{sbj} language {obj_language}\n"
        logger.info(f"{sbj} url {obj_string}")
        logger.info(f"{sbj} hits {obj_integer}")
        logger.info(f"{sbj} language {obj_language}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix User problem

    # Force 

    userp_df = ps.sqldf("select * from df where p == 'conductor' or p == 'artist' or p == 'actor' or p == 'director' or p == 'author' or p == 'editor' or p == 'contactPoint' or p == 'employee' or p == 'reviewer'") # Add contactPoint, employee and reviewer to solve Retailer and Review problem earlier
    logger.debug(userp_df)

    userp_df2 = (userp_df[~userp_df['o'].isin(df['s'])]).dropna()
    userp_df2 = userp_df2.drop_duplicates(subset=['o'])
    logger.debug(userp_df2)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug(integer_df)

    website_df = df[df['o'].str.contains('^Website',regex=True)]
    logger.debug(website_df)

    city_df = df[df['o'].str.contains('^City',regex=True)]
    logger.debug(city_df)

    agegroup_df = df[df['o'].str.contains('^AgeGroup',regex=True)]
    logger.debug(agegroup_df)

    gender_df = df[df['o'].str.contains('^Gender',regex=True)]
    logger.debug(gender_df)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    logger.debug(country_df)

    line2add = ""

    for index, row in userp_df2.iterrows():
        sbj = row['o']
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_website = list(website_df['o'].sample(n=1))[0]
        obj_city = list(city_df['o'].sample(n=1))[0]
        if not agegroup_df.empty:
            obj_agegroup = list(agegroup_df['o'].sample(n=1))[0]
            line2add += f"{sbj} age {obj_agegroup}\n"
            logger.info(f"{sbj} age {obj_agegroup}")
        if not gender_df.empty:
            obj_gender = list(gender_df['o'].sample(n=1))[0]
            line2add += f"{sbj} gender {obj_gender}\n"
            logger.info(f"{sbj} gender {obj_gender}")
        obj_country = list(country_df['o'].sample(n=1))[0]
        line2add += f"{sbj} userId {obj_integer}\n{sbj} subscribes {obj_website}\n{sbj} location {obj_city}\n{sbj} nationality {obj_country}\n"
        logger.info(f"{sbj} userId {obj_integer}")
        logger.info(f"{sbj} subscribes {obj_website}")
        logger.info(f"{sbj} location {obj_city}")
        logger.info(f"{sbj} nationality {obj_country}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix City problem

    # Force

    location_perf_df = ps.sqldf("select * from df where p == 'location' or p == 'performedIn'") # Add performedIn to solve Product problem earlier
    logger.debug(location_perf_df)

    location_perf_df2 = (location_perf_df[~location_perf_df['o'].isin(df['s'])]).dropna()
    location_perf_df2 = location_perf_df2.drop_duplicates(subset=['o'])
    logger.debug(location_perf_df2)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    logger.debug(integer_df)

    line2add = ""

    for index, row in location_perf_df2.iterrows():
        sbj = row['o']
        obj_country = list(country_df['o'].sample(n=1))[0]
        line2add += f"{sbj} parentCountry {obj_country}\n"
        logger.info(f"{sbj} parentCountry {obj_country}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Review problem

    # Force

    hasreview_df = ps.sqldf("select * from df where p == 'hasReview'")
    logger.debug(hasreview_df)

    hasreview_df2 = (hasreview_df[~hasreview_df['o'].isin(df['s'])]).dropna()
    hasreview_df2 = hasreview_df2.drop_duplicates(subset=['o'])
    logger.debug(hasreview_df2)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug(integer_df)

    string_df = df[df['o'].str.contains('^string',regex=True)]
    logger.debug(string_df)

    user_df = df[df['o'].str.contains('^User',regex=True)]
    logger.debug(user_df)

    line2add = ""

    for index, row in hasreview_df2.iterrows():
        sbj = row['o']
        obj_rating = list(integer_df['o'].sample(n=1))[0]
        obj_title = list(string_df['o'].sample(n=1))[0]
        obj_text = list(string_df['o'].sample(n=1))[0]
        obj_totalvotes = list(integer_df['o'].sample(n=1))[0]
        obj_user = list(user_df['o'].sample(n=1))[0]
        line2add += f"{sbj} rating {obj_rating}\n{sbj} title {obj_title}\n{sbj} text {obj_text}\n{sbj} totalVotes {obj_totalvotes}\n{sbj} reviewer {obj_user}\n"
        logger.info(f"{sbj} rating {obj_rating}")
        logger.info(f"{sbj} title {obj_title}")
        logger.info(f"{sbj} text {obj_text}")
        logger.info(f"{sbj} totalVotes {obj_totalvotes}")
        logger.info(f"{sbj} reviewer {obj_user}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Offer problem

    # Force

    includes_df = ps.sqldf("select * from df where p == 'includes'")
    logger.debug(includes_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "includes" not in line.strip("\n"):
                output.write(line)

    product_df = df[df['s'].str.contains("Concert|Album|Movie|Book|Product",regex=True)]
    product_df = product_df.drop_duplicates(subset=['s'])
    logger.debug(product_df)

    line2add = ""
        
    for index_product, row_product in product_df.iterrows():
        sbj = list(includes_df['s'].sample(n=1))[0]
        obj_product = row_product['s']
        line2add += f"{sbj} includes {obj_product}\n"
        logger.info(f"{sbj} includes {obj_product}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    offers_df = ps.sqldf("select * from df where p == 'offers'")
    logger.debug(offers_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "offers Offer_" not in line.strip("\n"):
                output.write(line)

    offer_df = includes_df[includes_df['s'].str.contains('^Offer',regex=True)]
    offer_df = offer_df.drop_duplicates(subset=['s'])
    logger.debug(offer_df)

    line2add = ""
        
    for index_offer, row_offer in offer_df.iterrows():
        sbj = list(offers_df['s'].sample(n=1))[0]
        obj_offer = row_offer['s']
        line2add += f"{sbj} offers {obj_offer}\n"
        logger.info(f"{sbj} offers {obj_offer}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Purchase problem

    # Force

    makespurchase_df = ps.sqldf("select * from df where p == 'makesPurchase'")
    logger.debug(makespurchase_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "makesPurchase" not in line.strip("\n"):
                output.write(line)

    purchase_df = df[df['s'].str.contains('^Purchase',regex=True)]
    purchase_df = purchase_df.drop_duplicates(subset=['s'])
    logger.debug(purchase_df)

    line2add = ""
        
    for index_purchase, row_purchase in purchase_df.iterrows():
        sbj = list(makespurchase_df['s'].sample(n=1))[0]
        obj_purchase = row_purchase['s']
        line2add += f"{sbj} makesPurchase {obj_purchase}\n"
        logger.info(f"{sbj} makesPurchase {obj_purchase}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    purchasefor_df = ps.sqldf("select * from df where p == 'purchaseFor'")
    logger.debug(purchasefor_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "purchaseFor" not in line.strip("\n"):
                output.write(line)

    purchaseproduct_df = df[df['s'].str.contains("Concert|Album|Movie|Book|Product",regex=True)]
    logger.debug(purchaseproduct_df)

    line2add = ""
        
    for index, row in purchasefor_df.iterrows():
        sbj = row['s']
        obj_purchase = list(purchaseproduct_df['s'].sample(n=1))[0]
        line2add += f"{sbj} purchaseFor {obj_purchase}\n"
        logger.info(f"{sbj} purchaseFor {obj_purchase}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Product problem

    # Force

    like_df = ps.sqldf("select * from df where p == 'like'")
    logger.debug(like_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "like" not in line.strip("\n"):
                output.write(line)

    likeproduct_df = df[df['s'].str.contains('Concert|Album|Movie|Book|Product',regex=True)]
    logger.debug(likeproduct_df)

    line2add = ""
        
    for index, row in like_df.iterrows():
        sbj = row['s']
        obj_like = list(likeproduct_df['s'].sample(n=1))[0]
        line2add += f"{sbj} like {obj_like}\n"
        logger.info(f"{sbj} like {obj_like}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix other User problem

    # Force

    follows_foaf_df = ps.sqldf("select * from df where p == 'follows' or p == 'friendOf'")
    logger.debug(follows_foaf_df)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "follows" not in line.strip("\n"):
                output.write(line)

    fuser_df = df[df['s'].str.contains('^User',regex=True)]
    logger.debug(fuser_df)

    line2add = ""
        
    for index, row in follows_foaf_df.iterrows():
        sbj = row['s']
        obj_follows = list(fuser_df['s'].sample(n=1))[0]
        obj_friends = list(fuser_df['s'].sample(n=1))[0]
        while obj_follows == sbj:
            obj_follows = list(fuser_df['s'].sample(n=1))[0]
        while obj_friends == sbj:
            obj_friends = list(fuser_df['s'].sample(n=1))[0]
        line2add += f"{sbj} follows {obj_follows}\n{sbj} friendOf {obj_friends}\n"
        logger.info(f"{sbj} follows {obj_follows}")
        logger.info(f"{sbj} friendOf {obj_friends}")

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

if __name__ == "__main__":
    fixator()