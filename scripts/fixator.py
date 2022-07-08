import logging
import warnings
from collections import defaultdict

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
@click.argument("output_file")

def fixator(input_file,output_file):
    df = pd.read_csv(input_file, sep=" ", names=['s','p','o'])

    # TODO : Need to fix problem for partial part (when object are in subject, but is bad construct) [Appaer few time]

    # RUN FIXATOR : python3 ./scripts/fixator.py ./lib/gmark/demo/shop-a/shop-a-graph.txt0.txt ./tmp/tmp.txt

    # Fix SubGenre problem

    # Force

    subgenre_df1 = ps.sqldf("select * from df where p == 'hasGenre'")
    logger.debug("select * from df where p == 'hasGenre'")
    logger.debug(subgenre_df1)

    subgenre_df2 = (subgenre_df1[~subgenre_df1['o'].isin(df['s'])]).dropna()
    subgenre_df2 = subgenre_df2.drop_duplicates(subset=['o'])
    logger.debug("subgenre_df1[~subgenre_df1['o'].isin(df['s'])]")
    logger.debug(subgenre_df2)

    genre_df = df[df['o'].str.contains('^Genre',regex=True)]
    logger.debug("df[df['o'].str.contains('^Genre',regex=True)]")
    logger.debug(genre_df)

    topic_df = df[df['o'].str.contains('^Topic',regex=True)]
    logger.debug("df[df['o'].str.contains('^Topic',regex=True)]")
    logger.debug(topic_df)

    line2add = ""

    for index, row in subgenre_df2.iterrows():
        sbj = row['o']
        obj_genre = list(genre_df['o'].sample(n=1))[0]
        obj_topic = list(topic_df['o'].sample(n=1))[0]
        logger.debug(f"obj_genre : {obj_genre}")
        logger.debug(f"obj_topic : {obj_topic}")
        line2add += f"{sbj} type {obj_genre}\n{sbj} tag {obj_topic}\n"

    with open(output_file, 'w') as wfile:
        with open(input_file, 'r') as rfile:
            rrfile = rfile.read()
            wfile.write(rrfile + line2add)

    # Fix Website problem

    # Force 

    subgenre_df7 = ps.sqldf("select * from df where p == 'homepage' or p == 'trailer' or p == 'subscribes'") # Add subscribes to solve User problem earlier
    logger.debug("select * from df where p == 'homepage' or p == 'trailer' or p == 'subscribes'")
    logger.debug(subgenre_df7)

    subgenre_df8 = (subgenre_df7[~subgenre_df7['o'].isin(df['s'])]).dropna()
    subgenre_df8 = subgenre_df8.drop_duplicates(subset=['o'])
    logger.debug("subgenre_df7[~subgenre_df7['o'].isin(df['s'])]")
    logger.debug(subgenre_df8)

    string_df = df[df['o'].str.contains('^string',regex=True)]
    logger.debug("df[df['o'].str.contains('^string',regex=True)]")
    logger.debug(string_df)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug("df[df['o'].str.contains('^integer',regex=True)]")
    logger.debug(integer_df)

    language_df = df[df['o'].str.contains('^Language',regex=True)]
    logger.debug("df[df['o'].str.contains('^Language',regex=True)]")
    logger.debug(language_df)

    line2add = ""

    for index, row in subgenre_df8.iterrows():
        sbj = row['o']
        obj_string = list(string_df['o'].sample(n=1))[0]
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_language = list(language_df['o'].sample(n=1))[0]
        logger.debug(f"obj_string : {obj_string}")
        logger.debug(f"obj_integer : {obj_integer}")
        logger.debug(f"obj_language : {obj_language}")
        line2add += f"{sbj} url {obj_string}\n{sbj} hits {obj_integer}\n{sbj} language {obj_language}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix User problem

    # Force 

    subgenre_df15 = ps.sqldf("select * from df where p == 'conductor' or p == 'artist' or p == 'actor' or p == 'director' or p == 'author' or p == 'editor' or p == 'contactPoint' or p == 'employee' or p == 'reviewer'") # Add contactPoint, employee and reviewer to solve Retailer and Review problem earlier
    logger.debug("select * from df where p == 'conductor' or p == 'artist' or p == 'actor' or p == 'director' or p == 'author' or p == 'editor' or p == 'contactPoint' or p == 'employee' or p == 'reviewer'")
    logger.debug(subgenre_df15)

    subgenre_df16 = (subgenre_df15[~subgenre_df15['o'].isin(df['s'])]).dropna()
    subgenre_df16 = subgenre_df16.drop_duplicates(subset=['o'])
    logger.debug("subgenre_df15[~subgenre_df15['o'].isin(df['s'])]")
    logger.debug(subgenre_df16)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug("df[df['o'].str.contains('^integer',regex=True)]")
    logger.debug(integer_df)

    website_df = df[df['o'].str.contains('^Website',regex=True)]
    logger.debug("df[df['o'].str.contains('^Website',regex=True)]")
    logger.debug(website_df)

    city_df = df[df['o'].str.contains('^City',regex=True)]
    logger.debug("df[df['o'].str.contains('^City',regex=True)]")
    logger.debug(city_df)

    agegroup_df = df[df['o'].str.contains('^AgeGroup',regex=True)]
    logger.debug("df[df['o'].str.contains('^AgeGroup',regex=True)]")
    logger.debug(agegroup_df)

    gender_df = df[df['o'].str.contains('^Gender',regex=True)]
    logger.debug("df[df['o'].str.contains('^Gender',regex=True)]")
    logger.debug(gender_df)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    logger.debug("df[df['o'].str.contains('^Country',regex=True)]")
    logger.debug(country_df)

    line2add = ""

    for index, row in subgenre_df16.iterrows():
        sbj = row['o']
        obj_integer = list(integer_df['o'].sample(n=1))[0]
        obj_website = list(website_df['o'].sample(n=1))[0]
        obj_city = list(city_df['o'].sample(n=1))[0]
        if not agegroup_df.empty:
            obj_agegroup = list(agegroup_df['o'].sample(n=1))[0]
            line2add += f"{sbj} age {obj_agegroup}\n"
        if not gender_df.empty:
            obj_gender = list(gender_df['o'].sample(n=1))[0]
            line2add += f"{sbj} gender {obj_gender}\n"
        obj_country = list(country_df['o'].sample(n=1))[0]
        logger.debug(f"obj_integer : {obj_integer}")
        logger.debug(f"obj_website : {obj_website}")
        logger.debug(f"obj_city : {obj_city}")
        logger.debug(f"obj_agegroup : {obj_agegroup}")
        logger.debug(f"obj_gender : {obj_gender}")
        logger.debug(f"obj_country : {obj_country}")
        line2add += f"{sbj} userId {obj_integer}\n{sbj} subscribes {obj_website}\n{sbj} location {obj_city}\n{sbj} nationality {obj_country}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix City problem

    # Force

    subgenre_df17 = ps.sqldf("select * from df where p == 'location' or p == 'performedIn'") # Add performedIn to solve Product problem earlier
    logger.debug("select * from df where p == 'location' or p == 'performedIn'")
    logger.debug(subgenre_df17)

    subgenre_df18 = (subgenre_df17[~subgenre_df17['o'].isin(df['s'])]).dropna()
    subgenre_df18 = subgenre_df18.drop_duplicates(subset=['o'])
    logger.debug("subgenre_df17[~subgenre_df17['o'].isin(df['s'])]")
    logger.debug(subgenre_df18)

    country_df = df[df['o'].str.contains('^Country',regex=True)]
    logger.debug("df[df['o'].str.contains('^Country',regex=True)]")
    logger.debug(integer_df)

    line2add = ""

    for index, row in subgenre_df18.iterrows():
        sbj = row['o']
        obj_country = list(country_df['o'].sample(n=1))[0]
        logger.debug(f"obj_country : {obj_country}")
        line2add += f"{sbj} parentCountry {obj_country}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Review problem

    # Force

    subgenre_df19 = ps.sqldf("select * from df where p == 'hasReview'")
    logger.debug("select * from df where p == 'hasReview'")
    logger.debug(subgenre_df19)

    subgenre_df20 = (subgenre_df19[~subgenre_df19['o'].isin(df['s'])]).dropna()
    subgenre_df20 = subgenre_df20.drop_duplicates(subset=['o'])
    logger.debug("subgenre_df19[~subgenre_df19['o'].isin(df['s'])]")
    logger.debug(subgenre_df20)

    integer_df = df[df['o'].str.contains('^integer',regex=True)]
    logger.debug("df[df['o'].str.contains('^integer',regex=True)]")
    logger.debug(integer_df)

    string_df = df[df['o'].str.contains('^string',regex=True)]
    logger.debug("df[df['o'].str.contains('^string',regex=True)]")
    logger.debug(string_df)

    user_df = df[df['o'].str.contains('^User',regex=True)]
    logger.debug("df[df['o'].str.contains('^User',regex=True)]")
    logger.debug(user_df)

    line2add = ""

    for index, row in subgenre_df20.iterrows():
        sbj = row['o']
        obj_rating = list(integer_df['o'].sample(n=1))[0]
        obj_title = list(string_df['o'].sample(n=1))[0]
        obj_text = list(string_df['o'].sample(n=1))[0]
        obj_totalvotes = list(integer_df['o'].sample(n=1))[0]
        obj_user = list(user_df['o'].sample(n=1))[0]
        logger.debug(f"obj_rating : {obj_rating}")
        logger.debug(f"obj_title : {obj_title}")
        logger.debug(f"obj_text : {obj_text}")
        logger.debug(f"obj_totalvotes : {obj_totalvotes}")
        logger.debug(f"obj_user : {obj_user}")
        line2add += f"{sbj} rating {obj_rating}\n{sbj} title {obj_title}\n{sbj} text {obj_text}\n{sbj} totalVotes {obj_totalvotes}\n{sbj} reviewer {obj_user}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Offer problem

    # Force

    subgenre_df22 = ps.sqldf("select * from df where p == 'includes'")
    logger.debug("select * from df where p == 'includes'")
    logger.debug(subgenre_df22)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "includes" not in line.strip("\n"):
                output.write(line)

    product_df = df[df['s'].str.contains("Concert|Album|Movie|Book|Product",regex=True)]
    product_df = product_df.drop_duplicates(subset=['s'])
    logger.debug("df[df['s'].str.contains(\"Concert|Album|Movie|Book|Product\",regex=True)]")
    logger.debug(product_df)

    line2add = ""
        
    for index_product, row_product in product_df.iterrows():
        sbj = list(subgenre_df22['s'].sample(n=1))[0]
        obj_product = row_product['s']
        logger.debug(f"obj_product : {obj_product}")
        line2add += f"{sbj} includes {obj_product}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    subgenre_df21 = ps.sqldf("select * from df where p == 'offers'")
    logger.debug("select * from df where p == 'offers'")
    logger.debug(subgenre_df21)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "offers Offer_" not in line.strip("\n"):
                output.write(line)

    offer_df = subgenre_df22[subgenre_df22['s'].str.contains('^Offer',regex=True)]
    offer_df = offer_df.drop_duplicates(subset=['s'])
    logger.debug("subgenre_df22[subgenre_df22['s'].str.contains('^Offer',regex=True)]")
    logger.debug(offer_df)

    line2add = ""
        
    for index_offer, row_offer in offer_df.iterrows():
        sbj = list(subgenre_df21['s'].sample(n=1))[0]
        obj_offer = row_offer['s']
        logger.debug(f"obj_offer : {obj_offer}")
        line2add += f"{sbj} offers {obj_offer}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Purchase problem

    # Force

    subgenre_df24 = ps.sqldf("select * from df where p == 'makesPurchase'")
    logger.debug("select * from df where p == 'makesPurchase'")
    logger.debug(subgenre_df24)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "makesPurchase" not in line.strip("\n"):
                output.write(line)

    purchase_df = df[df['s'].str.contains('^Purchase',regex=True)]
    purchase_df = purchase_df.drop_duplicates(subset=['s'])
    logger.debug("df[df['s'].str.contains('^Purchase',regex=True)]")
    logger.debug(purchase_df)

    line2add = ""
        
    for index_purchase, row_purchase in purchase_df.iterrows():
        sbj = list(subgenre_df24['s'].sample(n=1))[0]
        obj_purchase = row_purchase['s']
        logger.debug(f"obj_purchase : {obj_purchase}")
        line2add += f"{sbj} makesPurchase {obj_purchase}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    subgenre_df23 = ps.sqldf("select * from df where p == 'purchaseFor'")
    logger.debug("select * from df where p == 'purchaseFor'")
    logger.debug(subgenre_df23)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "purchaseFor" not in line.strip("\n"):
                output.write(line)

    purchaseproduct_df = df[df['s'].str.contains("Concert|Album|Movie|Book|Product",regex=True)]
    logger.debug("df[df['s'].str.contains(\"Concert|Album|Movie|Book|Product\",regex=True)]")
    logger.debug(purchaseproduct_df)

    line2add = ""
        
    for index, row in subgenre_df23.iterrows():
        sbj = row['s']
        obj_purchase = list(purchaseproduct_df['s'].sample(n=1))[0]
        logger.debug(f"obj_purchase : {obj_purchase}")
        line2add += f"{sbj} purchaseFor {obj_purchase}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix Product problem

    # Force

    subgenre_df25 = ps.sqldf("select * from df where p == 'like'")
    logger.debug("select * from df where p == 'like'")
    logger.debug(subgenre_df25)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "like" not in line.strip("\n"):
                output.write(line)

    likeproduct_df = df[df['s'].str.contains('Concert|Album|Movie|Book|Product',regex=True)]
    logger.debug("df[df['s'].str.contains('Concert|Album|Movie|Book|Product',regex=True)]")
    logger.debug(likeproduct_df)

    line2add = ""
        
    for index, row in subgenre_df25.iterrows():
        sbj = row['s']
        obj_like = list(likeproduct_df['s'].sample(n=1))[0]
        logger.debug(f"obj_like : {obj_like}")
        line2add += f"{sbj} like {obj_like}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix other User problem

    # Force

    subgenre_df26 = ps.sqldf("select * from df where p == 'follows' or p == 'friendOf'")
    logger.debug("select * from df where p == 'follows' or p == 'friendOf'")
    logger.debug(subgenre_df26)

    lines = []

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "follows" not in line.strip("\n"):
                output.write(line)

    fuser_df = df[df['s'].str.contains('^User',regex=True)]
    logger.debug("df[df['s'].str.contains('^User',regex=True)]")
    logger.debug(fuser_df)

    line2add = ""
        
    for index, row in subgenre_df26.iterrows():
        sbj = row['s']
        obj_follows = list(fuser_df['s'].sample(n=1))[0]
        obj_friends = list(fuser_df['s'].sample(n=1))[0]
        while obj_follows == sbj:
            obj_follows = list(fuser_df['s'].sample(n=1))[0]
        while obj_friends == sbj:
            obj_friends = list(fuser_df['s'].sample(n=1))[0]
        logger.debug(f"obj_follows : {obj_follows}")
        logger.debug(f"obj_friends : {obj_friends}")
        line2add += f"{sbj} follows {obj_follows}\n{sbj} friendOf {obj_friends}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Fix sameAs problem

    df_output = pd.read_csv(output_file, sep=" ", names=['s','p','o'])

    lines = []

    max_chance = 0

    with open(output_file, 'r') as input:
        lines = input.readlines()

    with open(output_file, 'w') as output:
        for line in lines:
            if "sameAs" not in line.strip("\n"):
                output.write(line)

    # Topic and HotTopic

    subgenre_df_output27 = ps.sqldf("select * from df_output where p == 'sameAs'")
    logger.debug("select * from df_output where p == 'sameAs'")
    logger.debug(subgenre_df_output27)

    subgenre_df_output28 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('Topic',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('Topic',regex=True)]")
    logger.debug(subgenre_df_output28)

    topic_df_output = df_output[df_output['o'].str.contains('Topic',regex=True)]
    logger.debug("df_output[df_output['o'].str.contains('Topic',regex=True)]")
    logger.debug(topic_df_output)

    line2add = ""
        
    for index, row in subgenre_df_output28.iterrows():
        sbj_topic = list(topic_df_output['o'].sample(n=1))[0]
        obj_topic = list(topic_df_output['o'].sample(n=1))[0]
        while obj_topic == sbj_topic:
            obj_topic = list(topic_df_output['o'].sample(n=1))[0]
        logger.debug(f"obj_topic : {obj_topic}")
        line2add += f"{sbj_topic} sameAs {obj_topic}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Product

    # Concert

    # Classical

    subgenre_df_output29 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicalMusicConcert',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicalMusicConcert',regex=True)]")
    logger.debug(subgenre_df_output29)

    retailer_df_output = df_output[(df_output['s'].str.contains('^Retailer',regex=True)) & (df_output['p'].str.contains('^offers',regex=True))]
    logger.debug("df_output[df_output['s'].str.contains('^Retailer',regex=True)]")
    logger.debug(retailer_df_output)

    line2add = ""

    for index, row in subgenre_df_output29.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicalMusicConcert'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicalMusicConcert'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicalMusicConcert'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicalMusicConcert'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Normal

    subgenre_df_output30 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('^MusicConcert',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('^MusicConcert',regex=True)]")
    logger.debug(subgenre_df_output30)

    line2add = ""

    for index, row in subgenre_df_output30.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^MusicConcert'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^MusicConcert'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^MusicConcert'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^MusicConcert'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Hit

    subgenre_df_output31 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('HitMusicConcert',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('HitMusicConcert',regex=True)]")
    logger.debug(subgenre_df_output31)

    line2add = ""

    for index, row in subgenre_df_output31.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'HitMusicConcert'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'HitMusicConcert'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'HitMusicConcert'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'HitMusicConcert'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Album

    # Classic

    subgenre_df_output32 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicMusicAlbum',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicMusicAlbum',regex=True)]")
    logger.debug(subgenre_df_output32)

    line2add = ""

    for index, row in subgenre_df_output32.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicMusicAlbum'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicMusicAlbum'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicMusicAlbum'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicMusicAlbum'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Normal

    subgenre_df_output33 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('^MusicAlbum',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('^MusicAlbum',regex=True)]")
    logger.debug(subgenre_df_output33)

    line2add = ""

    for index, row in subgenre_df_output33.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^MusicAlbum'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^MusicAlbum'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^MusicAlbum'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^MusicAlbum'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Hit

    subgenre_df_output34 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('HitMusicAlbum',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('HitMusicAlbum',regex=True)]")
    logger.debug(subgenre_df_output34)

    line2add = ""

    for index, row in subgenre_df_output34.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'HitMusicAlbum'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'HitMusicAlbum'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'HitMusicAlbum'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'HitMusicAlbum'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Movie

    # Normal

    subgenre_df_output35 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('^Movie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('^Movie',regex=True)]")
    logger.debug(subgenre_df_output35)

    line2add = ""

    for index, row in subgenre_df_output35.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^Movie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^Movie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^Movie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^Movie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Classic

    subgenre_df_output36 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicMovie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('ClassicMovie',regex=True)]")
    logger.debug(subgenre_df_output36)

    line2add = ""

    for index, row in subgenre_df_output36.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicMovie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicMovie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ClassicMovie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ClassicMovie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Top

    subgenre_df_output37 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('TopMovie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('TopMovie',regex=True)]")
    logger.debug(subgenre_df_output37)

    line2add = ""

    for index, row in subgenre_df_output37.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'TopMovie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'TopMovie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'TopMovie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'TopMovie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Animated

    subgenre_df_output38 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('AnimatedMovie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('AnimatedMovie',regex=True)]")
    logger.debug(subgenre_df_output38)

    line2add = ""

    for index, row in subgenre_df_output38.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'AnimatedMovie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'AnimatedMovie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'AnimatedMovie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'AnimatedMovie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Long

    subgenre_df_output39 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('LongMovie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('LongMovie',regex=True)]")
    logger.debug(subgenre_df_output39)

    line2add = ""

    for index, row in subgenre_df_output39.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'LongMovie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'LongMovie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'LongMovie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'LongMovie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Short

    subgenre_df_output40 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('ShortMovie',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('ShortMovie',regex=True)]")
    logger.debug(subgenre_df_output40)

    line2add = ""

    for index, row in subgenre_df_output40.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ShortMovie'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ShortMovie'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ShortMovie'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ShortMovie'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Book

    # Normal

    subgenre_df_output41 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('^Book',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('^Book',regex=True)]")
    logger.debug(subgenre_df_output41)

    line2add = ""

    for index, row in subgenre_df_output41.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^Book'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^Book'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^Book'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^Book'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Book

    # NewsArticle

    subgenre_df_output42 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('NewsArticleBook',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('NewsArticleBook',regex=True)]")
    logger.debug(subgenre_df_output42)

    line2add = ""

    for index, row in subgenre_df_output42.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'NewsArticleBook'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'NewsArticleBook'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'NewsArticleBook'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'NewsArticleBook'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Article

    subgenre_df_output43 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('^ArticleBook',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('^ArticleBook',regex=True)]")
    logger.debug(subgenre_df_output43)

    line2add = ""

    for index, row in subgenre_df_output43.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^ArticleBook'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^ArticleBook'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp '^ArticleBook'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp '^ArticleBook'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Novel

    subgenre_df_output44 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('NovelBook',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('NovelBook',regex=True)]")
    logger.debug(subgenre_df_output44)

    line2add = ""

    for index, row in subgenre_df_output44.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'NovelBook'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'NovelBook'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'NovelBook'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'NovelBook'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # ShortStory

    subgenre_df_output45 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('ShortStoryBook',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('ShortStoryBook',regex=True)]")
    logger.debug(subgenre_df_output45)

    line2add = ""

    for index, row in subgenre_df_output45.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ShortStoryBook'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ShortStoryBook'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'ShortStoryBook'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'ShortStoryBook'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Encyclopedia

    subgenre_df_output46 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('EncyclopediaBook',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('EncyclopediaBook',regex=True)]")
    logger.debug(subgenre_df_output46)

    line2add = ""

    for index, row in subgenre_df_output46.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'EncyclopediaBook'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'EncyclopediaBook'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'EncyclopediaBook'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'EncyclopediaBook'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

    # Other

    subgenre_df_output47 = subgenre_df_output27[subgenre_df_output27['o'].str.contains('Product',regex=True)]
    logger.debug("subgenre_df_output27[subgenre_df_output27['o'].str.contains('Product',regex=True)]")
    logger.debug(subgenre_df_output47)

    line2add = ""

    for index, row in subgenre_df_output47.iterrows():
        retailer_1 = list(retailer_df_output['s'].sample(n=1))[0]
        retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]
        logger.debug(f"retailer_1 : {retailer_1}")
        while retailer_2 == retailer_1:
            retailer_2 = list(retailer_df_output['s'].sample(n=1))[0]       
        logger.debug(f"retailer_2 : {retailer_2}")
        subquery_df_output1 = ps.sqldf(f"select * from df_output where s == '{retailer_1}' and p == 'offers'") # Offer for Retailer 1
        logger.debug("select * from df_output where s == '{retailer_1}' and p == 'offers'")
        logger.debug(subquery_df_output1)
        subquery_df_output2 = ps.sqldf(f"select * from df_output where s == '{retailer_2}' and p == 'offers'") # Offer for Retailer 2
        logger.debug("select * from df_output where s == '{retailer_2}' and p == 'offers'")
        logger.debug(subquery_df_output2)
        offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
        offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
        subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'Product'")
        subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'Product'")
        i = 0
        while (subquery_df_output11.empty or subquery_df_output12.empty) and i < max_chance:
            offer_1 = list(subquery_df_output1['o'].sample(n=1))[0]
            offer_2 = list(subquery_df_output2['o'].sample(n=1))[0]
            subquery_df_output11 = ps.sqldf(f"select * from df_output where s == '{offer_1}' and p == 'includes' and o regexp 'Product'")
            subquery_df_output12 = ps.sqldf(f"select * from df_output where s == '{offer_2}' and p == 'includes' and o regexp 'Product'")
            i += 1             
            logger.debug(f"{i}/{max_chance} to find different offer")
        if i == max_chance:
            continue
        logger.debug(f"offer_1 : {offer_1}")
        logger.debug(f"offer_2 : {offer_2}")
        product_1 = list(subquery_df_output11['o'].sample(n=1))[0]
        product_2 = list(subquery_df_output12['o'].sample(n=1))[0]
        logger.debug(f"obj_product_1 : {product_1}")
        logger.debug(f"obj_product_2 : {product_2}")
        line2add += f"{product_1} sameAs {product_2}\n"

    with open(output_file, 'a') as wfile:
        wfile.write(line2add)

if __name__ == "__main__":
    fixator()