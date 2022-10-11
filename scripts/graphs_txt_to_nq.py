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

# Goal : correctly convert txt data into nq data

warnings.simplefilter(action='ignore', category=FutureWarning)

coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("input_folder")
@click.argument("output")

def graphs_txt_to_nq(input_folder, output):

    logger.debug(input_folder)

    files = glob.glob(f'{input_folder}/*.txt')
    nb_site = len(files)
    logger.debug(files)

    dfs = dict()
    for file in files:
        df = pd.read_csv(file,sep=" ",names=['s','p','o'])
        
        site = os.path.basename(file).split('-')[1].split('.')[0]
        df["site"] = site
        dfs[site] = df
    
    logger.debug(dfs)
    
    result_df = pd.concat(dfs.values())
    result_df = result_df[~result_df['p'].isin(['sameAs'])]
    logger.debug("Result df " + str(result_df))
    result_df = result_df.reset_index().drop(["index"], axis=1)
    
    logger.debug("Tail " + str(result_df.tail(5)))

    result_df = add_federated_shop(result_df)
    result_df = add_types(result_df)

    result_df["sNq"] = result_df.apply(lambda l: subject(l), axis=1)
    result_df["pNq"] = result_df.apply(lambda l: predicate(l), axis=1)
    result_df["oNq"] = result_df.apply(lambda l: objecte(l), axis=1)
    result_df["gNq"] = result_df.apply(lambda l: "<http://example.org/s" + str(l["site"]) + ">", axis=1)
    
    result = generate_nq(result_df)


    with open(f'{output}', 'w') as nqfile:
        nqfile.write(result)   

def subject(line):
    subject = str(line['s'])
    if subject.startswith('<'):
        return subject


    site = str(line['site'])
    if "#" in subject:
        logger.debug("Found # in " + subject)
        site = subject.split('#')[0]
        subject = subject.split('#')[1]
        
    subject_split = subject.split("_")
    subject = str(subject_split[0] + "_s" + site +"_" + subject_split[1])
    return "<http://example.org/s" + str(int(site)) + "/" + str(subject) + ">"

def predicate(line):
    if line['p'].startswith('<http://'):
        return line['p']
    else:
        predicate = line['p']
        predicate = "<http://example.org/" + str(predicate) + ">"
        return predicate

def objecte(line):
    go2 = str(line['o']).split('_')[0]
    objecte = str(line['o'])
    if objecte.startswith('<'):
        return objecte
    site = str(line['site'])
    if "#" in objecte:
        logger.debug("Found # in " + str(objecte))
        site = objecte.split('#')[0]
        objecte = objecte.split('#')[1]
        
    if str(go2) in ["string", "integer", "date"] :
        if str(go2) in ["string"]:
            objecte = "\"" + str(objecte) + "\""
            logger.debug(f"String found {objecte}")
        else:
            if str(go2) in ["integer","date"]:
                logger.debug(str(go2))
                objecte = str(objecte).replace(go2 + "_","")
            else:
                objecte = str(objecte)

    else:
        objecte_split = str(objecte).split("_")
        objecte = str(objecte_split[0] + "_s" + site +"_" + objecte_split[1])
        logger.debug(objecte)
        objecte = "<http://example.org/s" + str(int(site)) + "/" + str(objecte) + ">"
    return objecte   

def add_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(df)

    done_entites = set()
    for index, row in df.iterrows():
        if row["o"] in ["integer","string","date"]:
            if row["o"] not in done_entites:
                df.loc[len(df)] = [row["o"],"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>","<http://example.org/federated_shop/" + row["o"].split("_")[0] + ">", row["site"]]
                done_entites.add(row["o"])
                logger.debug(f"Add rdf:type on  {row['o']} ({index})")
        
        if row["s"] not in done_entites:
                df.loc[len(df)] = [row["s"],"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>","<http://example.org/federated_shop/" + row["s"].split("_")[0] + ">", row["site"]]
                done_entites.add(row["s"])
                logger.debug(f"Add rdf:type on  {row['s']} ({index})")

    return df

def generate_nq(df: pd.DataFrame) -> str:
    data = df[["sNq","pNq","oNq","gNq"]]
   
    rdf = ""
    for index, row in data.iterrows():
        rdf += f'{row["sNq"]} {row["pNq"]} {row["oNq"]} {row["gNq"]} .\n'        
   
    return rdf

def add_federated_shop(df: pd.DataFrame) -> pd.DataFrame:
    for idx, row in df.iterrows():
        subject = row["s"]
        object = row["o"]
        
        type_subject = row["s"].split("_")[0]
        type_object = row["o"].split("_")[0]

        if type_subject in configuration["shared_types"]:
            new_subject = "<http://example.org/federated_shop/" + subject + ">"
            df.loc[idx] = [new_subject, row["p"], row["o"], row["site"]]
            logger.debug(idx)

        if type_object in configuration["shared_types"]:
            new_object = "<http://example.org/federated_shop/" + object + ">"
            df.loc[idx] = [row["s"], row["p"], new_object, row["site"]]
            logger.debug(idx)

    return df
    
if __name__ == "__main__":
    graphs_txt_to_nq()