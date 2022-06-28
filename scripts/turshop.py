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

warnings.simplefilter(action='ignore', category=FutureWarning)

coloredlogs.install(level='DEBUG', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("input_folder")
@click.argument("output")

def convert(input_folder, output):

    files = glob.glob(f'{input_folder}/*.txt')
    nb_site = len(files)

    dfs = dict()
    for file in files:
        df = pd.read_csv(file,sep=" ",names=['s','p','o'])
        
        site = os.path.basename(file).split('-')[1].split('.')[0]
        df["site"] = site
        dfs[site] = df
    
    
    result_df = pd.concat(dfs.values())
    result_df = result_df[~result_df['p'].isin(['sameAs'])]
    logger.debug("Result df " + str(result_df))
    result_df = result_df.reset_index().drop(["index"], axis=1)
    
    
    result_df = join_literal(result_df)
    result_df = add_predicates(result_df)
    logger.debug("Tail " + str(result_df.tail(5)))


    result_df["sNq"] = result_df.apply(lambda l: subject(l), axis=1)
    result_df["pNq"] = result_df.apply(lambda l: predicate(l), axis=1)
    result_df["oNq"] = result_df.apply(lambda l: objecte(l), axis=1)
    result_df["gNq"] = result_df.apply(lambda l: "<http://example.org/s" + str(l["site"]) + ">", axis=1)
    
    result = generate_nq(result_df)


    with open(f'{output}', 'w') as nqfile:
        nqfile.write(result)
    

def subject(line):
    subject = str(line['s'])
    site = str(line['site'])
    if "#" in subject:
        logger.debug("Found # in " + subject)
        site = subject.split('#')[0]
        subject = subject.split('#')[1]
        
    return "<http://example.org/s" + str(int(site)) + "/" + str(subject) + ">"

def predicate(line):
    if line['p'].startswith('http://'):
        return "<" + line['p'] + ">"
    else:
        predicate = line['p']
        predicate = "<http://example.org/" + str(predicate) + ">"
        return predicate

def objecte(line):
    go2 = str(line['o']).split('_')[0]
    objecte = str(line['o'])
    site = str(line['site'])
    if "#" in objecte:
        logger.debug("Found # in " + str(objecte))
        site = objecte.split('#')[0]
        objecte = objecte.split('#')[1]
        
    if str(go2) in ["string", "integer", "date"] :
        if str(go2) in ["string"]:
            objecte = "\"" + str(objecte) + "\""
        else:
            objecte = str(objecte)
    else:
        objecte = "<http://example.org/s" + str(int(site)) + "/" + str(objecte) + ">"
    return objecte
    
def generate_nq(df: pd.DataFrame) -> str:
    data = df[["sNq","pNq","oNq","gNq"]]
   
    rdf = ""
    for index, row in data.iterrows():
        rdf += f'{row["sNq"]} {row["pNq"]} {row["oNq"]} {row["gNq"]} . \n'
        
   
    return rdf

def add_predicates(df:pd.DataFrame) -> pd.DataFrame:
    df_new_quad = pd.DataFrame(columns=df.columns)
    for predicate in list(configuration["predicate"].keys()):
        logger.debug(predicate)
        osite = set()
        ssite = set()
        for source_yaml in (configuration["predicate"].get(predicate)):
            source = (list(source_yaml.keys())[0])
            osite.add(source.replace('s',''))
            for target_str in source_yaml:
                logger.debug("target_str = "+ target_str)
                if(target_str == "default"):
                    # Generic default (unknown source & target)
                    for target_yaml in source_yaml.get(target_str):
                        others_site = set(df["site"].unique()) - osite
                        for other_site_origin in others_site:
                            for other_site_target in set(df["site"].unique()):
                                for target_site in source_yaml.get(target_str):
                                    logger.debug(str(target_site))
                                    logger.debug(str(target_str))
                                    logger.debug(str(target_site.get("proba")))
                                    proba = float(target_site.get("proba"))
                                    random_number = random.random()
                                    if proba > random_number:
                                        logger.debug(str(proba)+  str(random_number))
                                        logger.debug("target_info = "+ str(target_site))
                            
                                        df_type_src = df.loc[df['s'].str.contains(target_site.get("source_type"))]
                                        logger.debug("df_type_src = "+ df_type_src)
                                        df_type_targ = df.loc[df['s'].str.contains(target_site.get("target_type"))]
                                        logger.debug("df_type_targ = "+ df_type_targ)
                                        site_src = int(other_site_origin)
                                        site_targ = int(other_site_target)
                                        res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                        res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]


                                        n_times = random.randint(target_site.get("min"),target_site.get("max"))
                                        logger.debug("n_times = "+ str(n_times))
                                        for i in range(0,n_times):
                                            entity1 = np.random.choice(res_src,1)[0]
                                            entity2 = np.random.choice(res_targ,1)[0]
                                            while (entity1 == entity2) and (site_src == site_targ):
                                                entity1 = np.random.choice(res_src,1)[0]
                                                entity2 = np.random.choice(res_targ,1)[0]
                                                
                                            logger.info(f"Adding {predicate} from s{site_src}#{entity1} to s{site_targ}#{entity2}")
                                            df_new_quad = df_new_quad.append({'s':str(site_src) + "#" + str(entity1), 'p':predicate, 'o':str(site_targ) + "#" + str(entity2), 'site':site_src}, ignore_index=True) 
                                            logger.debug("--------------")

                    logger.debug("Found default")
                else:
                
                    for target_yaml in source_yaml.get(target_str):
                        for target_site in (target_yaml["target"]):
                            logger.debug("target_site ="+ str(target_site))
                            trg_site = list(target_site.keys())[0]
                            logger.debug("trg_site = " + str(trg_site))
                            if(trg_site == "default"):
                                logger.debug("Found default")
                                # Specific default (unkown target)
                                others_site = set(df["site"].unique()) - ssite
                                for other_site in others_site:
                                    proba = float(target_site.get(trg_site).get("proba"))
                                    random_number = random.random()
                                    if proba > random_number:
                                        logger.debug(str(proba)+  str(random_number))
                                        logger.debug("target_info = "+ str(target_site))
                            
                                        df_type_src = df.loc[df['s'].str.contains(target_site.get(trg_site).get("source_type"))]
                                        logger.debug("df_type_src = "+ df_type_src)
                                        df_type_targ = df.loc[df['s'].str.contains(target_site.get(trg_site).get("target_type"))]
                                        logger.debug("df_type_targ = "+ df_type_targ)
                                        site_src = int(source.replace("s",""))
                                        site_targ = int(other_site)
                                        res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                        res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]


                                        n_times = random.randint(target_site.get(trg_site).get("min"),target_site.get(trg_site).get("max"))
                                        logger.debug("n_times = "+ str(n_times))
                                        for i in range(0,n_times):
                                            entity1 = np.random.choice(res_src,1)[0]
                                            entity2 = np.random.choice(res_targ,1)[0]
                                            while (entity1 == entity2) and (site_src == site_targ):
                                                entity1 = np.random.choice(res_src,1)[0]
                                                entity2 = np.random.choice(res_targ,1)[0]
                                                
                                            logger.info(f"Adding {predicate} from s{site_src}#{entity1} to s{site_targ}#{entity2}")
                                            df_new_quad = df_new_quad.append({'s':str(site_src) + "#" + str(entity1), 'p':predicate, 'o':str(site_targ) + "#" + str(entity2), 'site':site_src}, ignore_index=True) 
                                            logger.debug("--------------")
                            else:
                                ssite.add(trg_site.replace('s',''))

                                logger.debug("target_info = "+ str(target_site))
                                
                                df_type_src = df.loc[df['s'].str.contains(target_site.get(trg_site).get("source_type"))]
                                logger.debug("df_type_src = "+ df_type_src)
                                df_type_targ = df.loc[df['s'].str.contains(target_site.get(trg_site).get("target_type"))]
                                logger.debug("df_type_targ = "+ df_type_targ)
                                site_src = int(source.replace("s",""))
                                site_targ = int(trg_site.replace("s",""))
                                res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]


                                n_times = random.randint(target_site.get(trg_site).get("min"),target_site.get(trg_site).get("max"))
                                logger.debug("n_times = "+ str(n_times))
                                for i in range(0,n_times):
                                    entity1 = np.random.choice(res_src,1)[0]
                                    entity2 = np.random.choice(res_targ,1)[0]
                                    while (entity1 == entity2) and (site_src == site_targ):
                                        entity1 = np.random.choice(res_src,1)[0]
                                        entity2 = np.random.choice(res_targ,1)[0]
                                        
                                    logger.info(f"Adding {predicate} from s{site_src}#{entity1} to s{site_targ}#{entity2}")
                                    df_new_quad = df_new_quad.append({'s':str(site_src) + "#" + str(entity1), 'p':predicate, 'o':str(site_targ) + "#" + str(entity2), 'site':site_src}, ignore_index=True) 
                                    logger.debug("--------------")
                    
    
    logger.info(df_new_quad.shape)
    return pd.concat([df, df_new_quad],ignore_index=True, sort=False)

def join_literal(df:pd.DataFrame) -> pd.DataFrame:
    for literal in list(configuration["literal"].keys()):
        logger.debug(literal)
        osite = set()
        ssite = set()
        for source_yaml in (configuration["literal"].get(literal)):
            source = (list(source_yaml.keys())[0])
            osite.add(source.replace('s',''))
            for target_str in source_yaml:
                logger.debug("target_str = "+ target_str)
                if(target_str == "default"):
                    # Generic default (unknown source & target)
                    for target_yaml in source_yaml.get(target_str):
                        others_site = set(df["site"].unique()) - osite
                        for other_site_origin in others_site:
                            for other_site_target in set(df["site"].unique()):
                                for target_site in source_yaml.get(target_str):
                                    logger.debug(str(target_site))
                                    logger.debug(str(target_str))
                                    logger.debug(str(target_site.get("proba")))
                                    proba = float(target_site.get("proba"))
                                    random_number = random.random()
                                    if proba > random_number:
                                        logger.debug(str(proba)+  str(random_number))
                                        logger.debug("target_info = "+ str(target_site))
                            
                                        #df_type_src = df.loc[df['s'].str.contains(target_site.get("source_type"))]
                                        df_type_src = df.loc[(df['s'].str.contains(target_site.get("source_type"))) & (df['p'] == literal.split('/')[3])]
                                        logger.debug("df_type_src = "+ df_type_src)
                                        #df_type_targ = df.loc[df['o'].str.contains(target_site.get("target_type"))]
                                        df_type_targ = df.loc[(df['o'].str.contains(target_site.get("target_type"))) & df['s'].str.contains(target_site.get("source_type")) & (df['p'] == literal.split('/')[3])]
                                        logger.debug("df_type_targ = "+ df_type_targ)
                                        site_src = int(other_site_origin)
                                        site_targ = int(other_site_target)
                                        res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                        res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]


                                        n_times = random.randint(target_site.get("min"),target_site.get("max"))
                                        logger.debug("n_times = "+ str(n_times))
                                        for i in range(0,n_times):
                                            entity1 = np.random.choice(res_src,1)[0]
                                            entity2 = np.random.choice(res_targ,1)[0]
                                            while (entity1 == entity2) and (site_src == site_targ):
                                                entity1 = np.random.choice(res_src,1)[0]
                                                entity2 = np.random.choice(res_targ,1)[0]
                                               
                                               
                                            logger.debug("entity1 : " + entity1)
                                            logger.debug("site : " + str(site_src))
                                            logger.debug("literal : " + literal.split('/')[3])
                                            lit1 = (df[(df['s'] == entity1) & (df['site'] == str(site_src)) & (df['p'] == literal.split('/')[3])].iloc[0]["o"])
                                            df.loc[(df['s'] == entity2) & (df['site'] == str(site_targ)) & (df['p'] == literal.split('/')[3]),'o'] = lit1
                                    
                                            logger.info(f'Joining {literal} from s{site_src}#{entity1} ({lit1}) to s{site_targ}#{entity2}')

                    logger.debug("Found default")
                else:
                
                    for target_yaml in source_yaml.get(target_str):
                        for target_site in (target_yaml["target"]):
                            logger.debug("target_site ="+ str(target_site))
                            trg_site = list(target_site.keys())[0]
                            logger.debug("trg_site = " + str(trg_site))
                            if(trg_site == "default"):
                                logger.debug("Found default")
                                # Specific default (unkown target)
                                others_site = set(df["site"].unique()) - ssite
                                for other_site in others_site:
                                    proba = float(target_site.get(trg_site).get("proba"))
                                    random_number = random.random()
                                    if proba > random_number:
                                        logger.debug(str(proba)+  str(random_number))
                                        logger.debug("target_info = "+ str(target_site))
                            
                                        #df_type_src = df.loc[df['s'].str.contains(target_site.get(trg_site).get("source_type"))]
                                        df_type_src = df.loc[(df['s'].str.contains(target_site.get(trg_site).get("source_type"))) & (df['p'] == literal.split('/')[3])]
                                        logger.debug("df_type_src = "+ df_type_src)
                                        #df_type_targ = df.loc[df['o'].str.contains(target_site.get(trg_site).get("target_type"))]
                                        df_type_targ = df.loc[(df['o'].str.contains(target_site.get(trg_site).get("target_type"))) & df['s'].str.contains(target_site.get(trg_site).get("source_type")) & (df['p'] == literal.split('/')[3])]
                                        logger.debug("df_type_targ = "+ df_type_targ)
                                        site_src = int(source.replace("s",""))
                                        site_targ = int(other_site)
                                        res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                        res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]


                                        n_times = random.randint(target_site.get(trg_site).get("min"),target_site.get(trg_site).get("max"))
                                        logger.debug("n_times = "+ str(n_times))
                                        for i in range(0,n_times):
                                            entity1 = np.random.choice(res_src,1)[0]
                                            entity2 = np.random.choice(res_targ,1)[0]
                                            while (entity1 == entity2) and (site_src == site_targ):
                                                entity1 = np.random.choice(res_src,1)[0]
                                                entity2 = np.random.choice(res_targ,1)[0]
                                                
                                            logger.debug("entity1 : " + entity1)
                                            logger.debug("site : " + str(site_src))
                                            logger.debug("literal : " + literal.split('/')[3])
                                            lit1 = (df[(df['s'] == entity1) & (df['site'] == str(site_src)) & (df['p'] == literal.split('/')[3])].iloc[0]["o"])
                                            df.loc[(df['s'] == entity2) & (df['site'] == str(site_targ)) & (df['p'] == literal.split('/')[3]),'o'] = lit1
                                    
                                            logger.info(f'Joining {literal} from s{site_src}#{entity1} ({lit1}) to s{site_targ}#{entity2}')
                            else:
                                ssite.add(trg_site.replace('s',''))

                                logger.debug("target_info = "+ str(target_site))
                                
                                
                                logger.debug('target_site.get(trg_site).get("source_type")) = ' + str(target_site.get(trg_site).get("source_type")))
                                df_type_src = df.loc[(df['s'].str.contains(target_site.get(trg_site).get("source_type"))) & (df['p'] == literal.split('/')[3])]
                                logger.debug("df_type_src = "+ df_type_src)
                                logger.debug('target_site.get(trg_site).get("target_type")) = ' + str(target_site.get(trg_site).get("target_type")))
                                df_type_targ = df.loc[(df['o'].str.contains(target_site.get(trg_site).get("target_type"))) & df['s'].str.contains(target_site.get(trg_site).get("source_type")) & (df['p'] == literal.split('/')[3])]
                                logger.debug("df_type_targ = "+ df_type_targ)
                                site_src = int(source.replace("s",""))
                                site_targ = int(trg_site.replace("s",""))
                                res_src = df_type_src.groupby('site')['s'].unique()[site_src]
                                res_targ = df_type_targ.groupby('site')['s'].unique()[site_targ]
                                logger.debug("res_src = ")
                                logger.debug(str(res_src))
                                logger.debug("res_targ = ")
                                logger.debug(str(res_targ))

                                n_times = random.randint(target_site.get(trg_site).get("min"),target_site.get(trg_site).get("max"))
                                logger.debug("n_times = "+ str(n_times))
                                for i in range(0,n_times):
                                    entity1 = np.random.choice(res_src,1)[0]
                                    entity2 = np.random.choice(res_targ,1)[0]
                                    while (entity1 == entity2) and (site_src == site_targ):
                                        entity1 = np.random.choice(res_src,1)[0]
                                        entity2 = np.random.choice(res_targ,1)[0]
                                        
                                    logger.debug("entity1 : " + entity1)
                                    logger.debug("site : " + str(site_src))
                                    logger.debug("literal : " + literal.split('/')[3])
                                    lit1 = (df[(df['s'] == entity1) & (df['site'] == str(site_src)) & (df['p'] == literal.split('/')[3])].iloc[0]["o"])
                                    df.loc[(df['s'] == entity2) & (df['site'] == str(site_targ)) & (df['p'] == literal.split('/')[3]),'o'] = lit1
                                    
                                    logger.info(f'Joining {literal} from s{site_src}#{entity1} ({lit1}) to s{site_targ}#{entity2}')
                    
    
    logger.info(df.shape)
    return df
    
if __name__ == "__main__":
    convert()