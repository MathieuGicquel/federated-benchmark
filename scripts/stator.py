import glob
import click
import pandas as pd
import yaml
import logging
import coloredlogs
import warnings 
from collections import defaultdict
from yaml.representer import Representer

yaml.add_representer(defaultdict, Representer.represent_dict)

warnings.simplefilter(action='ignore', category=FutureWarning)

coloredlogs.install(level='DEBUG', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def stator(input_file,output_file):
    nb_site = int(configuration["site"])

    df = pd.read_csv(input_file,sep="> <",names=['s','p','o','g'])

    dict_type = defaultdict(lambda: defaultdict(set))

    dict_sameas_in = defaultdict(int)
    dict_sameas_out = defaultdict(int)

    for subj in df['s']:
        list_subj = str(subj).split('/')
        #logger.debug(f"Site : {list_subj[3]} and type : {str(list_subj[4]).split('_')[0]} for entity : {str(list_subj[4])}")
        dict_type[str(list_subj[3])][str(list_subj[4].split('_')[0])] |= {str(list_subj[4])}

    for index, row in df.iterrows():
        if "sameAs" in str(row['p']):
            site_in = str(row['s']).split('/')[3]
            site_out = str(row['o']).split('/')[3]
            dict_sameas_in[str(site_in)] += 1
            dict_sameas_out[str(site_out)] += 1

    #logger.debug(dict_sameas_in)
    #logger.debug(dict_sameas_out)

    yaml_dict = defaultdict(lambda: defaultdict(int))

    for key, val in dict_type.items():
        for key_t, val_t in val.items():
            logger.info(f"Site {key} have {len(val_t)} entity for type {key_t}")
            yaml_dict[str(key)][str(key_t)] = len(val_t)

    for key, val in dict_type.items():
        for key_t, val_t in val.items():
            logger.info(f"For all site, we have {len(val_t)} entity for type {key_t}")
            yaml_dict["all_site"][str(key_t)] += len(val_t)

    for key, val in dict_sameas_in.items():
        yaml_dict[str(key)]["nb_sameas_in"] = int(val)

    for key, val in dict_sameas_out.items():
        yaml_dict[str(key)]["nb_sameas_out"] = int(val)

    with open(f"{output_file}", 'w') as yaml_file:
        yaml.dump(yaml_dict, yaml_file)

    #logger.debug(dict_type)

if __name__ == "__main__":
    stator()