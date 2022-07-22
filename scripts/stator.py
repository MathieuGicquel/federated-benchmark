# Import part

import logging
import warnings
from collections import defaultdict
import click
import coloredlogs
import pandas as pd
import yaml
from yaml.representer import Representer

# Goal : Create a yaml statistic files to do some plot on it

yaml.add_representer(defaultdict, Representer.represent_dict)
warnings.simplefilter(action='ignore', category=FutureWarning)
coloredlogs.install(level='DEBUG', fmt='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

@click.command()
@click.argument("input_file")
@click.argument("output_file")

def stator(input_file, output_file):
    df = pd.read_csv(input_file, sep=" ", names=['s', 'p', 'o', 'g','dot'])
    logger.debug(str(df.head()))
    logger.debug(str(df[df["p"].str.contains("sameAs")]))
    set_predicates: set = set(df["p"].apply(lambda p: p.split("/")[3].replace(">","") if "#" not in p else "#type" if "#sameAs" not in p else "#sameAs").unique())
    logger.debug(set_predicates)

    dict_type = defaultdict(lambda: defaultdict(set))

    dict_predicate_in = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    dict_predicate_out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for subj in df['s']:
        list_subj = str(subj).split('/')
        logger.debug(
            f"Site : {list_subj[3]} and type : {str(list_subj[4]).split('_')[0]} for entity : {str(list_subj[4])}")
        dict_type[str(list_subj[3])][str(list_subj[4].split('_')[0])] |= {str(list_subj[4])}

    logger.debug(dict_type)

    for index, row in df.iterrows():
        logger.debug(str(row))
        for predicate in set_predicates:
            if predicate in str(row['p']):
                site_in = str(row['s']).split('/')[3]
                site_out_split = str(row['o']).split('/')
                if len(site_out_split) >= 3 and ('#' not in str(row['o'])):
                    site_out = site_out_split[3]
                    dict_predicate_in[str(site_out)][predicate][str(site_in)] += 1
                    dict_predicate_out[str(site_in)][predicate][str(site_out)] += 1

    yaml_dict = defaultdict(lambda: defaultdict(int))

    yaml_dict_properties = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))))

    for key, val in dict_type.items():
        for key_t, val_t in val.items():
            logger.debug(f"Site {key} have {len(val_t)} entity for type {key_t}")
            yaml_dict[str(key)][str(key_t)] = len(val_t)

    for key, val in dict_type.items():
        for key_t, val_t in val.items():
            logger.debug(f"For all site, we have {len(val_t)} entity for type {key_t}")
            yaml_dict["all_site"][str(key_t)] += len(val_t)

    logger.debug("Merge predicate in out")
    for key_inner in dict_predicate_in.keys():
        for key_outtter in dict_predicate_in.keys():
            for predicate in set_predicates:
                yaml_dict_properties[key_inner][predicate][key_outtter]["in"] = dict_predicate_in[key_inner][predicate][
                    key_outtter]
                yaml_dict_properties[key_inner][predicate][key_outtter]["out"] = \
                    dict_predicate_out[key_inner][predicate][key_outtter]

    logger.debug("Merge dict")
    for k in yaml_dict_properties.keys():
        yaml_dict[k]["predicates"] = yaml_dict_properties[k]

    with open(f"{output_file}", 'w') as yaml_file:
        logger.debug("Writing yaml")
        yaml.dump(yaml_dict, yaml_file)

if __name__ == "__main__":
    stator()
