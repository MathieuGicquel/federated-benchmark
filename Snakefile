from glob import glob
from re import search
import yaml

configuration = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
nbsites = configuration["nb_sites"]

def todo_query(wildcards):
    print(os.getcwd())
    res=[]
    f=glob(f"lib/gmark/demo/shop/shop-translated/*.sparql")
    for e in f:
    	m = search(f'lib/gmark/demo/shop/shop-translated/(.*).sparql', e)
        query=m.group(1)
        for site in nbsites:
            res.append(f'queries/{site}/{query}.noask.sparql')
    print(f'todo query:{res}')
    return res

def todo_data(wildcards):
    print(os.getcwd())
    res=[]
    for site in nbsites:
        res.append(f'data/{site}/shop-graph.nq')
    print(f'todo:{res}')
    return res

def todo_fede(wildcards):
    print(os.getcwd())
    res=[]
    for site in nbsites:
        res.append(f'Federator/{site}/config.ttl')
    print(f'todo:{res}')
    return res

rule all:
    input:
        todo_query,
        todo_data,
        todo_fede

rule compile_gmark:
    output:
        "lib/gmark/demo/shop/shop-graph.txt0.txt"
    shell:
        "cd lib/gmark/demo/scripts && ./compile-all.sh && ./shop.sh"

rule run_turshop:
    input:
        "lib/gmark/demo/shop/shop-graph.txt0.txt"
    output:
        "data/{site}/shop-graph.nq"
    shell:
        "python3 scripts/turshop.py {input} {wildcards.site} {output}"

rule run_configator:
    input:
        "data/{site}/shop-graph.nq"
    output:
        "Federator/{site}/config.ttl"
    shell:
        "python3 scripts/configator.py {input} {output} localhost:9000/sparql" # TODO

rule run_querylator:
    input:
        compil="lib/gmark/demo/shop/shop-graph.txt0.txt",
        query="lib/gmark/demo/shop/shop-translated/{query}.sparql"
    output:
        "queries/{site}/{query}.noask.sparql"
    shell:
        "python3 scripts/querylator.py {input.query} {output}"