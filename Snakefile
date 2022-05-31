from glob import glob
from re import search

def todo(wildcards):
    print(os.getcwd())
    res=[]
    f=glob(f"lib/gmark/demo/shop/shop-translated/*.sparql")
    for e in f:
    	m = search(f'lib/gmark/demo/shop/shop-translated/(.*).sparql', e)
        query=m.group(1)
        res.append(f'queries/{query}.noask.sparql')
    print(f'todo:{res}')
    return res

rule all:
    input:
        "data/shop-graph.nq",
        "Federator/config.ttl",
        todo

rule compile_gmark:
    output:
        "lib/gmark/demo/shop/shop-graph.txt0.txt"
    shell:
        "cd lib/gmark/demo/scripts && ./compile-all.sh && ./shop.sh"

rule run_turshop:
    input:
        "lib/gmark/demo/shop/shop-graph.txt0.txt"
    output:
        "data/shop-graph.nq"
    shell:
        "python3 scripts/turshop.py {input} 3 {output}" # TODO

rule run_configator:
    input:
        "data/shop-graph.nq"
    output:
        "Federator/config.ttl"
    shell:
        "python3 scripts/configator.py {input} {output} localhost:9000/sparql" # TODO

rule run_querylator:
    input:
        "lib/gmark/demo/shop/shop-translated/{query}.sparql"
    output:
        "queries/{query}.noask.sparql"
    shell:
        "python3 scripts/querylator.py {input} {output}"