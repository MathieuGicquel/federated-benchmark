from glob import glob
from re import search
import yaml

CONFIGURATION = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
SITE = CONFIGURATION["site"]
ENDPOINT = CONFIGURATION["endpoint"]
ISQL = CONFIGURATION["isql_virtuoso_path"]
QUERY_NUMBER=50
OUTPUT_FILES = expand("result/{site}/query-{query_number}.{ext}",
    site=SITE,
    query_number=range(0,QUERY_NUMBER),
    ext=["out","csv","log"]
)


rule all:
    input:
        OUTPUT_FILES,
        "./log/" + str(SITE) + "/digestuoso.log",
        "result/" + "all_" + str(SITE) + ".csv"


rule compile_gmark:
    output:
        graph="lib/gmark/demo/shop/shop-graph.txt0.txt",
        queries=expand("lib/gmark/demo/shop/shop-translated/query-{query_id}.{query_type}", query_id=[i for i in range(0,QUERY_NUMBER)], query_type=['sparql', 'cypher', 'lb', 'sql'])
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
        "python3 scripts/configator.py {input} {output} " + ENDPOINT + " data/{wildcards.site}/sitelist.txt"

rule run_ingestuoso:
    input:
        "data/{site}/shop-graph.nq"
    output:
        "log/{site}/ingestuoso.log"
    shell:
        "./scripts/ingestuoso.sh " + ISQL + " '" + os.getcwd() + "/data/{wildcards.site}' >> {output}"
        #"./scripts/ingestuoso.sh " + isql + " C:/Users/yotla/OneDrive/Bureau/Code/TER/yotmat/federated-benchmark/data/{wildcards.site} >> {output}"

rule run_querylator:
    input:
        queries="lib/gmark/demo/shop/shop-translated/query-{query_number}.sparql",
    output:
        output="queries/{site}/query-{query_number}.noask.sparql"
    params:
        query="lib/gmark/demo/shop/shop-translated/query-{query_number}.sparql"
    shell:
        "python3 scripts/querylator.py {params.query} {output.output}"


rule compile_and_run_federapp:
    input:
        "log/{site}/ingestuoso.log", # pré-condition : run_ingestuoso
        query="queries/{site}/{query}.noask.sparql",
        config="Federator/{site}/config.ttl"
    output:
        result="result/{site}/{query}.out",
        stat="result/{site}/{query}.csv",
        log="result/{site}/{query}.log"
    shell:
        "./scripts/federapp_com_and_run.sh "+ os.getcwd() +"/{input.config} " + os.getcwd() +"/{input.query} " + os.getcwd() +"/{output.result}  " + os.getcwd() +"/{output.stat} > " + os.getcwd() +"/{output.log}"

rule run_digestuoso:
    input:
        OUTPUT_FILES
    output:
        "./log/" + str(SITE) + "/digestuoso.log"
    shell:
        "./scripts/digestuoso.sh " + ISQL + " '" + os.getcwd() + "/data/" + str(SITE) +"/sitelist.txt' >> {output}"

rule run_mergeall:
    input:
        ["result/" + str(SITE) + "/query-" + str(i) + ".csv" for i in range(0,QUERY_NUMBER)]
    output:
        "result/" + "all_" + str(SITE) + ".csv"
    shell:
        "python3 scripts/mergall.py 'result/" + str(SITE) + "' {output}"
