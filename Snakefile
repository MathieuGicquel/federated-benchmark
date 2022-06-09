from glob import glob
from re import search
import yaml

CONFIGURATION = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
SITE = CONFIGURATION["site"]
ENDPOINT = CONFIGURATION["endpoint"]
ISQL = CONFIGURATION["isql_virtuoso_path"]
QUERY_NUMBER=50
RUN=range(0,1)


OUTPUT_FILES_FEDERATED = expand("result/{site}/{run}/query-{query_number}/rdf4j/{mode}/query-{query_number}.{ext}",
    site=SITE,
    query_number=range(0,QUERY_NUMBER),
    ext=["out","csv","log","sourceselection.csv","httpreq.txt"],
    run=RUN,
    mode=["default","force"]
)
OUTPUT_FILES_VIRTUOSO = expand("result/{site}/{run}/query-{query_number}/virtuoso/query-{query_number}.{ext}",
    site=SITE,
    query_number=range(0,QUERY_NUMBER),
    ext=["out","csv"],
    run=RUN
)

rule all:
    input:
        OUTPUT_FILES_FEDERATED,
        OUTPUT_FILES_VIRTUOSO,
        "result/" + "all_" + str(SITE) +"_"  + "virtuoso" + ".csv",
        "result/" + "all_" + str(SITE) +"_"  + "rdf4j_force" + ".csv",
        "result/" + "all_" + str(SITE) +"_"  + "rdf4j_default" + ".csv",
        expand("queries/{site}/query-{query_number}.sourceselection.sparql", site=SITE, query_number=range(0,QUERY_NUMBER)),
        "./log/" + str(SITE) + "/digestuoso.log", # pré-condition : run_digestuoso


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
        "python3 scripts/turshop.py {input} "+ str(SITE) +" {output}"

rule run_configator:
    input:
        "data/{site}/shop-graph.nq"
    output:
        "Federator/{site}/config.ttl"
    shell:
        "python3 scripts/configator.py {input} {output} " + ENDPOINT + " data/{wildcards.site}/sitelist.txt"




rule run_querylator:
    input:
        queries="lib/gmark/demo/shop/shop-translated/query-{query_number}.sparql",
    output:
        output_query="queries/{site}/query-{query_number}.noask.sparql",
        output_source_selection_query="queries/{site}/query-{query_number}.sourceselection.sparql"
    params:
        query="lib/gmark/demo/shop/shop-translated/query-{query_number}.sparql"
    shell:
        "python3 scripts/querylator.py {params.query} {output.output_query} {output.output_source_selection_query}"

rule run_ingestuoso:
    input:
        "data/{site}/shop-graph.nq"
    output:
        "log/{site}/ingestuoso.log"
    shell:
        "./scripts/ingestuoso.sh '" + ISQL + "' '" + os.getcwd() + "/data/{wildcards.site}' >> {output}"
    #"./scripts/ingestuoso.sh " + ISQL + " C:/Users/yotla/OneDrive/Bureau/Code/TER/yotmat/federated-benchmark/data/{wildcards.site} >> {output}" #Work on Windows with WSL

rule run_virtuoso_sourceselection_query:
    input:
        "log/{site}/ingestuoso.log", # pré-condition : run_ingestuoso
        query="queries/{site}/{query}.sourceselection.sparql",
    output:
        result="result/{site}/{run}/{query}/rdf4j/force/{query}.sourceselection.opt"
    params:
        endpoint=ENDPOINT,
        run=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result}"

rule compile_and_run_federapp_default:
    threads: 1
    input:
        "log/{site}/ingestuoso.log", # pré-condition : run_ingestuoso
        query="queries/{site}/{query}.noask.sparql",
        config="Federator/{site}/config.ttl"
    params:
        run=RUN
    output:
        result="result/{site}/{run}/{query}/rdf4j/"+ "default/" +"{query}.out",
        stat="result/{site}/{run}/{query}/rdf4j/"+ "default/" +"{query}.csv",
        log="result/{site}/{run}/{query}/rdf4j/"+ "default/" +"{query}.log",
        sourceselection="result/{site}/{run}/{query}/rdf4j/default/{query}.sourceselection.csv",
        httpreq="result/{site}/{run}/{query}/rdf4j/"+ "default/" +"{query}.httpreq.txt"

    shell:
        "./scripts/federapp_com_and_run.sh "
        + os.getcwd() +"/{input.config} "
        + os.getcwd() +"/{input.query} "
        + os.getcwd() +"/{output.result}  "
        + os.getcwd() +"/{output.stat} "
        + os.getcwd() +"/{output.sourceselection} "
        + os.getcwd() +"/{output.httpreq} "
        + " > " + os.getcwd() +"/{output.log}"



rule compile_and_run_federapp_force:
    threads: 1
    input:
        expand("result/{site}/{run}/query-{query_number}/rdf4j/"+ "default/" +"query-{query_number}.out",run=RUN, site=SITE, query_number=range(0,QUERY_NUMBER)), # pré-condition: compile_and_run_federapp_default
        query="queries/{site}/{query}.noask.sparql",
        config="Federator/{site}/config.ttl",
        ssopt="result/{site}/{run}/{query}/rdf4j/force/{query}.sourceselection.opt"
    params:
        run=RUN
    output:
        result="result/{site}/{run}/{query}/rdf4j/"+ "force/" +"{query}.out",
        stat="result/{site}/{run}/{query}/rdf4j/"+ "force/" +"{query}.csv",
        log="result/{site}/{run}/{query}/rdf4j/"+ "force/" +"{query}.log",
        sourceselection="result/{site}/{run}/{query}/rdf4j/"+ "force/" +"{query}.sourceselection.csv",
        httpreq="result/{site}/{run}/{query}/rdf4j/"+ "force/" +"{query}.httpreq.txt",


    shell:
        "./scripts/federapp_com_and_run.sh "
        + os.getcwd() +"/{input.config} "
        + os.getcwd() +"/{input.query} "
        + os.getcwd() +"/{output.result}  "
        + os.getcwd() +"/{output.stat} "
        + os.getcwd() +"/{output.sourceselection} "
        + os.getcwd() +"/{output.httpreq} "
        + os.getcwd() +"/{input.ssopt} "
        + " > " + os.getcwd() +"/{output.log}"

rule run_virtuoso_query:
    input:
        expand("result/{site}/{run}/query-{query_number}/rdf4j/"+ "force/" +"query-{query_number}.out",run=RUN, site=SITE, query_number=range(0,QUERY_NUMBER)), # pré-condition
        query="queries/{site}/{query}.noask.sparql"
    output:
        result="result/{site}/{run}/{query}/virtuoso/{query}.out",
        stats="result/{site}/{run}/{query}/virtuoso/{query}.csv"
    params:
        endpoint=ENDPOINT,
        run=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result} --measures {output.stats}"

rule run_digestuoso:
    input:
        OUTPUT_FILES_VIRTUOSO,
        OUTPUT_FILES_FEDERATED
    output:
        "./log/" + str(SITE) + "/digestuoso.log"
    shell:
        "./scripts/digestuoso.sh '" + ISQL + "' '" + os.getcwd() + "/data/" + str(SITE) +"/sitelist.txt' >> {output}"
        #"./scripts/digestuoso.sh " + ISQL + " 'C:/Users/yotla/OneDrive/Bureau/Code/TER/yotmat/federated-benchmark/data/" + str(SITE) + "/sitelist.txt' >> {output}" #Work on Windows with WSL

rule run_mergeall:
    input:
        OUTPUT_FILES_VIRTUOSO, #pré-condition
        OUTPUT_FILES_FEDERATED
    output:
        "result/" + "all_" + str(SITE) +"_"  + "virtuoso" + ".csv",
        "result/" + "all_" + str(SITE) +"_"  + "rdf4j_force" + ".csv",
        "result/" + "all_" + str(SITE) +"_"  + "rdf4j_default" + ".csv"
    shell:
        "python3 scripts/mergall.py 'result/" + str(SITE) + "' 'result/'"
