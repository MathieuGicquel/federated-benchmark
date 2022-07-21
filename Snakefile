from glob import glob
from re import search
import yaml
from bs4 import BeautifulSoup

CONFIGURATION = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
SITE = CONFIGURATION["site"]
ENDPOINT = CONFIGURATION["endpoint"]
ISQL = CONFIGURATION["isql_virtuoso_path"]
USE_CASE_INPUT_FILE = "lib/gmark/use-cases/shop.xml"
RUN=range(0,1)
KEEP_QUERIES = CONFIGURATION["queries"]
clean_after = CONFIGURATION["clean_after"]
use_watdiv = CONFIGURATION["use_watdiv"]
use_fixator = str(CONFIGURATION["use_fixator"]).lower()

if use_watdiv:
    QUERY_NUMBER = 20
else:
    QUERY_NUMBER = int(BeautifulSoup(open(USE_CASE_INPUT_FILE, 'r').read(),'html.parser').find('workload').get('size'))*SITE

if use_watdiv:
    EXT = ".template.sparql"
else:
    EXT = ".sparql"
use_watdiv = str(use_watdiv).lower()

MULTI_GMARK_GRAPH = "prepa/" + str(SITE) + "/" +"gmark/data-{i}.txt0.txt"
MULTI_GMARK_GRAPH_EXPAND = expand(MULTI_GMARK_GRAPH,i=range(0,SITE))

FIXATOR_GRAPH = "prepa/" + str(SITE) + "/" +"gmark/data-{i}.txt0.fixed.txt"
FIXATOR_GRAPH_EXPAND = expand(FIXATOR_GRAPH,i=range(0,SITE))


MULTI_GMARK_QUERIES = "prepa/" + str(SITE) + "/" +"queries/query-{query_id}" + EXT
MULTI_GMARK_QUERIES_EXPAND = expand(MULTI_GMARK_QUERIES,query_id=[i for i in range(0,QUERY_NUMBER)])

SHOP_XML = "prepa/" + str(SITE) + "/" +"use-cases/shop-{i}.xml"
SHOP_XML_EXPAND = expand(SHOP_XML,i=range(0,SITE))

RAW_DATA_NQ = "prepa/" + str(SITE) + "/" +"data/" + "/data.tmp.nq" 

DATA_NQ = "result/site-" + str(SITE) +"/data/data.nq"

CONFIG_TTL="result/"+ "site-" +str(SITE) +"/config/config.ttl"

QUERIES_PREPA="prepa/queries/" + str(SITE) +"/query-{query_id}.noask" + EXT
QUERIES_PREPA_EXPAND=expand(QUERIES_PREPA, query_id=range(0,QUERY_NUMBER))

INGEST_TTL_LOG="prepa/site-" + str(SITE) + "/log/ingest_ttl.log"

FILTERROYAL_PREPA_QUERIES="prepa/filter_queries/" + str(SITE)  + "/query-{query_id}.noask" + EXT
FILTERROYAL_PREPA_QUERIES_EXPAND=expand(FILTERROYAL_PREPA_QUERIES, query_id=range(0,KEEP_QUERIES))

QUERY_VARIATION="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.noask.cst.sparql"
QUERY_VARIATION_EXPAND=expand(QUERY_VARIATION,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

QUERY_VARIATION_SS="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.ss.cst.sparql"
QUERY_VARIATION_SS_EXPAND=expand(QUERY_VARIATION_SS,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

SOURCE_SELECTION_QUERY="result/" + "site-" +str(SITE) + "/run-{run_id}/query-{query_id}-{query_variation_id}/ssopt/query-{query_id}-{query_variation_id}.ss.opt"
SOURCE_SELECTION_QUERY_EXPAND=expand(SOURCE_SELECTION_QUERY,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

VIRTUOSO_QUERY_OUT="result/"+ "site-" + str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.out"
VIRTUOSO_QUERY_OUT_EXPAND=expand(VIRTUOSO_QUERY_OUT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
VIRTUOSO_QUERY_STAT="result/"+ "site-" +str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.csv"
VIRTUOSO_QUERY_STAT_EXPAND=expand(VIRTUOSO_QUERY_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

FEDERAPP_DEFAULT_RESULT="result/"+"site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.out"
FEDERAPP_DEFAULT_RESULT_EXPAND=expand(FEDERAPP_DEFAULT_RESULT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_DEFAULT_STAT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.csv"
FEDERAPP_DEFAULT_STAT_EXPAND=expand(FEDERAPP_DEFAULT_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_DEFAULT_LOG="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.log"
FEDERAPP_DEFAULT_LOG_EXPAND=expand(FEDERAPP_DEFAULT_LOG,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_DEFAULT_SS="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.ss.csv"
FEDERAPP_DEFAULT_SS_EXPAND=expand(FEDERAPP_DEFAULT_SS,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_DEFAULT_HTTPREQ="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.httpreq.txt"
FEDERAPP_DEFAULT_HTTPREQ_EXPAND=expand(FEDERAPP_DEFAULT_HTTPREQ,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)


FEDERAPP_VARIATION_FORCE_RESULT="result/"+"site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "force/" +"query-{query_id}-{query_variation_id}.out"
FEDERAPP_VARIATION_FORCE_RESULT_EXPAND=expand(FEDERAPP_VARIATION_FORCE_RESULT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_FORCE_STAT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "force/" +"query-{query_id}-{query_variation_id}.csv"
FEDERAPP_VARIATION_FORCE_STAT_EXPAND=expand(FEDERAPP_VARIATION_FORCE_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_FORCE_LOG="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "force/" +"query-{query_id}-{query_variation_id}.log"
FEDERAPP_VARIATION_FORCE_LOG_EXPAND=expand(FEDERAPP_VARIATION_FORCE_LOG,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_FORCE_SS="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "force/" +"query-{query_id}-{query_variation_id}.ss.csv"
FEDERAPP_VARIATION_FORCE_SS_EXPAND=expand(FEDERAPP_VARIATION_FORCE_SS,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_FORCE_HTTPREQ="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "force/" +"query-{query_id}-{query_variation_id}.httpreq.txt"
FEDERAPP_VARIATION_FORCE_HTTPREQ_EXPAND=expand(FEDERAPP_VARIATION_FORCE_HTTPREQ,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)


DIGESTUOSO_LOG="result/" + "site-" +str(SITE) + "/log/digestuoso.log"

MERGEALL_RDF4J_DEFAULT="result/" + "all_" + str(SITE) +"_"  + "rdf4j_default" + ".csv"
MERGEALL_RDF4J_FORCE="result/" + "all_"   +str(SITE) +"_"  + "rdf4j_force" + ".csv"
MERGEALL_VIRTUOSO="result/" + "all_" +str(SITE) +"_"  + "virtuoso" + ".csv"
        
STATOR_OUT = "result/stat_" + str(SITE) + ".yaml"  

rule all:
    input:
        MERGEALL_RDF4J_DEFAULT,
        MERGEALL_RDF4J_FORCE,
        MERGEALL_VIRTUOSO,
        DIGESTUOSO_LOG if clean_after else [],
        STATOR_OUT

rule run_shopanhour:
    output:
        SHOP_XML_EXPAND
    shell:
        "python3 ./scripts/shopanhour.py " + USE_CASE_INPUT_FILE + " " + os.path.dirname(SHOP_XML)

rule compile_gmark:
    input:
        SHOP_XML_EXPAND
    output:
        graph=MULTI_GMARK_GRAPH_EXPAND,
        queries=MULTI_GMARK_QUERIES_EXPAND
    shell:
        "./scripts/multi_gmark.sh " + str(SITE) + " " + str(use_watdiv)

rule run_fixator:
    input:
        graph=MULTI_GMARK_GRAPH
    output:
        graph=FIXATOR_GRAPH
    shell:
        "if "+ str(use_fixator) +" ; then python3 scripts/fixator.py {input.graph} {output.graph} ; else cat {input.graph} >> {output.graph} ; fi"

rule run_turshop:
    input:
        FIXATOR_GRAPH_EXPAND
    output:
        RAW_DATA_NQ
    shell:
        "python3 scripts/turshop.py "+ os.path.dirname(MULTI_GMARK_GRAPH) +" {output}"


rule run_replicator:
    input:
        RAW_DATA_NQ
    output:
        DATA_NQ
    shell:
        "python3 scripts/replicator.py {input} {output}"


rule run_configator:
    input:
        DATA_NQ
    output:
       CONFIG_TTL
    shell:
        "python3 scripts/configator.py {input} {output} " + ENDPOINT


rule run_querylator:
    input:
        queries=MULTI_GMARK_QUERIES,
    output:
        output_query=QUERIES_PREPA
    params:
        query=MULTI_GMARK_QUERIES
    shell:
        "python3 scripts/querylator.py {params.query} {output.output_query}"


rule run_ingestuoso:
    input:
        DATA_NQ
    output:
        INGEST_TTL_LOG
    shell:
        "./scripts/ingestuoso.sh '" + ISQL + "' " + os.path.dirname(os.path.abspath(DATA_NQ)) + " > {output}"
    #"./scripts/ingestuoso.sh " + ISQL + " C:/Users/yotla/OneDrive/Bureau/Code/TER/yotmat/federated-benchmark/data/{wildcards.site} >> {output}" #Work on Windows with WSL

rule run_filterroyal_prepa:
    input:
        INGEST_TTL_LOG,
        queries=QUERIES_PREPA_EXPAND
    output:
        queries=FILTERROYAL_PREPA_QUERIES_EXPAND
    shell:
        "python3 ./scripts/filter_royal.py " 
            + os.path.dirname(QUERIES_PREPA) + " " 
            + "--output " + os.path.dirname(FILTERROYAL_PREPA_QUERIES) + " "
            + "--entrypoint " + ENDPOINT + " "
            + str(KEEP_QUERIES) + " "

# add constant to queries
rule run_constantin:
    input:
        query=FILTERROYAL_PREPA_QUERIES_EXPAND
    output:
        QUERY_VARIATION_EXPAND,
        QUERY_VARIATION_SS_EXPAND
    shell:
        "python3 ./scripts/constantin_first.py "
            + os.path.dirname(FILTERROYAL_PREPA_QUERIES) # get the directory of queries
            + " --output " + os.path.dirname(QUERY_VARIATION) + ""




rule run_virtuoso_sourceselection_query: # compute source selection query
    input:
        query=QUERY_VARIATION_SS
    output:
        result=SOURCE_SELECTION_QUERY
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result}"


rule run_virtuoso:
    input:
        query=QUERY_VARIATION
    output:
        out=VIRTUOSO_QUERY_OUT,
        stat=VIRTUOSO_QUERY_STAT
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.out} --measures {output.stat}"




rule compile_and_run_federapp_default:
    input:
        query=QUERY_VARIATION,
        config=CONFIG_TTL
    params:
        run=RUN
    output:
        result=FEDERAPP_DEFAULT_RESULT,
        stat=FEDERAPP_DEFAULT_STAT,
        log=FEDERAPP_DEFAULT_LOG,
        sourceselection=FEDERAPP_DEFAULT_SS,
        httpreq=FEDERAPP_DEFAULT_HTTPREQ
    shell:
        "./scripts/federapp_com_and_run.sh "
        + os.getcwd() +"/{input.config} "
        + os.getcwd() +"/{input.query} "
        + os.getcwd() +"/{output.result}  "
        + os.getcwd() +"/{output.stat} "
        + os.getcwd() +"/{output.sourceselection} "
        + os.getcwd() +"/{output.httpreq} "
        + " > " + os.getcwd() +"/{output.log}"

rule compile_and_run_federapp_variation_force:
    input:
        query=QUERY_VARIATION,
        config=CONFIG_TTL,
        ssopt=SOURCE_SELECTION_QUERY
    params:
        run=RUN
    output:
        result=FEDERAPP_VARIATION_FORCE_RESULT,
        stat=FEDERAPP_VARIATION_FORCE_STAT,
        log=FEDERAPP_VARIATION_FORCE_LOG,
        sourceselection=FEDERAPP_VARIATION_FORCE_SS,
        httpreq=FEDERAPP_VARIATION_FORCE_HTTPREQ
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


rule run_digestuoso:
    input:
        # pré-condition
        VIRTUOSO_QUERY_STAT_EXPAND,
        FEDERAPP_DEFAULT_STAT_EXPAND,
        FEDERAPP_VARIATION_FORCE_STAT_EXPAND
    output:
        log=DIGESTUOSO_LOG
    shell:
        "./scripts/digestuoso.sh '" + ISQL + "' > {output.log}"


rule run_mergeall:
    input:
        #pré-condition
        VIRTUOSO_QUERY_STAT_EXPAND,
        FEDERAPP_DEFAULT_STAT_EXPAND,
        FEDERAPP_VARIATION_FORCE_STAT_EXPAND
    output:
        MERGEALL_RDF4J_DEFAULT,
        MERGEALL_RDF4J_FORCE,
        MERGEALL_VIRTUOSO
    shell:
        "python3 scripts/mergall.py 'result/site-" + str(SITE) + "' 'result/'"


rule run_stator:
    input:
        data=DATA_NQ
    output:
        STATOR_OUT
    shell:
        "python3 ./scripts/stator.py {input.data} {output}"