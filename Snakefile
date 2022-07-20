from glob import glob
from re import search
import yaml
from bs4 import BeautifulSoup

CONFIGURATION = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
SITE = CONFIGURATION["site"]
ENDPOINT = CONFIGURATION["endpoint"]
ISQL = CONFIGURATION["isql_virtuoso_path"]
RUN=range(0,CONFIGURATION["run"])
clean_after = CONFIGURATION["clean_after"]
use_watdiv = CONFIGURATION["use_watdiv"]
use_fixator = str(CONFIGURATION["use_fixator"]).lower()

KEEP_QUERIES = CONFIGURATION["queries"]

USE_CASE_INPUT_FILE = "lib/gmark/use-cases/shop.xml"

if use_watdiv:
    GMARK_RAW_QUERY_NUMBER = 20
else:
    GMARK_RAW_QUERY_NUMBER = int(BeautifulSoup(open(USE_CASE_INPUT_FILE, 'r').read(),'html.parser').find('workload').get('size'))

USE_CASE_OUTPUT_FILE = "prepa/gmark/" + str(SITE) + "/use-case/shop.xml"

GMARK_RAW_QUERY="prepa/gmark/" + str(SITE) + "/query/query-{query_id}"  # + r"(\.sparql)|(\.template\.sparql)"
if use_watdiv:
    GMARK_RAW_QUERY += ".template.sparql"
else:
    GMARK_RAW_QUERY += ".sparql"
#GMARK_RAW_QUERY="prepa/gmark/" + str(SITE) + "/query/query-{query_id}.template.sparql"
#GMARK_RAW_QUERY_NUMBER = 10
use_watdiv = str(use_watdiv).lower()

GMARK_RAW_QUERY_EXPAND =  expand(GMARK_RAW_QUERY, query_id=range(0,GMARK_RAW_QUERY_NUMBER))

GMARK_RAW_DATA = "prepa/gmark/" + str(SITE) + "/data/shop-a-graph.txt0.txt"
GMARK_RAW_WORKLOAD = "prepa/gmark/" + str(SITE) + "/workload/workload.xml"

FIXATOR_DATA="prepa/gmark/" + str(SITE) + "/data/shop-a-graph.fixed.txt"

TTL_RAW="prepa/ttl/" + str(SITE) + "/data/data.ttl"

INGEST_TTL_LOG="prepa/ttl/" + str(SITE) + "/log/ingest_ttl.log"

CONSTRUCTOR_NQ="result/"+"site-" + str(SITE) +"/data/data.nq"

DIGEST_TTL_LOG="prepa/ttl/" + str(SITE) + "/log/digestuoso_ttl.log"

DATA_PREPA="prepa/data.nq"

QUERIES_PREPA="prepa/queries/" + str(SITE) +"/query-{query_id}.noask.sparql"
QUERIES_PREPA_EXPAND=expand(QUERIES_PREPA, query_id=range(0,GMARK_RAW_QUERY_NUMBER))

QUERIES_SOURCE_SELECTION_PREPA="prepa/queries/" + str(SITE) +"/query-{query_id}.ss.sparql"
QUERIES_SOURCE_SELECTION_PREPA_EXPAND=expand(QUERIES_SOURCE_SELECTION_PREPA, query_id=range(0,GMARK_RAW_QUERY_NUMBER))

INGESTUOSO_PREPA_LOG="prepa/" + str(SITE)  + "/log/ingestuoso.log"

FILTERROYAL_PREPA_QUERIES="prepa/filter_queries/" + str(SITE)  + "/query-{query_id}.noask.sparql"
FILTERROYAL_PREPA_QUERIES_EXPAND=expand(FILTERROYAL_PREPA_QUERIES, query_id=range(0,KEEP_QUERIES))

FILTERROYAL_PREPA_SS_QUERIES="prepa/filter_queries/" + str(SITE)  + "/query-{query_id}.ss.sparql"
FILTERROYAL_PREPA_SS_QUERIES_EXPAND=expand(FILTERROYAL_PREPA_SS_QUERIES,query_id=range(0,KEEP_QUERIES))

DIGESTUOSO_PREPA_LOG="prepa/" + str(SITE)  + "/log/digestuoso.log"

DATA="result/"+"site-" + str(SITE) +"/data/data.nq"

CONFIG_TTL="result/"+ "site-" +str(SITE) +"/config/config.ttl"

INGESTUOSO_LOG="result/" +"site-" + str(SITE) + "/log/ingestuoso.log"

QUERY_VARIATION="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.noask.cst.sparql"
QUERY_VARIATION_EXPAND=expand(QUERY_VARIATION,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
QUERY_VARIATION_SS="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.ss.cst.sparql"
QUERY_VARIATION_SS_EXPAND=expand(QUERY_VARIATION_SS,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

SOURCE_SELECTION_QUERY="result/" + "site-" +str(SITE) + "/run-{run_id}/query-{query_id}/ssopt/query-{query_id}.ss.opt"
SOURCE_SELECTION_QUERY_EXPAND=expand(SOURCE_SELECTION_QUERY,run_id=RUN,query_id=range(0,KEEP_QUERIES))

SOURCE_SELECTION_QUERY_VARIATION="result/" + "site-" +str(SITE) + "/run-{run_id}/query-{query_id}-{query_variation_id}/ssopt/query-{query_id}-{query_variation_id}.ss.opt"
SOURCE_SELECTION_QUERY_VARIATION_EXPAND=expand(SOURCE_SELECTION_QUERY_VARIATION,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

VIRTUOSO_QUERY_OUT="result/"+ "site-" + str(SITE)+"/run-{run_id}/query-{query_id}/virtuoso/query-{query_id}.out"
VIRTUOSO_QUERY_OUT_EXPAND=expand(VIRTUOSO_QUERY_OUT,run_id=RUN,query_id=range(0,KEEP_QUERIES))
VIRTUOSO_QUERY_STAT="result/"+ "site-" +str(SITE)+"/run-{run_id}/query-{query_id}/virtuoso/query-{query_id}.csv"
VIRTUOSO_QUERY_STAT_EXPAND=expand(VIRTUOSO_QUERY_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES))

VIRTUOSO_QUERY_VARIATION_OUT="result/"+ "site-" + str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.out"
VIRTUOSO_QUERY_VARIATION_OUT_EXPAND=expand(VIRTUOSO_QUERY_VARIATION_OUT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
VIRTUOSO_QUERY_VARIATION_STAT="result/"+ "site-" + str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.csv"
VIRTUOSO_QUERY_VARIATION_STAT_EXPAND=expand(VIRTUOSO_QUERY_VARIATION_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

FEDERAPP_DEFAULT_RESULT="result/"+"site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "default/" +"query-{query_id}.out"
FEDERAPP_DEFAULT_RESULT_EXPAND=expand(FEDERAPP_DEFAULT_RESULT,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_DEFAULT_STAT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "default/" +"query-{query_id}.csv"
FEDERAPP_DEFAULT_STAT_EXPAND=expand(FEDERAPP_DEFAULT_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_DEFAULT_LOG="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "default/" +"query-{query_id}.log"
FEDERAPP_DEFAULT_LOG_EXPAND=expand(FEDERAPP_DEFAULT_LOG,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_DEFAULT_SS="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "default/" +"query-{query_id}.ss.csv"
FEDERAPP_DEFAULT_SS_EXPAND=expand(FEDERAPP_DEFAULT_SS,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_DEFAULT_HTTPREQ="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "default/" +"query-{query_id}.httpreq.txt"
FEDERAPP_DEFAULT_HTTPREQ_EXPAND=expand(FEDERAPP_DEFAULT_HTTPREQ,run_id=RUN,query_id=range(0,KEEP_QUERIES))

FEDERAPP_VARIATION_DEFAULT_RESULT="result/"+"site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.out"
FEDERAPP_VARIATION_DEFAULT_RESULT_EXPAND=expand(FEDERAPP_VARIATION_DEFAULT_RESULT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_DEFAULT_STAT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.csv"
FEDERAPP_VARIATION_DEFAULT_STAT_EXPAND=expand(FEDERAPP_VARIATION_DEFAULT_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_DEFAULT_LOG="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.log"
FEDERAPP_VARIATION_DEFAULT_LOG_EXPAND=expand(FEDERAPP_VARIATION_DEFAULT_LOG,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_DEFAULT_SS="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.ss.csv"
FEDERAPP_VARIATION_DEFAULT_SS_EXPAND=expand(FEDERAPP_VARIATION_DEFAULT_SS,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
FEDERAPP_VARIATION_DEFAULT_HTTPREQ="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}-{query_variation_id}/rdf4j/"+ "default/" +"query-{query_id}-{query_variation_id}.httpreq.txt"
FEDERAPP_VARIATION_DEFAULT_HTTPREQ_EXPAND=expand(FEDERAPP_VARIATION_DEFAULT_HTTPREQ,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

FEDERAPP_FORCE_RESULT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "force/" +"query-{query_id}.out"
FEDERAPP_FORCE_RESULT_EXPAND=expand(FEDERAPP_FORCE_RESULT,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_FORCE_STAT="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "force/" +"query-{query_id}.csv"
FEDERAPP_FORCE_STAT_EXPAND=expand(FEDERAPP_FORCE_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_FORCE_LOG="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "force/" +"query-{query_id}.log"
FEDERAPP_FORCE_LOG_EXPAND=expand(FEDERAPP_FORCE_LOG,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_FORCE_SS="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "force/" +"query-{query_id}.ss.csv"
FEDERAPP_FORCE_SS_EXPAND=expand(FEDERAPP_FORCE_SS,run_id=RUN,query_id=range(0,KEEP_QUERIES))
FEDERAPP_FORCE_HTTPREQ="result/"+ "site-" +str(SITE) +"/run-{run_id}/query-{query_id}/rdf4j/"+ "force/" +"query-{query_id}.httpreq.txt"
FEDERAPP_FORCE_HTTPREQ_EXPAND=expand(FEDERAPP_FORCE_HTTPREQ,run_id=RUN,query_id=range(0,KEEP_QUERIES))

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

VIRTUOSO_CST_QUERY_OUT="result/"+ "site-" + str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.out"
VIRTUOSO_CST_QUERY_OUT_EXPAND=expand(VIRTUOSO_CST_QUERY_OUT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)
VIRTUOSO_CST_QUERY_STAT="result/"+ "site-" +str(SITE)+"/run-{run_id}/query-{query_id}-{query_variation_id}/virtuoso/query-{query_id}-{query_variation_id}.csv"
VIRTUOSO_CST_QUERY_STAT_EXPAND=expand(VIRTUOSO_CST_QUERY_STAT,run_id=RUN,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

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


# PREPARATION PHASE
rule run_generate_use_case:
    input:
        USE_CASE_INPUT_FILE
    output:
        USE_CASE_OUTPUT_FILE
    shell:
        "python3 ./scripts/retailer_evolution.py {input} " + str(SITE) + " {output}"

# run gmark and generate queries
rule run_gmark:
    input:
        use_case=USE_CASE_OUTPUT_FILE
    output:
        graph=GMARK_RAW_DATA,
        query=GMARK_RAW_QUERY_EXPAND,
        workload=GMARK_RAW_WORKLOAD
    shell:
         "cd lib/gmark/src/ && ./test -a -c ../../../{input.use_case}  -g ../../../" + "prepa/gmark/" + str(SITE) + "/data/shop-a-graph.txt "+" -w ../../../{output.workload} && cd ../src/querytranslate && if "+ str(use_watdiv) +" ; then cp ../../../../lib/watdiv-queries/* ../../../../" + os.path.dirname(GMARK_RAW_QUERY) + " ; else ./test -w ../../../../{output.workload} -o ../../../../" + os.path.dirname(GMARK_RAW_QUERY) + " ; fi"

# PREPARATION PHASE - GENERATE THE DATASET

# run fixator
rule run_fixator:
    input:
        graph=GMARK_RAW_DATA
    output:
        graph=FIXATOR_DATA
    shell:
        "if "+ str(use_fixator) +" ; then python3 scripts/fixator.py {input.graph} {output.graph} ; else cat {input.graph} >> {output.graph} ; fi"

# convert txt in ttl
rule run_fixatordata_2_ttl:
    input:
        graph=FIXATOR_DATA
    output:
        graph=TTL_RAW
    shell:
        """
        awk '{{print "<http://example.org/_/"$1">" " <http://example.org/_/"$2"> " "<http://example.org/_/"$3"> ."}}' {input.graph} > {output.graph}
        """

# ingest the ttl
rule run_ingest_ttl:
    input:
        data=TTL_RAW
    output:
        log=INGEST_TTL_LOG
    shell:
        "./scripts/ingestuoso.sh '" + ISQL + "' " + os.getcwd()+ "/" + os.path.dirname(TTL_RAW) + " > {output.log}"

# generate the final dataset
rule run_constructor:
    input:
        INGEST_TTL_LOG, # pré-condition
    output:
        data=DATA
    shell:
         "python3 ./scripts/constructor.py ./scripts/constructor/ {output.data}"

# remove the old dataset
rule run_digest_ttl:
    input:
        DATA # pré-condition
    output:
        log=DIGEST_TTL_LOG
    shell:
        "./scripts/digestuoso.sh '" + ISQL + "' > {output.log}"

# PREPARATION PHASE - .GENERATE THE DATASET

# PREPARATION PHASE - PREPARE QUERIES

# translate queries to safe format
rule run_querylator_prepa:
    input:
        DIGEST_TTL_LOG, # pre condition
        queries=GMARK_RAW_QUERY
    output:
        queries=QUERIES_PREPA,
        queries_ss=QUERIES_SOURCE_SELECTION_PREPA
    params:
        query=GMARK_RAW_QUERY
    shell:
        "python3 scripts/querylator.py {params.query} {output.queries} {output.queries_ss}"

# ingest the dataset
rule run_ingestuoso_prepa:
    input:
        DIGEST_TTL_LOG, # pre condition
        data=DATA
    output:
        log=INGESTUOSO_PREPA_LOG
    shell:
        "./scripts/ingestuoso.sh '" + ISQL + "' " + os.getcwd()+ "/" + os.path.dirname(DATA) + " > {output.log}"

# keep queries that return atleast 1 result
rule run_filterroyal_prepa:
    input:
        INGESTUOSO_PREPA_LOG,
        queries=QUERIES_PREPA_EXPAND
    output:
        queries=FILTERROYAL_PREPA_QUERIES_EXPAND,
        ss_queries=FILTERROYAL_PREPA_SS_QUERIES_EXPAND
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

# digest the dataset
rule run_digestuoso_prepa:
    input:
        QUERY_VARIATION_EXPAND, # pré-condition
        QUERY_VARIATION_SS_EXPAND # pré-condition
    output:
        DIGESTUOSO_PREPA_LOG
    shell:
        "./scripts/digestuoso.sh '" + ISQL + "' > {output}"

# PREPARATION PHASE - .PREPARE QUERIES
# .PREPARATION PHASE

# MAIN PHASE

rule run_ingestuoso:
    input:
        DIGESTUOSO_PREPA_LOG,
        data=DATA
    output:
        log=INGESTUOSO_LOG
    shell:
        "./scripts/ingestuoso.sh '" + ISQL + "' " + os.getcwd()+ "/" + os.path.dirname(DATA) + " > {output.log}"


# generate the ttl configuration for fedx
rule run_configator:
    input:
        data=DATA
    output:
        config=CONFIG_TTL
    shell:
         "python3 scripts/configator.py {input.data} {output.config} " + ENDPOINT


rule run_virtuoso_sourceselection_query: # compute source selection query
    input:
        INGESTUOSO_LOG, # pré-condition : run_ingestuoso
        query=FILTERROYAL_PREPA_SS_QUERIES
    output:
        result=SOURCE_SELECTION_QUERY
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result}"


rule run_virtuoso_sourceselection_variation_query:
    input:
        INGESTUOSO_LOG, # pré-condition : run_ingestuoso
        query=QUERY_VARIATION_SS
    output:
        result=SOURCE_SELECTION_QUERY_VARIATION
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result}"



rule run_virtuoso:
    input:
        INGESTUOSO_LOG, # pré-condition : run_ingestuoso
        query=FILTERROYAL_PREPA_QUERIES
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

rule run_virtuoso_variation_query:
    input:
        INGESTUOSO_LOG, # pré-condition : run_ingestuoso
        query=QUERY_VARIATION
    output:
        out=VIRTUOSO_QUERY_VARIATION_OUT,
        stat=VIRTUOSO_QUERY_VARIATION_STAT
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.out} --measures {output.stat}"

rule compile_and_run_federapp_default:
    input:
        INGESTUOSO_LOG,
        query=FILTERROYAL_PREPA_QUERIES,
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


rule compile_and_run_federapp_variation_default:
    input:
        INGESTUOSO_LOG,
        query=QUERY_VARIATION,
        config=CONFIG_TTL
    params:
        run=RUN
    output:
        result=FEDERAPP_VARIATION_DEFAULT_RESULT,
        stat=FEDERAPP_VARIATION_DEFAULT_STAT,
        log=FEDERAPP_VARIATION_DEFAULT_LOG,
        sourceselection=FEDERAPP_VARIATION_DEFAULT_SS,
        httpreq=FEDERAPP_VARIATION_DEFAULT_HTTPREQ
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
    input:
        INGESTUOSO_LOG,
        query=FILTERROYAL_PREPA_QUERIES,
        config=CONFIG_TTL,
        ssopt=SOURCE_SELECTION_QUERY
    params:
        run=RUN
    output:
        result=FEDERAPP_FORCE_RESULT,
        stat=FEDERAPP_FORCE_STAT,
        log=FEDERAPP_FORCE_LOG,
        sourceselection=FEDERAPP_FORCE_SS,
        httpreq=FEDERAPP_FORCE_HTTPREQ
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

rule compile_and_run_federapp_variation_force:
    input:
        INGESTUOSO_LOG,
        query=QUERY_VARIATION,
        config=CONFIG_TTL,
        ssopt=SOURCE_SELECTION_QUERY_VARIATION
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
        FEDERAPP_VARIATION_DEFAULT_LOG_EXPAND,
        FEDERAPP_VARIATION_FORCE_LOG_EXPAND,
        VIRTUOSO_QUERY_VARIATION_OUT_EXPAND
    output:
        log=DIGESTUOSO_LOG
    shell:
        "./scripts/digestuoso.sh '" + ISQL + "' > {output.log}"


rule run_mergeall:
    input:
        #pré-condition
        VIRTUOSO_QUERY_VARIATION_STAT_EXPAND,
        FEDERAPP_VARIATION_DEFAULT_STAT_EXPAND,
        FEDERAPP_VARIATION_FORCE_STAT_EXPAND
    output:
        MERGEALL_RDF4J_DEFAULT,
        MERGEALL_RDF4J_FORCE,
        MERGEALL_VIRTUOSO
    shell:
        "python3 scripts/mergall.py 'result/site-" + str(SITE) + "' 'result/'"

rule run_stator:
    input:
        data=DATA
    output:
        STATOR_OUT
    shell:
        "python3 ./scripts/stator.py {input.data} {output}"