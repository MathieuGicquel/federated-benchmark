import os.path
import sys
print("#", sys.version)
print("#", sys.executable)
from glob import glob
from re import search
import yaml
from bs4 import BeautifulSoup

CONFIGURATION = yaml.load(open("configuration.yaml"), Loader=yaml.FullLoader)
SITE = CONFIGURATION["site"]
ENDPOINT = CONFIGURATION["endpoint"]
ISQL = CONFIGURATION["isql_virtuoso_path"]
USE_CASE_INPUT_FILE = "use-case/shop.xml"
RUN=range(0,int(CONFIGURATION["run"]))
KEEP_QUERIES = CONFIGURATION["queries"]
FEDERAPP_QUERY_TIMEOUT= CONFIGURATION["federapp_query_timeout"]
clean_after = CONFIGURATION["clean_after"]
use_watdiv = CONFIGURATION["use_watdiv"]
improve_data_coherency = str(CONFIGURATION["improve_data_coherency"]).lower()

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

COMPILE_GMARK_LOG="prepa/" + str(SITE) + "/log/compile_gmark.log"

WORKLOAD_GMARK="prepa/" + str(SITE) + "/" +"gmark/shop-workload-{i}.xml"
WORKLOAD_GMARK_EXPAND=expand(MULTI_GMARK_GRAPH,i=range(0,SITE))

ALLWORKLOAD_GMARK="prepa/" + str(SITE)+ "/workload/shop-workload.xml"

IMPROVED_GRAPH = "prepa/" + str(SITE) + "/" +"gmark/data-{i}.txt0.fixed.txt"
IMPROVED_GRAPH_EXPAND = expand(IMPROVED_GRAPH,i=range(0,SITE))

MULTI_GMARK_QUERIES = "prepa/" + str(SITE) + "/" +"queries/query-{query_id}" + EXT
MULTI_GMARK_QUERIES_EXPAND = expand(MULTI_GMARK_QUERIES,query_id=[i for i in range(0,QUERY_NUMBER)])

SHOP_XML = "prepa/" + str(SITE) + "/" +"use-cases/shop-{i}.xml"
SHOP_XML_EXPAND = expand(SHOP_XML,i=range(0,SITE))

RAW_DATA_NQ = "prepa/" + str(SITE) + "/" +"data/" + "data.tmp.nq"

DATA_NQ = "result/site-" + str(SITE) +"/data/data.nq"

CONFIG_TTL="result/"+ "site-" +str(SITE) +"/config/config.ttl"

QUERY_TRANSLATED="prepa/" + str(SITE) +"/queries_translated/query-{query_id}.noask" + EXT
QUERY_TRANSLATED_EXPAND=expand(QUERY_TRANSLATED, query_id=range(0,QUERY_NUMBER))

INGEST_TTL_LOG="prepa/" + str(SITE) + "/log/ingest_ttl.log"

FILTERED_QUERY="prepa/"+ str(SITE)  +"/filter_queries/query-{query_id}.noask" + EXT
FILTERED_QUERY_EXPAND=expand(FILTERED_QUERY, query_id=range(0,KEEP_QUERIES))

QUERY_WCST="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.noask.cst.sparql"
QUERY_WCST_EXPAND=expand(QUERY_WCST,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

QUERY_WCST_SS="result/site-" + str(SITE) + "/queries/query-{query_id}-{query_variation_id}.ss.cst.sparql"
QUERY_WCST_SS_EXPAND=expand(QUERY_WCST_SS,query_id=range(0,KEEP_QUERIES),query_variation_id=1)

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


REMOVEDATA_LOG="result/" + "site-" +str(SITE) + "/log/digestuoso.log"

MERGEALL_RDF4J_DEFAULT="result/" + "all_" + str(SITE) +"_"  + "rdf4j_default" + ".csv"
MERGEALL_RDF4J_FORCE="result/" + "all_"   +str(SITE) +"_"  + "rdf4j_force" + ".csv"
MERGEALL_VIRTUOSO="result/" + "all_" +str(SITE) +"_"  + "virtuoso" + ".csv"

STATS_FILE = "result/stat_" + str(SITE) + ".yaml"

rule all:
    input:
        MERGEALL_RDF4J_DEFAULT,
        MERGEALL_RDF4J_FORCE,
        MERGEALL_VIRTUOSO,
        REMOVEDATA_LOG if clean_after else [],
        STATS_FILE if True else []


# -- gMark rules
rule run__generate_use_cases:
    output:
        SHOP_XML_EXPAND
    shell:
        "python3 ./scripts/generate_use_cases.py " + USE_CASE_INPUT_FILE + " " + os.path.dirname(SHOP_XML)

rule compile__gmark:
    output:
        COMPILE_GMARK_LOG
    shell:
        """cd lib/gmark/demo/scripts && ./compile-all.sh > ../../../../{output}"""

rule run__graph_gmark:
    input:
        SHOP_XML,
        COMPILE_GMARK_LOG
    output:
        graph=MULTI_GMARK_GRAPH,
        workload=WORKLOAD_GMARK
    params:
        use_case=SHOP_XML
    shell: #TODO: remove hardcoded -g path
        "cd lib/gmark/src/ && ./test -c ../../../{params.use_case}  -g ../../../prepa/" + str(SITE) + "/gmark/data-{wildcards.i}.txt"+" -w ../../../{output.workload} -a && sed -i '1,1d' ../../../{output.workload} && sed -i '$d' ../../../{output.workload}"

rule run__valide_gmark_workload:
    input:
        WORKLOAD_GMARK_EXPAND
    output:
        workload=ALLWORKLOAD_GMARK
    shell:
        """
            cd lib/gmark/src/
            echo "<queries>" > ../../../{output.workload}
            cat ../../../"""+ os.path.dirname(WORKLOAD_GMARK) +"""/*.xml >> ../../../{output.workload}
            echo "</queries>" >> ../../../{output.workload}
        """

rule run__compute_queries:
    input:
        workload=ALLWORKLOAD_GMARK
    output:
        MULTI_GMARK_QUERIES_EXPAND
    shell:
        """
            cd lib/gmark/src/querytranslate
            if """ + str(use_watdiv) +""" ; then cp ../../../../lib/watdiv-queries/* ../../../../"""+ os.path.dirname(MULTI_GMARK_QUERIES)+""" ; else ./test -w ../../../../{input.workload} -o ../../../../""" + os.path.dirname(MULTI_GMARK_QUERIES)+""" ; fi
        """

# -- .gMark rules
# -- output: MULTI_GMARK_GRAPH, MULTI_GMARK_QUERIES_EXPAND

rule run_or_skip__improve_data_coherency:
    input:
        graph=MULTI_GMARK_GRAPH
    output:
        graph=IMPROVED_GRAPH
    shell:
        "if "+ str(improve_data_coherency) +" ; then python3 scripts/improve_data_coherency.py {input.graph} {output.graph} ; else cat {input.graph} >> {output.graph} ; fi"

rule run__graphs_txt_to_nq:
    input:
        IMPROVED_GRAPH_EXPAND
    output:
        RAW_DATA_NQ
    shell:
        "python3 scripts/graphs_txt_to_nq.py "+ os.path.dirname(MULTI_GMARK_GRAPH) +" {output}"


rule run__replicate_data_across_sites:
    input:
        RAW_DATA_NQ
    output:
        DATA_NQ
    shell:
        "python3 scripts/replicate_data_across_sites.py {input} {output} " + str(SITE)


rule run__generate_fedx_config_file:
    input:
        DATA_NQ
    output:
       CONFIG_TTL
    shell:
        "python3 scripts/generate_fedx_config_file.py {input} {output} " + ENDPOINT


rule run__translate_gmark_query:
    input:
        queries=MULTI_GMARK_QUERIES,
    output:
        output_query=QUERY_TRANSLATED
    params:
        query=MULTI_GMARK_QUERIES
    shell:
        "python3 scripts/translate_gmark_query.py {params.query} {output.output_query}"


rule run__ingest_data:
    input:
        DATA_NQ
    output:
        INGEST_TTL_LOG
    shell:
        "./scripts/ingest_data.sh '" + ISQL + "' " + os.path.dirname(os.path.abspath(DATA_NQ)) + " > {output}"
    #"./scripts/ingestuoso.sh " + ISQL + " C:/Users/yotla/OneDrive/Bureau/Code/TER/yotmat/federated-benchmark/data/{wildcards.site} >> {output}" #Work on Windows with WSL

rule run__filter_queries:
    input:
        INGEST_TTL_LOG,
        queries=QUERY_TRANSLATED_EXPAND
    output:
        queries=FILTERED_QUERY_EXPAND
    shell:
        "python3 ./scripts/filter_queries.py "
            + os.path.dirname(QUERY_TRANSLATED) + " "
            + "--output " + os.path.dirname(FILTERED_QUERY) + " "
            + "--entrypoint " + ENDPOINT + " "
            + str(KEEP_QUERIES) + " "

# add constant to queries
rule run__add_constant_to_queries:
    input:
        query=FILTERED_QUERY_EXPAND
    output:
        QUERY_WCST_EXPAND,
        QUERY_WCST_SS_EXPAND
    shell:
        "python3 ./scripts/add_constant_to_queries.py "
            + os.path.dirname(FILTERED_QUERY) # get the directory of queries
            + " --output " + os.path.dirname(QUERY_WCST) + ""




rule run__virtuoso_compute_sourceselection_query: # compute source selection query
    input:
        query=QUERY_WCST_SS
    output:
        result=SOURCE_SELECTION_QUERY
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/run_query_on_virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.result}"


rule run__virtuoso_compute_query:
    input:
        query=QUERY_WCST
    output:
        out=VIRTUOSO_QUERY_OUT,
        stat=VIRTUOSO_QUERY_STAT
    threads: 1
    params:
        endpoint=ENDPOINT,
        run_id=RUN
    shell:
        "python3 ./scripts/run_query_on_virtuoso.py {input.query} \
            --entrypoint {params.endpoint} \
            --output {output.out} --measures {output.stat}"




rule run__compile_and_run_federapp_default:
    input:
        query=QUERY_WCST,
        config=CONFIG_TTL
    params:
        run=RUN
    threads: 1
    output:
        result=FEDERAPP_DEFAULT_RESULT,
        stat=FEDERAPP_DEFAULT_STAT,
        log=FEDERAPP_DEFAULT_LOG,
        sourceselection=FEDERAPP_DEFAULT_SS,
        httpreq=FEDERAPP_DEFAULT_HTTPREQ
    shell:
        "./scripts/compile_and_run_federapp.sh "
        + ("%s " % FEDERAPP_QUERY_TIMEOUT)
        + os.getcwd() +"/{input.config} "
        + os.getcwd() +"/{input.query} "
        + os.getcwd() +"/{output.result}  "
        + os.getcwd() +"/{output.stat} "
        + os.getcwd() +"/{output.sourceselection} "
        + os.getcwd() +"/{output.httpreq} "
        + " > " + os.getcwd() +"/{output.log}"

rule run__compile_and_run_federapp_forcess:
    input:
        query=QUERY_WCST,
        config=CONFIG_TTL,
        ssopt=SOURCE_SELECTION_QUERY
    params:
        run=RUN
    threads: 1
    output:
        result=FEDERAPP_VARIATION_FORCE_RESULT,
        stat=FEDERAPP_VARIATION_FORCE_STAT,
        log=FEDERAPP_VARIATION_FORCE_LOG,
        sourceselection=FEDERAPP_VARIATION_FORCE_SS,
        httpreq=FEDERAPP_VARIATION_FORCE_HTTPREQ
    shell:
        "./scripts/compile_and_run_federapp.sh "
        + ("%s " % FEDERAPP_QUERY_TIMEOUT)
        + os.getcwd() +"/{input.config} "
        + os.getcwd() +"/{input.query} "
        + os.getcwd() +"/{output.result}  "
        + os.getcwd() +"/{output.stat} "
        + os.getcwd() +"/{output.sourceselection} "
        + os.getcwd() +"/{output.httpreq} "
         + os.getcwd() +"/{input.ssopt} "
        + " > " + os.getcwd() +"/{output.log}"


rule run__remove_data:
    input:
        # pré-condition
        VIRTUOSO_QUERY_STAT_EXPAND,
        FEDERAPP_DEFAULT_STAT_EXPAND,
        FEDERAPP_VARIATION_FORCE_STAT_EXPAND
    output:
        log=REMOVEDATA_LOG
    shell:
        "./scripts/remove_data.sh '" + ISQL + "' > {output.log}"


rule run__merge_results:
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
        "python3 scripts/merge_results.py 'result/site-" + str(SITE) + "' 'result/'"


rule run__compute_statistics:
    input:
        data=DATA_NQ
    output:
        STATS_FILE
    shell:
        "python3 ./scripts/compute_statistics.py {input.data} {output}"
