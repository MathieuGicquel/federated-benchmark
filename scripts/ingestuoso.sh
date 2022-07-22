#!/bin/bash

# Examples of use :
# ./scripts/ingestuoso.sh  /home/ubuntu/virtuoso-opensource/bin/isql /GDD/federated-benchmark/repo/prepa/
# ./scripts/ingestuoso.sh  /home/ubuntu/virtuoso-opensource/bin/isql /GDD/federated-benchmark/repo/result/site-5000/data

# Parameters 1 is where Virtuoso's isql are

PARAM1=$1

# Parameters 2 is where rdf data are
# /!\ YOU NEED TO ADD THE PATH IN VIRTUOSO.INI FILES /!\

PARAM2=$2

# Load in virtuoso rdf data files

echo -e "ld_dir ('${PARAM2}','*.nq', 'http://example.org'); \n rdf_loader_run();" \
  | eval "$PARAM1" "localhost:1111 dba dba"
