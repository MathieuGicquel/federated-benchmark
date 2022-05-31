#!/bin/bash

PARAM1=$1

PARAM2=$2

echo $1

echo -e "ld_dir ('${PARAM2}','*', 'http://example.org'); \n rdf_loader_run();" \
  | eval $PARAM1 "localhost:1111 dba dba"