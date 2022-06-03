#!/bin/bash

PARAM1=$1

PARAM2=$2

echo -e "ld_dir ('${PARAM2}','*.nq', 'http://example.org'); \n rdf_loader_run();" \
   | eval "/mnt/c/Program\\ Files/OpenLink\\ Software/Virtuoso\\ OpenSource\\ 7.2/bin/isql.exe 1111 dba dba" #  | eval $PARAM1 "localhost:1111 dba dba"