#!/bin/bash

PARAM1=$1

PARAM2=$2

string="DELETE FROM DB.DBA.load_list; \n"

while IFS= read -r line
do
  string+="DELETE FROM rdf_quad WHERE g = iri_to_id('$line'); \n"
done < $2
  echo -e "$string" \
  | eval $PARAM1 "localhost:1111 dba dba"