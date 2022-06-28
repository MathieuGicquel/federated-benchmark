#!/bin/bash

# ./scripts/digestuoso.sh  /home/ubuntu/virtuoso-opensource/bin/isql

PARAM1=$1

LINES=$($PARAM1 localhost:1111 dba dba exec=DB.DBA.SPARQL_SELECT_KNOWN_GRAPHS\(\));

string="DELETE FROM DB.DBA.load_list; \n"

for LINE in $LINES
do
  if [[ "$LINE" =~ ^http://example.org/.* ]]; then
      echo $LINE
      string+="DELETE FROM rdf_quad WHERE g = iri_to_id('$LINE'); \n"
  fi
done <<< $LINES

echo $string

echo -e "$string" \
  | eval $PARAM1 "localhost:1111 dba dba"

