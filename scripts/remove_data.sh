#!/bin/bash

# Example of use : 
# ./scripts/remove_data.sh  /home/ubuntu/virtuoso-opensource/bin/isql

# The first parameters is where Virtuoso's isql are

PARAM1=$1

# Connect to Virtuoso's isql and get all named graph

LINES=$($PARAM1 localhost:1111 dba dba exec=DB.DBA.SPARQL_SELECT_KNOWN_GRAPHS\(\));

# Delete failed attempt

string="DELETE FROM DB.DBA.load_list; \n"

# Delete just named graph we add

for LINE in $LINES
do
  if [[ "$LINE" =~ ^http://example.org.* ]]; then
      echo $LINE
      string+="DELETE FROM rdf_quad WHERE g = iri_to_id('$LINE'); \n"
  fi
done <<< $LINES

echo $string

echo -e "$string" \
  | eval $PARAM1 "localhost:1111 dba dba"
