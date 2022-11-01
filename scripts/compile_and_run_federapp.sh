#!/bin/bash

# Parameters 3 is for the configuration file which RDF4J need to consedering named graph as endpoint

PARAM2=$2

# Parameters 3 is for the SPARQL query

PARAM3=$3

# Parameters 4 is for the result of the query

PARAM4=$4

# Parameters 5 is for the statistics of the query

PARAM5=$5

# Parameters 6 is for the source selection he used
# Parameters 7 is for the number of HTTP request he do


mkdir -p "$(dirname "$4")" && touch "$4"
mkdir -p "$(dirname "$5")" && touch "$5"
mkdir -p "$(dirname "$6")" && touch "$6"
mkdir -p "$(dirname "$7")" && touch "$7"

cd Federapp
mvn install dependency:copy-dependencies package


if [ "$1" -lt "0" ]; then
  echo "Timeout disabled"
  java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "${@:2}"
else
  echo "Timeout enabled"
  timeout --signal=SIGKILL $1 java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "${@:2}"
  EXIT_STATUS=$?
  echo "EXIT_STATUS = $EXIT_STATUS"
  if [ $EXIT_STATUS -eq 137 ]
  then
      echo 'Process Timed Out!'
      echo "query,exec_time,total_distinct_ss,nb_http_request,total_ss" > "$5"
      echo "$3,failed,failed,failed,failed" >> "$5"
  else
      echo 'Process did not timeout :)'
  fi
fi
