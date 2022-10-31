#!/bin/bash

# Parameters 1 is for the configuration file which RDF4J need to consedering named graph as endpoint

PARAM1=$1

# Parameters 2 is for the SPARQL query

PARAM2=$2

# Parameters 3 is for the result of the query

PARAM3=$3

# Parameters 4 is for the statistics of the query

PARAM4=$4

# Parameters 5 is for the source selection he used
# Parameters 6 is for the number of HTTP request he do
# Parameters 7 is for the log file of the query's execution

mkdir -p "$(dirname "$3")" && touch "$3"
mkdir -p "$(dirname "$4")" && touch "$4"
mkdir -p "$(dirname "$5")" && touch "$5"
mkdir -p "$(dirname "$6")" && touch "$6"

cd Federapp
mvn install dependency:copy-dependencies package

timeout --signal=SIGKILL 1 java -Xmx32384m -Xms32384m -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "$@"
EXIT_STATUS=$?
echo "EXIT_STATUS = $EXIT_STATUS"
if [ $EXIT_STATUS -eq 137 ]
then
    echo 'Process Timed Out!'
    echo "query,exec_time,total_distinct_ss,nb_http_request,total_ss " > "$4"
    echo "$2,failed,failed,failed,failed" >> "$4"
else
    echo 'Process did not timeout :)'
fi
