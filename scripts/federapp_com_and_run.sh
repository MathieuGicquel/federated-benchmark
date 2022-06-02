#!/bin/bash

PARAM1=$1

PARAM2=$2

PARAM3=$3

PARAM4=$4

#mkdir -p "$(dirname "$3")" && touch "$3"
#mkdir -p "$(dirname "$4")" && touch "$4"

cd Federapp
mvn dependency:copy-dependencies package
java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp $PARAM1 $PARAM2 $PARAM3 $PARAM4