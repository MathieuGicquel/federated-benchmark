#!/bin/bash

PARAM1=$1

PARAM2=$2

PARAM3=$3

PARAM4=$4

mkdir -p "$(dirname "$3")" && touch "$3"
mkdir -p "$(dirname "$4")" && touch "$4"
mkdir -p "$(dirname "$5")" && touch "$5"
mkdir -p "$(dirname "$6")" && touch "$6"

cd Federapp
mvn dependency:copy-dependencies package
java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "$@"
