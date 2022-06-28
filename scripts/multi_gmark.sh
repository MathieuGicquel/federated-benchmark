#!/bin/bash

cd lib/gmark/src/

for (( i=0; i<$1; i++))
do
    if [[ -f "../../../prepa/gmark/data-$i.txt0.txt" ]]
    then
        echo "File data-$i.txt already exist !"
    else
        ./test -c ../../../prepa/use-cases/shop-$i.xml  -g ../../../prepa/gmark/data-$i.txt -w ../../../prepa/gmark/shop-workload-$i.xml -a
    fi
done