#!/bin/bash

# Goal : Generate one gMark for each site and merge shop-workload.xml into 1 and then generate queries from this file

# Compile gMark at first
cd lib/gmark/demo/scripts
./compile-all.sh
cd - 
cd lib/gmark/src/

NB_SITE=$1

for (( i=0; i<$NB_SITE; i++))
do
    if [[ -f "../../../prepa/$NB_SITE/gmark/data-$i.txt0.txt" ]]
    then
        echo "File data-$i.txt already exist !"
    else
        ./test -c ../../../prepa/$NB_SITE/use-cases/shop-$i.xml  -g ../../../prepa/$NB_SITE/gmark/data-$i.txt -w ../../../prepa/$NB_SITE/gmark/shop-workload-$i.xml -a || exit 1
    fi
done

for (( i=0; i<$NB_SITE; i++ ))
do
    sed -i '1,1d' ../../../prepa/$NB_SITE/gmark/shop-workload-$i.xml
    sed -i '$d' ../../../prepa/$NB_SITE/gmark/shop-workload-$i.xml
done

mkdir -p ../../../prepa/$NB_SITE/workload

echo "<queries>" > ../../../prepa/$NB_SITE/workload/shop-workload.xml
cat ../../../prepa/$1/gmark/*.xml >> ../../../prepa/$NB_SITE/workload/shop-workload.xml
echo "</queries>" >> ../../../prepa/$NB_SITE/workload/shop-workload.xml

mkdir -p ../../../prepa/$NB_SITE/queries

cd querytranslate

if $2 ; then cp ../../../../lib/watdiv-queries/* ../../../../prepa/$NB_SITE/queries ; else ./test -w ../../../../prepa/$NB_SITE/workload/shop-workload.xml -o ../../../../prepa/$NB_SITE/queries ; fi
