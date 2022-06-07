for i in 10 50 100
do
 sed -r -i "" "s/site: [0-9]+/site: $i/g" ./configuration.yaml
 snakemake -c1 -p
done
