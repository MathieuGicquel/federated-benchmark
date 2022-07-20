
for i in 20 40 60 80 100
do
 sed -r -i "s/site: [0-9]+/site: $i/g" ./configuration.yaml
 snakemake -c1 -p
done

mkdir -p plot

python3 ./scripts/harry_plotter.py