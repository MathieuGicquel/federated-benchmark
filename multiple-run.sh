# rm -rf result/ && rm -rf plot && rm -rf prepa

for i in 10 20 30 40 50 60 70 80 90 100 110 120 130 140 150 160 170 180 190 200
do
 sed -r -i "s/site: [0-9]+/site: $i/g" ./configuration.yaml
 snakemake -c1 -p
done

mkdir -p plot

python3 ./scripts/harry_plotter.py