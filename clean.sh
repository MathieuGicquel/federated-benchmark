./scripts/remove_data.sh $(cat ./configuration.yaml | grep "isql_virtuoso_path:" | sed 's/.*://' | sed "s/['\"]//g" )
cd Federapp && mvn clean && cd -
rm -rf result/
rm -rf prepa/
rm -rf plot/
rm -rf lib/gmark/demo/shop-a/shop-a-graph.txt0.txt 
rm -rf lib/gmark/demo/shop-a/shop-a-translated/