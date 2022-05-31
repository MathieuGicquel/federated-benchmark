import click

@click.command()
@click.argument("data_file")
@click.argument("config_file")
@click.argument("endpoint")

def convert(data_file, config_file, endpoint):
    with open(data_file) as file:
        with open(f'{config_file}', 'a') as ffile:
            t_file = file.readlines()
            ssite = set()
            for line in t_file:
                site = line.split()[3]
                site = site.replace("<", "")
                site = site.replace(">", "")
                ssite.add(site)
            ffile.write(
"""
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix fedx: <http://rdf4j.org/config/federation#> .

"""
                )
            for s in ssite:
                ffile.write(
f"""
<{s}> a sd:Service ;
    fedx:store "SPARQLEndpoint";
    sd:endpoint "{endpoint}/?default-graph-uri={s}";
    fedx:supportsASKQueries false .   

"""
                )

if __name__ == "__main__":
    convert()