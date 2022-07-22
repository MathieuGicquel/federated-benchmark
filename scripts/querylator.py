# Import part

import click
import re

# Goal : Translate correctly gMark queries

@click.command()
@click.argument("query_input")
@click.argument("query_output")

def convert(query_input, query_output):
    with open(query_input) as file:
        with open(f'{query_output}', 'w') as ffile:
            query : str = str(file.read())

            # Convert ASK query into SELECT * WHERE { ... } LIMIT 1 query because RDF4J can't support it

            if "ASK" in query:
                query = query.replace("ASK", "SELECT * WHERE")
                query = query.replace("\n", " LIMIT 1")

            # Fix predicate use in gMark query
                
            query = query.replace(":p",":")
            query = query.replace(":sameAs","owl:sameAs")
            query = query.replace("owlowl:","owl:")
            query = query.replace("((","")
            query = query.replace("))","")
            
            # Get all triples of the query and fix some problem of duplicate triples

            triples = list(re.findall(r"\?x[0-9]+ \S+ \?x[0-9]+ \.", query))
            for triple in triples:
                occurence = query.count(triple)
                if occurence > 1:
                    query = query.replace(triple,"",occurence - 1)

            ffile.write(query)

        triples = list(set(re.findall(r"\?x[0-9]+ \S+ \?x[0-9]+ \.", query)))
        print(triples)

if __name__ == "__main__":
    convert()
