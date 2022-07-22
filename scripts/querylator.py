# Import part

import click
import re

# Goal : Translate correctly gMark queries

@click.command()
@click.argument("query_input")
@click.argument("query_output")
@click.argument("source_selection_query_output")

def convert(query_input, query_output,source_selection_query_output):
    with open(query_input) as file:
        with open(f'{query_output}', 'a') as ffile:
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
            ffile.write(query)

        # Get all triples of the query

        triples = list(set(re.findall(r"\?x[0-9]+ \S+ \?x[0-9]+ \.", query)))
        print(triples)

        prefixes = query.split("SELECT")[0]

        # SELECT only named graph of each triples

        source_selection_query = prefixes + "\n" + "SELECT DISTINCT "
        for i in range(0,len(triples)):
            source_selection_query += "?tp" + str(i) + " "
        source_selection_query += "{"

        # Add a GRAPH clause for each triples to get their named graph

        for i in range(0,len(triples)):
            source_selection_query += "GRAPH ?tp" + str(i) + " { " + triples[i] + " } . \n"
        source_selection_query += "}"

        with open(f'{source_selection_query_output}', 'a') as ffile:
            ffile.write(source_selection_query)

if __name__ == "__main__":
    convert()
