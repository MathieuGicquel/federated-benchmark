import click
import re
@click.command()
@click.argument("query_input")
@click.argument("query_output")
@click.argument("source_selection_query_output")

def convert(query_input, query_output,source_selection_query_output):
    with open(query_input) as file:
        with open(f'{query_output}', 'a') as ffile:
            query : str = str(file.read())
            if "ASK" in query:
                query = query.replace("ASK", "SELECT *")
                query = query.replace("\n", " LIMIT 1")
                query = query.replace(":p82", "owl:sameAs")
                query = query.replace("((","")
                query = query.replace("))","")
            ffile.write(query)

        triples = re.findall(r"\?x[0-9]+ \(\S+\) \?x[0-9]+ .", query)
        print(triples)

        prefixes = query.split("SELECT")[0]

        source_selection_query = prefixes + "\n" + "SELECT DISTINCT "
        for i in range(0,len(triples)):
            source_selection_query += "?tp" + str(i) + " "
        source_selection_query += "{"

        for i in range(0,len(triples)):
            source_selection_query += "GRAPH ?tp" + str(i) + " { " + triples[i] + " } . \n"
        source_selection_query += "}"

        with open(f'{source_selection_query_output}', 'a') as ffile:
            ffile.write(source_selection_query)




if __name__ == "__main__":
    convert()
