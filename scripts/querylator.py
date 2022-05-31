import click

@click.command()
@click.argument("query_input")
@click.argument("query_output")

def convert(query_input, query_output):
    with open(query_input) as file:
        with open(f'{query_output}', 'a') as ffile:
            t_file = str(file.read())
            if "ASK" in t_file:
                t_file = t_file.replace("ASK", "SELECT *")
                t_file = t_file.replace("\n", " LIMIT 1")
            t_file = t_file.replace("/gmark/", "/")
            ffile.write(t_file)

if __name__ == "__main__":
    convert()