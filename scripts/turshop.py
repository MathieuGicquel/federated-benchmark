import click

@click.command()
@click.argument("txt_file")
@click.argument("nb_site")
@click.argument("output")

def convert(txt_file, nb_site, output):
    with open(txt_file) as file:
        with open(f'{output}', 'a') as ffile:
            t_file = file.readlines()
            for line in t_file:
                t_line = line.split()
                subject = t_line[0]
                predicate = t_line[1]
                objecte = t_line[2]

                graph = "<http://example.org/s" + str(int(subject) % int(nb_site)) + ">"
                subject = "<http://example.org/s" + str(int(subject) % int(nb_site)) + "/" + str(subject) + ">"
                predicate = "<http://example.org/p" + str(predicate) + ">"
                objecte = "<http://example.org/s" + str(int(objecte) % int(nb_site)) + "/" + str(objecte) + ">"
                
                ffile.write(f"{subject} {predicate} {objecte} {graph} .\n")

if __name__ == "__main__":
    convert()