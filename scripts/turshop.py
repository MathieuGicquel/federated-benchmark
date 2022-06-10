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
                go2 = predicate
                if predicate == "82":
                    predicate = "<http://www.w3.org/2002/07/owl#sameAs>"
                else:
                    predicate = "<http://example.org/p" + str(predicate) + ">"
                #if int(go2) < 51 :
                if False :
                    if int(go2) in [0,2,3,5,9,10,11,15,19,26,28,29,33,41,46,47,49,50]:
                        objecte = "\"" + str(objecte) + "\""
                    else:
                        objecte = str(objecte)
                else:
                    objecte = "<http://example.org/s" + str(int(objecte) % int(nb_site)) + "/" + str(objecte) + ">"
                
                ffile.write(f"{subject} {predicate} {objecte} {graph} .\n")

if __name__ == "__main__":
    convert()
