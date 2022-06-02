import glob
import csv
import click

@click.command()
@click.argument("input_path")
@click.argument("nb_site")
@click.argument("output_file")

def merge(input_path, nb_site, output_file):

    #Select files
    files = glob.glob(f'{input_path}[1-3]/{nb_site}/*.csv')
    with open(f'{output_file}', 'a') as ffile:
        ffile.write('query,exec_time\n')
    for file in files:
        with open(file) as ifile:
            i = csv.reader(ifile)
            j = 0
            for row in i:
                if j > 0:
                    with open(f'{output_file}', 'a') as ffile:
                        ffile.write(f'{row[0]},{row[1]}\n')
                j += 1

if __name__ == "__main__":
    merge()
