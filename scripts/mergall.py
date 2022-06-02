import glob
import csv
import click

@click.command()
@click.argument("input_path")
@click.argument("output_file")

def merge(input_path, output_file):

    #Select files
    files = glob.glob(f'{input_path}/*.csv')
    with open(f'{output_file}') as ffile:
        ffile.write('query,exec_time\n')
    for file in files:
        with open(file) as ifile:
            i = csv.reader(ifile)
            j = 0
            for row in i:
                if j > 0:
                    with open(f'{output_file}'):
                        ffile.write(f'{row[0]},{row[1]}\n')

if __name__ == "__main__":
    merge()