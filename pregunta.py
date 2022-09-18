"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re


def load_file(url):
    with open(url, 'r') as file:
        data = file.readlines()
    return data

def clean_data(data):
    data = [line.replace('\n', '') for line in data]
    data = [line.strip() for line in data]
    data = data[4:]
    # data = [line.split('\t') for line in data]
    data1 = []
    line = ''
    for i in data:
        a = re.search(r'^\d', i)
        if a:
            if line == '':
                line = i
            else:
                data1.append(line)
            line = i
        else:
            line = line + ' ' + i
    data1.append(line)
    data = map(clean_line, data1)
    return list(data)

def clean_head(data):
    data = [line.replace('\n', '') for line in data]
    head = data[:4]
    head = [line.strip() for line in data]
    head = [x.split('  ') for x in head]
    head = [[x.strip().lower() for x in line if x != ''] for line in head]
    head = head[:2] 
    [h1, h2] = head
    head = [h1[0], h1[1] + ' ' + h2[0], h1[2] + ' ' + h2[1], h1[3]]
    head = [x.replace(' ', '_') for x in head]
    
    return head

def clean_line(line):
    dat = re.search(r'(^[0-9]+)\W+([0-9]+)\W+([0-9]+)([!#$%&*+-.^_`|~:\[\]]+)(\d+)(\W+)(.+)', line)
    line = dat.group(1) + ';' + dat.group(2) + ';' + dat.group(3) + '.' + dat.group(5) + ';' + dat.group(7) + '\n'
    
    return line

def list_data(data):
    data = [line.replace('\n', '') for line in data]
    data = [data.split(';') for data in data]
    data = [[int(line[0]), int(line[1]), float(line[2]), line[3].split(',')] for line in data]
    data = [[line[0], line[1], line[2], [i.strip()  for i in line[3]]] for line in data]
    data = [[line[0], line[1], line[2], [i.split(' ')  for i in line[3]]] for line in data]
    data = [[line[0], line[1], line[2], [[i.strip() for i in j] for j in line[3]]] for line in data]
    data = [[line[0], line[1], line[2], [[i for i in j if i != ''] for j in line[3]]] for line in data]
    data = [[line[0], line[1], line[2], [' '.join(i) for i in line[3]]] for line in data]
    data = [[line[0], line[1], line[2], ', '.join(line[3])]  for line in data]
    data = [[line[0], line[1], line[2], line[3].replace('.', '')]  for line in data]

    return data


def ingest_data():

    data = load_file('clusters_report.txt')
    head = clean_head(data)
    data = clean_data(data)
    data = list_data(data)
    
    df = pd.DataFrame(data, columns=head)
    
    return df