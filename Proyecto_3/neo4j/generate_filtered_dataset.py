import sys
import csv

with open('dataset_fixed.csv') as csv_file:
    csv_reader = list(csv.reader(csv_file, delimiter=';'))

with open('topological_metrics_4.csv') as csv_file:
    metrics = list(csv.reader(csv_file, delimiter=','))

# Calcular el numero de clicks para cada producto
m = {}
filter_set = set()

i = 1
while i < len(csv_reader):
    product_id = csv_reader[i][7]
    if product_id in m:
        m[product_id]['clicks'] += 1
    else:
        m[product_id] = {}
        m[product_id]['clicks'] = 1
    i += 1

# Cargar datos de medidas topologicas para cada producto
j = 1
while j < len(metrics):
    product_id = metrics[j][0]

#    m[product_id]['deg'] = metrics[j][1]
#    m[product_id]['pagerank'] = metrics[j][2]
#    m[product_id]['comm_id'] = metrics[j][3]
#    m[product_id]['triangle_count'] = metrics[j][4]
#    m[product_id]['x'] = metrics[j][5]
#    m[product_id]['y'] = metrics[j][6]
#    m[product_id]['z'] = metrics[j][7]

#    m[product_id]['comm_id'] = metrics[j][1]
#    m[product_id]['x1'] = metrics[j][2]
#    m[product_id]['x2'] = metrics[j][3]
#    m[product_id]['x3'] = metrics[j][4]
#    m[product_id]['x4'] = metrics[j][5]
#    m[product_id]['x5'] = metrics[j][6]

#    m[product_id]['comm_id'] = metrics[j][1]
#    m[product_id]['x'] = metrics[j][2]
#    m[product_id]['y'] = metrics[j][3]

    m[product_id]['louvain_comm'] = metrics[j][1]
    m[product_id]['clustering_coeff'] = metrics[j][2]
    m[product_id]['betweenness'] = metrics[j][3]
    m[product_id]['closeness'] = metrics[j][4]
    m[product_id]['x'] = metrics[j][5]
    m[product_id]['y'] = metrics[j][6]
    m[product_id]['z'] = metrics[j][7]

    filter_set.add(product_id)
    j += 1

if sys.argv[1] == '0':
    print(";".join(csv_reader[0][1:5]+csv_reader[0][6:7]+csv_reader[0][8:]) + ";clicks")
elif sys.argv[1] == '1':
    print(";".join(csv_reader[0][1:5]+csv_reader[0][6:7]+csv_reader[0][8:]) + ";" + ";".join(metrics[0][1:]) + ";clicks")

for row in csv_reader[1:]:
    product_id = row[7]
    if product_id in filter_set:
        if sys.argv[1] == '0':
            print(";".join(row[1:5]+row[6:7]+row[8:])+f";{m[product_id]['clicks']}")
        elif sys.argv[1] == '1':
#            print(";".join(row)+f";{m[product_id]['deg']};{m[product_id]['pagerank']};{m[product_id]['comm_id']};{m[product_id]['triangle_count']};{m[product_id]['x']};{m[product_id]['y']};{m[product_id]['z']};{m[product_id]['clicks']}")
            print(";".join(row[1:5]+row[6:7]+row[8:])+f";{m[product_id]['louvain_comm']};{m[product_id]['clustering_coeff']};{m[product_id]['betweenness']};{m[product_id]['closeness']};{m[product_id]['x']};{m[product_id]['y']};{m[product_id]['z']};{m[product_id]['clicks']}")
