import csv

with open('dataset_fixed.csv') as csv_file:
    csv_reader = list(csv.reader(csv_file, delimiter=';'))

m  = {}
i = 1
while i < len(csv_reader):
    row_1 = csv_reader[i]
    ID_1 = row_1[7]
    session_ID = row_1[5]
    j = i+1
    while j < len(csv_reader) and csv_reader[j][5] == session_ID:
        row_2 = csv_reader[j]
        ID_2 = row_2[7]
        if row_1[5] == row_2[5] and ID_1 != ID_2:
            if (ID_1, ID_2) in m:
                m[(ID_1, ID_2)]['w'] += 1
            elif (ID_2, ID_1) in m:
                m[(ID_2, ID_1)]['w'] += 1
            else:
                m[(ID_1, ID_2)] = {'id1':ID_1,'category1':row_1[6],'colour1':row_1[8],'photo1':row_1[10],'price1':row_1[11],
                                   'id2':ID_2,'category2':row_2[6],'colour2':row_2[8],'photo2':row_2[10],'price2':row_2[11],
                                   'w':1}
        j += 1
    i += 1

print("id1;category1;colour1;photo1;price1;id2;category2;colour2;photo2;price2;w")
for i in m:
    print(f"{m[i]['id1']};{m[i]['category1']};{m[i]['colour1']};{m[i]['photo1']};{m[i]['price1']};{m[i]['id2']};{m[i]['category2']};{m[i]['colour2']};{m[i]['photo2']};{m[i]['price2']};{m[i]['w']}")
