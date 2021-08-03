import csv
from managers.graph_manager import GraphManager
import hashlib


out_file = open("bag_shippin_in-1.csv", 'w', encoding='utf-8', newline='')
writer = csv.writer(out_file)



#skip brief

graph_manager = GraphManager()
graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")

mpids = graph_manager.get_deleted_mpid() # list

with open('/home/pse/pse-driver/bag-1.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    for line in reader:
        if int(line[1][4:]) in mpids: 
           writer.writerow([line[0], 'Y', 'Y'])


out_file.close()

