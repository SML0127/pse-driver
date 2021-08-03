import csv

from managers.graph_manager import GraphManager
import hashlib
out_file = open("code_compare1.csv", 'w', encoding='utf-8', newline='')
writer = csv.writer(out_file)



#skip brief

graph_manager = GraphManager()
graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")

with open('/home/pse/pse-driver/code.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    for line in reader:
        #print(graph_manager.compare_checkum_using_mpid(line[0]))
        node_id1, name1, description1 = graph_manager.compare_checkum_using_mpid(line[0])
        #print('-------------------------------------------------')
        node_id2, name2, description2 = graph_manager.compare_checkum_using_mpid(line[1])
        print(line[0], line[1])
        writer.writerow([node_id1,name1,hashlib.sha256(name1.encode()).hexdigest(), node_id2, name2,hashlib.sha256(name2.encode()).hexdigest()])
        writer.writerow([description1,hashlib.sha256(description1.encode()).hexdigest(),description2 , hashlib.sha256(description2.encode()).hexdigest()])
        writer.writerow([])
#out_file.close()
#for row in range(len(delivery_charge_list)):
#    delivery_charge_list[row].append(float(dollar2krw))
#    delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

