import csv
from managers.graph_manager import GraphManager
import hashlib
out_file = open("code.csv", 'w', encoding='utf-8', newline='')
writer = csv.writer(out_file)



#skip brief

graph_manager = GraphManager()
graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")

with open('/home/pse/PSE-engine/code_res.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    for line in reader:
        print(manager.compare_checkum_using_mpid(line[0]))
        print(manager.compare_checkum_using_mpid(line[1]))
        
        #writer.writerow([str(line[0]), str(graph_manager.get_url_using_mpid(int(line[0][4:]))[0][0])])
out_file.close()
#for row in range(len(delivery_charge_list)):
#    delivery_charge_list[row].append(float(dollar2krw))
#    delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

