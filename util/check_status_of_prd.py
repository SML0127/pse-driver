import csv
from managers.graph_manager import GraphManager
import hashlib
out_file = open("new_prd.csv", 'w', encoding='utf-8', newline='')
writer = csv.writer(out_file)



#skip brief

graph_manager = GraphManager()
graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")


res = graph_manager.get_mpid_in_job_source_view_using_status(2)
for i in res:
  val = str(i[0]).zfill(7)
  writer.writerow([val])
out_file.close()
#for row in range(len(delivery_charge_list)):
#    delivery_charge_list[row].append(float(dollar2krw))
#    delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

