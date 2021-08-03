from engine.operators import *
from managers.graph_manager import *
from managers.web_manager import *
from managers.redis_manager import *
from rq import Queue, Worker
import subprocess
import traceback
from redis import Redis
from rq.command import send_shutdown_command
from rq.command import send_kill_horse_command
from rq.worker import Worker, WorkerStatus
from price_parser import Price
import re

if __name__ == '__main__':

   # Redis tester
   #redis = Redis()
   #workers = Worker.all(redis)
   ##redis = RedisManager()
   ##settings = {'redis_host': '127.0.0.1' , 'redis_port': '6379', 'redis_queue': 'cafe24_queue' }
   # redis.connect(settings)
   ##rq = redis.create_rq(redis.get_connection(), 'cafe24_queue')
   ##workers = Worker.all(queue=rq)
   # for worker in workers:
   #   print(worker)
   #   #worker.failed_queue.quarantine(job, exc_info=("Dead worker", "Moving job to failed queue"))
   #   worker.register_death() # register worker as dead
   #   if worker.state == WorkerStatus.BUSY:
   #      #print('shut down')
   #      print(worker.pid)
   #      #send_kill_horse_command(redis, worker.name)
   #      #send_shutdown_command(redis, worker.name)  # Tells worker to shutdown

    gvar = GlovalVariable()
    gvar.graph_mgr = GraphManager()
    gvar.graph_mgr.connect(
        "host=141.223.197.35 port=54320 user=pse password=pse dbname=pse")
    try:
        #tmp = gvar.graph_mgr.get_node_properties_from_mysite_for_update(317, 679)
        tmp = gvar.graph_mgr.get_options()
        for op_v in tmp:
            print('------------------------------------------------------------------------')
            print('-------------------')
            print('option value : \t\t\t', op_v)
            print('-------------------')
            start1 =0
            end1 = op_v.find('\n')
            if end1 == -1:
               option_size = op_v
            else:
               option_size = op_v[start1:end1]
            additional_price = '0'
            additional_stock = '999'
            if re.findall( r'([0-9]*\,?\.?[0-9,'']*.€)', op_v):
               additional_price = re.findall( r'([0-9]*\,?\.?[0-9,.]*.€)', op_v)[0]
            if re.findall( r'(Notify Me)', op_v):
               additional_stock = '0'
            if re.findall( r'(Only [0-9]* left)', op_v):
               additional_stock = int(Price.fromstring(re.findall( r'(Only [0-9]* left)', op_v)[0]).amount_float)
            print('smlee option_size: \t\t\t\t', option_size)
            print('smlee regex additional price: \t\t\t', additional_price)
            print('smlee regex & price parser additional price: \t', Price.fromstring(additional_price))
            print('smlee regex Notify Me: \t\t\t\t', additional_stock)
            print('smlee regex Only ? Left: \t\t\t', additional_stock)
            
           
    except Exception as e:
        # print(e)
        print(str(traceback.format_exc()))
        pass
    gvar.graph_mgr.disconnect()
