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



if __name__ == '__main__':

   redis = Redis()
   #workers = Worker.all(redis)
   redis = RedisManager()
   settings = {'redis_host': '127.0.0.1' , 'redis_port': '6379', 'redis_queue': 'cafe24_queue' }
   redis.connect(settings)
   rq = redis.create_rq(redis.get_connection(), 'cafe24_queue')
   workers = Worker.all(queue=rq)
   for worker in workers:
      #print(worker)
      #worker.failed_queue.quarantine(job, exc_info=("Dead worker", "Moving job to failed queue"))
      # modify worker state as dead
      #worker.register_death()
      #print(worker.queues)
      print(worker.name)
      #print(worker.state)
      #if 'upload36_1' in worker.name:
      #print(worker.pid)
      #print(worker.hostname)
      # delete job that worker processing (job is fail)
      #send_kill_horse_command(redis, worker.name)
      # shut down worker
      #send_shutdown_command(redis, worker.name)  # Tells worker to shutdown
      worker.register_death()


  #gvar = GlovalVariable()
  #gvar.graph_mgr = GraphManager()
  #gvar.graph_mgr.connect("host=141.223.197.35 port=54320 user=pse password=pse dbname=pse")
  #gvar.graph_mgr.delete_stock_zero_in_mysite()
  #redis_manager = RedisManager()
  #settings = {'redis_host': '127.0.0.1' , 'redis_port': '6379', 'redis_queue': 'real_queue' }
  #redis_manager.connect(settings)
  #rq = redis_manager.create_rq(redis_manager.get_connection(),settings['redis_queue'])
  #try:
  #   #print(gvar.graph_mgr.get_shipping_fee('A Company'))
  #   #workers = Worker.all(connection=redis_manager.get_connection)
  #   #subprocess.Popen("ssh -p 20220 pse@141.223.197.34 'cd /home/pse/PSE-engine; /home/pse/.pyenv/shims/rq worker  --url redis://141.223.197.33:63790 -n worker_34_1 -w engine.pse_worker.pseWorker --job-class engine.pse_job.pseJob real_queue'" , shell=True)
  #   workers = Worker.all(queue=rq)
  #   for worker in workers:
  #      print(worker)
  #      print(worker.name)
  #      print(worker.state)
  #      print(worker.pid)
  #      print(worker.ip)
  #      #print("ssh -p 20220 pse@141.223.197.34 'kill -8 {}'".format(worker.pid))
  #      #subprocess.Popen("ssh -p 20220 pse@141.223.197.34 'kill -8 {}'".format(worker.pid) , shell=True)
  #      print('killllllllllll')

  #except Exception as e:
  #  #print(e)
  #  print(str(traceback.format_exc()))
  #  pass
  #gvar.graph_mgr.disconnect()
