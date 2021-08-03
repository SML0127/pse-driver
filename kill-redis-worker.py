from rq import Queue, Worker
from redis import Redis
from rq.command import send_shutdown_command
from rq.command import send_kill_horse_command
from rq.worker import Worker, WorkerStatus
import sys


if __name__ == '__main__':

   redis = Redis()
   workers = Worker.all(redis)
   worker_name = sys.argv[1]
   if worker_name == 'all':
      for worker in workers:
         print("Kill worker name: ", worker.name)
         send_kill_horse_command(redis, worker.name)
         send_shutdown_command(redis, worker.name)  # Tells worker to shutdown
         worker.register_death()

   else:
      for worker in workers:
         if worker_name == worker.name:
             print(worker.name)
             print(worker.state)
             print("Kill worker name: ", worker.name)
             send_kill_horse_command(redis, worker.name)
             # shut down worker
             send_shutdown_command(redis, worker.name)  # Tells worker to shutdown
             worker.register_death()
