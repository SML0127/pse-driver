import time
import sys
import traceback
import json
import datetime
from datetime import datetime, timedelta
import argparse

from managers.redis_manager import *
from managers.graph_manager import GraphManager
from managers.settings_manager import *
from functools import partial
print_flushed = partial(print, flush=True)

class Cafe24Driver():

  def __init__(self):
    pass

  def init_managers(self, settings):
    self.redis_manager = RedisManager()
    self.redis_manager.connect(settings)
    self.redis_manager.create_rq(self.redis_manager.get_connection(),settings['redis_queue'])

    setting_manager = SettingsManager()
    setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
    settings = setting_manager.get_settings()
    self.graph_manager = GraphManager()
    print(settings)
    self.graph_manager.init(settings)


  #def wait(self, running_tasks, targetsite, num_product_per_task, mt_history_id):
  def wait(self, running_tasks):
    start_time = time.time()
    successful_tasks, failed_tasks = [], []
    prev_cnt = 0
    while len(running_tasks) > 0:
      indexes = []
      for idx, task in enumerate(running_tasks):
        stat = self.redis_manager.get_status(task)
        if stat == 'finished':
          indexes.append(idx)
          successful_tasks.append(task)
        elif stat == 'failed':
          indexes.append(idx)
          failed_tasks.append(task)
      for val in sorted(indexes, reverse = True):
        running_tasks.pop(val)
      print("### SUCCESSFUL: {}, FAILED: {}, RUNNING: {} ".format(len(successful_tasks), len(failed_tasks), len(running_tasks)))
      #cnt = len(successful_tasks) + len(failed_tasks) - prev_cnt
      #if cnt != 0 and cnt != prev_cnt:
      #  if num_product_per_task != 30:
      #    self.graph_manager.log_expected_num_target_success(mt_history_id, int(cnt) * int(num_product_per_task), targetsite) 
      #    num_product_per_task = 30
      #else:
      #  self.graph_manager.log_expected_num_target_success(mt_history_id, int(cnt) * int(num_product_per_task), targetsite) 
      #prev_cnt = len(successful_tasks) + len(failed_tasks)  
      time.sleep(10)

    for task in successful_tasks:
      if (type(self.redis_manager.get_result(task)) == type({})):
        for key, item in self.redis_manager.get_result(task).items():
          print(key, item)
    print('elapsed time per step:', time.time() - start_time)
    return len(successful_tasks)

  def run(self, args, mpids):
    mt_history_id = -1
    #smlee
    sm_history_id = self.graph_manager.get_latest_sm_history_id(args['job_id']) 
    #sm_history_id = -1
    try:
      targetsite_label_encode = self.graph_manager.get_targetsite_label_encode(args['tsid'])
      targetsite_label = self.graph_manager.get_targetsite_label(args['tsid'])
      utcnow = datetime.utcnow()
      time_gap = timedelta(hours=9)
      cur_time = datetime.utcnow() + time_gap
      cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
      mt_history_id = self.graph_manager.insert_mt_history(targetsite_label_encode, cur_time, args['job_id'], sm_history_id)

      running_tasks = []
      
      num_product_per_task = 30
      print("# of task "+str(int(len(mpids))))
      end = int(len(mpids) / num_product_per_task) + 1
      print("end "+str(int(end)))
      num_threads_per_worker = args['num_threads']

      num_workers, max_num_workers = 0, args['max_num_workers']

      clients = args['clients']

      if len(clients) < max_num_workers * num_threads_per_worker:
        print("The clients are not enough")
        raise

      clients_per_worker = []
      for i in range(max_num_workers):
        clients_per_worker.append(clients[i * num_threads_per_worker: (i + 1) * num_threads_per_worker])
    
      #total_num_s = 0
      time_gap = timedelta(hours=9)
      cur_time = datetime.utcnow() + time_gap
      cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
      self.graph_manager.re_log_to_job_current_targetsite_working('\n{}\n[Start] Uploading / Update / Delete items to {}'.format(cur_time, targetsite_label), args['job_id']) 
      num_task_in_job = -1
      self.graph_manager.log_expected_num_target_all(mt_history_id, len(mpids), targetsite_label_encode)
      for idx in range(end):
        job = {}
        job['mpids'] = mpids[(idx)*num_product_per_task:(idx+1)*num_product_per_task]
        num_task_in_job = len(job['mpids'])
        job['args'] = args.copy()
        job['args']['clients'] = clients_per_worker[num_workers]
        job['args']['mt_history_id'] = mt_history_id 
        job['args']['targetsite_encode'] = targetsite_label_encode
        #print(job)
        running_tasks.append(self.redis_manager.enqueue(job))
        num_workers += 1
        if num_workers == max_num_workers:
          #num_s = self.wait(running_tasks, targetsite_label_encode, num_task_in_job, mt_history_id)
          num_s = self.wait(running_tasks)
          #total_num_s += num_s
          num_workers = 0
          cur_time = datetime.utcnow() + time_gap
          cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
          self.graph_manager.log_to_job_current_targetsite_working('\n{}\n[Running] Uploading / Update / Delete items to {}'.format(cur_time, targetsite_label), args['job_id']) 
      #num_s = self.wait(running_tasks, targetsite_label_encode, num_task_in_job, mt_history_id)
      num_s = self.wait(running_tasks)
      #total_num_s += num_s
      cur_time = datetime.utcnow() + time_gap
      cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
      self.graph_manager.log_to_job_current_targetsite_working('\n{}\n[Finished] Uploading / Update / Delete items to {}'.format(cur_time, targetsite_label), args['job_id']) 
    except Exception as e:
      print("-------Raised Exception in DRIVER-------")
      print(e)
      print("---------------------------------------")
      print("--------------STACK TRACE--------------")
      print(str(traceback.format_exc()))
      print("---------------------------------------")
    finally:
      utcnow = datetime.utcnow()
      time_gap = timedelta(hours=9)
      cur_time = datetime.utcnow() + time_gap
      cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
      self.graph_manager.update_mt_history(cur_time, mt_history_id)
      pass

  def run_from_db(self, args):
    pass

  def run_from_file(self, args):
    label, exec_id = args['label'], args['execution_id']
    start_time = time.time()
    node_ids = self.graph_manager.find_nodes_of_execution_with_label(exec_id, label)
    print("num of nodes: ", len(node_ids))
    #print(args)
    self.run(args, node_ids)
    print("num of nodes: ", len(node_ids))
    print("elapsed time: ", time.time() - start_time)



  def run_from_mpid(self, args):
    start_time = time.time()
    mpids = args['mpids'] 
    job_id = args['job_id'] 
    utcnow = datetime.utcnow()
    time_gap = timedelta(hours=9)
    kor_time = utcnow + time_gap
    self.graph_manager.update_last_mt_date_in_job_configuration(datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S"), job_id)
    print("num of item to upload / update / deleted: ", len(mpids))
    #print(args)
    args['selected'] = True
    self.run(args, mpids)
    print("elapsed time: ", time.time() - start_time)



  def run_onetime_from_ui(self, args):
    start_time = time.time()

    job_id = args['job_id']
    #target_ids = args['target_id']
    targetsite_ids = self.graph_manager.get_targetsite_id_using_job_id(job_id)
    print(targetsite_ids)
    #targetsite_ids = [36]
    input_args = args
    for tsid in targetsite_ids:
      #smlee
      exec_id = self.graph_manager.get_latest_eid_from_job_id(job_id)
      
      #label = self.graph_manager.get_max_label_from_eid(exec_id)
      max_items = self.graph_manager.get_max_items_from_tsid(tsid)
      #mpids = self.graph_manager.get_mpid_from_mysite_without_up_to_date(job_id, max_items)
      mpids = self.graph_manager.get_mpid_from_mysite(job_id, max_items)

      #mpids = self.graph_manager.get_mpid_from_mysite(job_id, max_items)
      #mpids = [1364, 1368, 262004]
      targs = {}
      targs = json.loads(self.graph_manager.get_selected_gateway_configuration_program_onetime(tsid))
      targs.update(input_args)
      args = targs
   
      args['onetime'] = True
      args['code'] = self.graph_manager.get_selected_transformation_program_onetime(tsid)
      args['tsid'] = tsid
      args['execution_id'] = exec_id
      args['num_threads'] = 1#self.graph_manager.get_num_threads_in_job_configuration_onetime(job_id)
      args['max_num_workers'] =10#self.graph_manager.get_num_worker_in_job_configuration_onetime(job_id)

      utcnow = datetime.utcnow()
      time_gap = timedelta(hours=9)
      kor_time = utcnow + time_gap
      self.graph_manager.update_last_mt_date_in_job_configuration(datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S"), job_id, tsid)
      print("num of item to upload / update / deleted: ", len(mpids))
  
      self.run(args, mpids)
      print("elapsed time: ", time.time() - start_time)


  def run_scheduled_upload(self, args):
    start_time = time.time()

    job_id = args['job_id']
    tsid = args['target_id']
    exec_id = self.graph_manager.get_latest_eid_from_job_id(job_id)
    
    max_items = self.graph_manager.get_max_items_from_tsid(tsid)
    #mpids = self.graph_manager.get_mpid_from_mysite_without_up_to_date(job_id, max_items)
    mpids = self.graph_manager.get_mpid_from_mysite(job_id, max_items)

    targs = json.loads(self.graph_manager.get_selected_gateway_configuration_program_onetime(tsid))
    targs.update(args)
    args = targs

    
    args['code'] = self.graph_manager.get_selected_transformation_program_onetime(tsid)
    args['execution_id'] = exec_id
    args['tsid'] = tsid
    args['num_threads'] = 1#self.graph_manager.get_num_threads_in_job_configuration_onetime(job_id)
    args['max_num_workers'] = 10#self.graph_manager.get_num_worker_in_job_configuration_onetime(tsid)

    utcnow = datetime.utcnow()
    time_gap = timedelta(hours=9)
    kor_time = utcnow + time_gap
    self.graph_manager.update_last_mt_date_in_job_configuration(datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S"), job_id, tsid)
    print("num of item to upload / update / deleted: ", len(mpids))
    #print(args)
    self.run(args, mpids)
    print("elapsed time: ", time.time() - start_time)




  def run_from_file_from_mpid(self, args):
    label, exec_id = args['label'], args['execution_id']
    start_time = time.time()
    tsid = args['target_id']
    max_items = self.graph_manager.get_max_items_from_tsid(tsid)
    #mpids = self.graph_manager.get_mpid_from_mysite_without_up_to_date(args['job_id'], max_items)
    mpids = self.graph_manager.get_mpid_from_mysite(args['job_id'], max_items)
    job_id = self.graph_manager.get_job_id_from_eid(exec_id)
    utcnow = datetime.utcnow()
    time_gap = ti
    self.run(args, mpids)
    print("elapsed time: ", time.time() - start_time)

  #def single_run_from_file(self, args):
  #  start_time = time.time()
  #  uploader = Cafe24SingleUploader()
  #  uploader.upload_products(args)
  #  print("elapsed time: ", time.time() - start_time)

  def execute(self, args):
    try:
      parser = argparse.ArgumentParser()
      parser.add_argument('--c', required=False, help='')
      parser.add_argument('--eid', required=False)
      parser.add_argument('--wf', required=False)
      parser.add_argument('--ct', required=False)
      parser.add_argument('--cno', required=False)
      parser.add_argument('--url', required=False)
      parser.add_argument('--wfn', required=False)
      parser.add_argument('--ctn', required=False)
      parser.add_argument('--job_id', required=False)
      parser.add_argument('--target_id', required=False)
      parser.add_argument('--target_ids',nargs='+', required=False)
      parser.add_argument('--mpids',nargs='+', required=False)
      parser.add_argument('--max_page', required=False)
      parser.add_argument('--cafe24_c', required=False, help='')
      parser.add_argument('--cafe24_eid', required=False)
      parser.add_argument('--cafe24_label', required=False)
      parser.add_argument('--cafe24_host', required=False)
      parser.add_argument('--cafe24_port', required=False)
      parser.add_argument('--cafe24_queue', required=False)
      parser.add_argument('--cafe24_code', required=False)
      parser.add_argument('--cafe24_mall', required=False)
      sys_args, unknown = parser.parse_known_args()
      if sys_args.cafe24_mall != None:
        f = open(sys_args.cafe24_mall)
        targs = json.load(f)
        targs.update(args)
        args = targs
        f.close()
      if sys_args.cafe24_code != None:
        f = open(sys_args.cafe24_code)
        args['code'] = f.read()
        f.close()

      if sys_args.cafe24_eid != None: args['execution_id'] = sys_args.cafe24_eid
      if sys_args.url != None: args['url'] = sys_args.url
      if sys_args.cafe24_label != None:
        args['label'] = sys_args.cafe24_label
      if sys_args.job_id != None:
        args['job_id'] = sys_args.job_id
      if sys_args.mpids != None:
        args['mpids'] = sys_args.mpids
      if sys_args.target_id != None:
        args['target_id'] = sys_args.target_id
      if sys_args.cafe24_c == 'run':
        self.init_managers({'redis_host': sys_args.cafe24_host, 'redis_port': sys_args.cafe24_port, 'redis_queue': sys_args.cafe24_queue})
        self.run_from_file_from_mpid(args)
      elif sys_args.cafe24_c == 'run_scheduled':
        self.init_managers({'redis_host': sys_args.cafe24_host, 'redis_port': sys_args.cafe24_port, 'redis_queue': sys_args.cafe24_queue})
        self.run_scheduled_upload(args)
      elif sys_args.cafe24_c == 'run_onetime_from_ui':
        self.init_managers({'redis_host': sys_args.cafe24_host, 'redis_port': sys_args.cafe24_port, 'redis_queue': sys_args.cafe24_queue})
        self.run_onetime_from_ui(args)
      elif sys_args.cafe24_c == 'run_mpid':
        self.init_managers({'redis_host': sys_args.cafe24_host, 'redis_port': sys_args.cafe24_port, 'redis_queue': sys_args.cafe24_queue})
        self.run_from_mpid(args)
      #elif sys_args.cafe24_c == 'single_run':
      #  self.single_run_from_file(args)
    except Exception as e:
      print(str(traceback.format_exc()))
      raise e

if __name__ == "__main__":
  driver = Cafe24Driver()
  driver.execute({})

