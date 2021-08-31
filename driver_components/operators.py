import traceback
import time
import subprocess
from datetime import datetime, timedelta
from managers.graph_manager import GraphManager
from managers.settings_manager import *
from functools import partial
print_flushed = partial(print, flush=True)

class Program():
  pass

class OpenURL():

  def run0(self, rm, results):

    task = self.props
    task['parent_task_id'] = 0
    task['parent_node_id'] = 0
    task['is_detail'] = False
    #print_flushed(task)
    running_task = rm.enqueue(task)
    stat = False
    while True:
      stat = rm.get_status(running_task)
      #print_flushed(stat)
      if stat == 'finished': 
        stat = True
        break
      elif stat == 'failed': 
        stat = False
        break
      time.sleep(1)
    print_flushed('### STAGE {} - {}'.format(self.props['id'], 'success' if stat else 'fail'))
    if stat and type(rm.get_result(running_task)) == type({}):
      #print_flushed(rm.get_result(running_task))
      for key, item in rm.get_result(running_task).items():
        results[key] = results.get(key, []) + item

  def rerun(self, rm, results, previous_tasks):
    if len(previous_tasks) == 0: return
    task = self.props
    task['parent_task_id'] = 0
    task['parent_node_id'] = 0
    previous_task = previous_tasks[0]
    task['previous_task_id'] = previous_task[0]
    task['url'] = previous_task[1]
    running_task = rm.enqueue(task)
    stat = False
    while True:
      stat = rm.get_status(running_task)
      #print_flushed(stat)
      if stat == 'finished':
        stat = True
        break
      elif stat == 'failed':
        stat = False
        break
      time.sleep(1)
    print_flushed('### STAGE {} - {}'.format(self.props['id'], 'success' if stat else 'fail'))
    if stat and type(rm.get_result(running_task)) == type({}):
      #print_flushed(rm.get_result(running_task))
      for key, item in rm.get_result(running_task).items():
        results[key] = results.get(key, []) + item

  def run(self, rm, results, previous_tasks):
    if previous_tasks == None:
      self.run0(rm,results)
    else:
      self.rerun(rm, results, previous_tasks)

class BFSIterator():

  def wait(self, rm, running_tasks, results, graph_manager, exec_id):
    try:
      successful_tasks, failed_tasks = [], []
      prev_cnt = 0
      while len(running_tasks) > 0:
        indexes = []
        for idx, task in enumerate(running_tasks):
          stat = rm.get_status(task)
          if stat == 'finished':
            indexes.append(idx)
            successful_tasks.append(task)
          elif stat == 'failed':
            indexes.append(idx)
            failed_tasks.append(task)
        for val in sorted(indexes, reverse = True):
          running_tasks.pop(val)
        print_flushed("### STAGE {} - SUCCESSFUL: {}, FAILED: {}, RUNNING: {} ".format(self.props['id'], len(successful_tasks), len(failed_tasks), len(running_tasks)))
        cnt = len(successful_tasks) + len(failed_tasks) - prev_cnt
        graph_manager.log_expected_num_summary_success(exec_id, cnt)
        prev_cnt = len(successful_tasks) + len(failed_tasks)  
        time.sleep(10)
  
      for stask in successful_tasks:
        task_result = rm.get_result(stask)
        if (type(task_result) == type({})):
          for key, item in task_result.items():
            results[key] = results.get(key, []) + item
  
    except:
      raise
    return len(successful_tasks), len(failed_tasks)


  def wait_detail_pagination(self, rm, running_tasks, results, graph_manager, job_id, exec_id):
    try:
      successful_tasks, failed_tasks = [], []
      prev_cnt = 0
      while len(running_tasks) > 0:
        indexes = []
        for idx, task in enumerate(running_tasks):
          stat = rm.get_status(task)
          if stat == 'finished':
            indexes.append(idx)
            successful_tasks.append(task)
          elif stat == 'failed':
            indexes.append(idx)
            failed_tasks.append(task)
            graph_manager.update_is_error(job_id)
          #else:
          #  print_flushed(stat)
        for val in sorted(indexes, reverse = True):
          running_tasks.pop(val)
        print_flushed("### STAGE {} - SUCCESSFUL: {}, FAILED: {}, RUNNING: {} ".format(self.props['id'], len(successful_tasks), len(failed_tasks), len(running_tasks)))
        cnt = len(successful_tasks) + len(failed_tasks) - prev_cnt
        graph_manager.log_expected_num_detail_success(exec_id, cnt) 
        prev_cnt = len(successful_tasks) + len(failed_tasks)  
        time.sleep(10)

      for stask in successful_tasks:
        task_result = rm.get_result(stask)
        if (type(task_result) == type({})):
          for key, item in task_result.items():
            results[key] = results.get(key, []) + item
    except:
      raise
    return len(successful_tasks), len(failed_tasks)



  # for detail page pagination
  def run0(self, rm, results):
    job_id = int(self.props.get('job_id', 0))
    setting_manager = SettingsManager()
    setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
    settings = setting_manager.get_settings()
    graph_manager = GraphManager()
    graph_manager.init(settings)

    max_num_tasks = int(self.props.get('max_num_tasks', -1))
    max_num_local_tasks = int(self.props.get('max_num_local_tasks', -1))
    task = self.props
    running_tasks, num_tasks = [], 0
    input_op_id = task['input']
    exec_id = task['execution_id']
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 100)
    total_num_s = 0
    
    time_gap = timedelta(hours=9)
    cur_time = datetime.utcnow() + time_gap 
    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    graph_manager.add_is_error(job_id)
    url_cnt = 0
    for (parent_task_id, parent_node_id, urls) in results.get(input_op_id, []):
      url_cnt = url_cnt + len(urls)
    if max_num_tasks != -1 and max_num_tasks < url_cnt:
      url_cnt = max_num_tasks
    graph_manager.log_expected_num_detail(exec_id, url_cnt) 
    graph_manager.re_log_to_job_current_crawling_working('{}\n[Running] Crawled 0 items (# of expected items = {})'.format(cur_time, url_cnt), job_id) 
    for (parent_task_id, parent_node_id, urls) in results.get(input_op_id, []):
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = parent_task_id
      task['parent_node_id'] = parent_node_id
      num_local_tasks = 0
      for url in urls:
        if max_num_tasks > -1 and num_tasks >= max_num_tasks:
          break
        if max_num_local_tasks > -1 and num_local_tasks >= max_num_local_tasks:
          break
        task['url'] = url
        running_tasks.append(rm.enqueue(task))
        num_tasks += 1
        num_local_tasks += 1
        chunk_size += 1
        if chunk_size == max_chunk_size:
          chunk_size = 0
          num_s, num_f = self.wait_detail_pagination(rm, running_tasks, results, graph_manager, job_id, exec_id)
          total_num_s += num_s
          cur_time = datetime.utcnow() + time_gap 
          cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
          graph_manager.log_to_job_current_crawling_working('\n{}\n[Running] Crawled {} items'.format(cur_time, total_num_s), job_id) 
    if task['input'] in results: del results[task['input']]
    num_s, num_f = self.wait_detail_pagination(rm, running_tasks, results, graph_manager, job_id, exec_id)
    total_num_s += num_s
    cur_time = datetime.utcnow() + time_gap 
    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    graph_manager.log_to_job_current_crawling_working('\n{}\n[Finished] Crawled {} items'.format(cur_time, total_num_s), job_id) 

  def rerunOLD(self, rm, results, previous_tasks):
    if len(previous_tasks) == 0: return
    max_num_tasks = -1
    max_num_local_tasks = -1
    task = self.props
    running_tasks, num_tasks = [], 0
    input_op_id = task['input']
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 100)
    for (previous_task_id, url) in previous_tasks:
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = -1
      task['parent_node_id'] = -1
      task['previous_task_id'] = previous_task_id
      task['url'] = url
      running_tasks.append(rm.enqueue(task))
      chunk_size += 1
      if chunk_size == max_chunk_size:
        chunk_size = 0
        self.wait(rm, running_tasks, results)
    self.wait(rm, running_tasks, results)



  def rerun(self, rm, results, previous_tasks):
    if len(previous_tasks) == 0: return
    job_id = int(self.props.get('job_id', 0))
    setting_manager = SettingsManager()
    setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
    settings = setting_manager.get_settings()
    graph_manager = GraphManager()
    graph_manager.init(settings)

    max_num_tasks = -1
    max_num_local_tasks = -1 
    task = self.props
    running_tasks, num_tasks = [], 0
    input_op_id = task['input']
    exec_id = task['execution_id']
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 100)
    total_num_s = 0
    
    time_gap = timedelta(hours=9)
    cur_time = datetime.utcnow() + time_gap 
    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    graph_manager.add_is_error(job_id)
    url_cnt = len(previous_tasks)

    
    graph_manager.log_expected_num_detail(exec_id, url_cnt)
    #print(previous_tasks)
    #print(len(previous_tasks))
    #print('{}\n[Running] Crawled 0 items (# of expected items = {})'.format(cur_time, url_cnt))
    graph_manager.re_log_to_job_current_crawling_working('{}\n[Running] Crawled 0 items (# of expected items = {})'.format(cur_time, url_cnt), job_id) 

    for (previous_task_id, url) in previous_tasks:
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = -1
      task['parent_node_id'] = -1
      task['previous_task_id'] = previous_task_id
      task['url'] = url
      running_tasks.append(rm.enqueue(task))
      chunk_size += 1
      if chunk_size == max_chunk_size:
        chunk_size = 0
        num_s, num_f = self.wait_detail_pagination(rm, running_tasks, results, graph_manager, job_id, exec_id)
        total_num_s += num_s
        cur_time = datetime.utcnow() + time_gap 
        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
        graph_manager.log_to_job_current_crawling_working('\n{}\n[Running] Crawled {} items'.format(cur_time, total_num_s), job_id) 
    if task['input'] in results: del results[task['input']]
    num_s, num_f = self.wait_detail_pagination(rm, running_tasks, results, graph_manager, job_id, exec_id)
    print(num_s, num_f)
    total_num_s += num_s
    cur_time = datetime.utcnow() + time_gap 
    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    graph_manager.log_to_job_current_crawling_working('\n{}\n[Finished] Crawled {} items'.format(cur_time, total_num_s), job_id) 




  # for summary page pagination
  def run1(self, rm, results):

    setting_manager = SettingsManager()
    setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
    settings = setting_manager.get_settings()
    graph_manager = GraphManager()
    graph_manager.init(settings)

    max_num_tasks = int(self.props.get('max_num_tasks', -1))
    max_num_local_tasks = int(self.props.get('max_num_local_tasks', -1))
    
    task = self.props
  
    initial_values = self.props['initial_values']
    increments = self.props['increments']
    query = self.props['url_query']
    btn_query = self.props.get('selected_btn_query','')
    exec_id = task['execution_id']
    
    running_tasks, num_tasks = [], 0
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 10)
    #print_flushed(results)
    num_failed = 0
    for (parent_task_id, parent_node_id, urls) in results.get(task['input'], []):
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = parent_task_id
      task['parent_node_id'] = parent_node_id
      num_local_tasks = 0
      for url in urls:
        while True:
          if max_num_tasks > -1 and num_tasks >= max_num_tasks: 
            break
          if max_num_local_tasks > -1 and num_local_tasks >= max_num_local_tasks: 
            break
          values = list(map(lambda x, y: int(x) + num_local_tasks * int(y), initial_values, increments))
          #print (url, query, values)
          task['url'] = url + (query % tuple(values))
          print_flushed(values)
          print_flushed(task['url'])
          task['btn_query'] = btn_query
          task['page_id'] = values[0]
          task['is_detail'] = False
          running_tasks.append(rm.enqueue(task))
          num_tasks += 1
          num_local_tasks += 1
          chunk_size += 1
          if chunk_size == max_chunk_size:
            graph_manager.log_expected_num_summary(exec_id, num_tasks) 
            chunk_size = 0
            num_s, num_failed = self.wait(rm, running_tasks, results, graph_manager, exec_id)
          if num_failed > 0: break
        if num_failed > 0: break
      if num_failed > 0: break
    if task['input'] in results: del results[task['input']]
    self.wait(rm, running_tasks, results, graph_manager, exec_id)

  def run(self, rm, results, previous_tasks):
    #print_flushed(self.props)
    if previous_tasks != None:
      self.rerun(rm, results, previous_tasks)
    elif len(self.props.get('url_query', '').strip()) > 0:
      self.run1(rm, results)
    else:
      self.run0(rm, results)

driver_operators = {
  'BFSIterator': BFSIterator,
  'OpenURL': OpenURL
}

def materialize(lop):
  pop = driver_operators[lop['name']]()
  pop.props = lop
  return pop
