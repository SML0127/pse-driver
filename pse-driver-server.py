import sys
import os
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flask_cors import CORS

import json
import psycopg2
import subprocess
import traceback
import time

import decimal
from datetime import date
from datetime import datetime


from pse_driver import PseDriver
from managers.log_manager import LogManager
from managers.settings_manager import SettingsManager
from managers.graph_manager import GraphManager
from managers.redis_manager import *
from rq import Queue, Worker
from functools import partial
print_flushed = partial(print, flush=True)

class JsonExtendEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, decimal.Decimal):
            return (str(o) for o in [o])
        else:
            return json.JSONEncoder.default(self, o)
            #except:
            #    return ""

def register_New_Date():
    NewDate = psycopg2.extensions.new_type((1082,), 'DATE', psycopg2.STRING)
    psycopg2.extensions.register_type(NewDate)

register_New_Date()

driver = PseDriver()
driver.init()

class DriverManager(Resource):

    def register_execution(self, program_id, db_schema_id):
        try:
            query = make_query_insert_and_returning_id("execution", ["program_id", "db_schema_id"], [program_id, db_schema_id], "id")
            cur = conn.cursor()
            cur.execute(query, [program_id, db_schema_id])
            result = cur.fetchone()[0]
            conn.commit()
            return result
        except:
            conn.rollback()
            raise

    def register_program_execution(self, program, category):
        try:
            cur = conn.cursor()
            query = make_query_insert_and_returning_id("program", ["program"], [program], "id")
            cur.execute(query, [program])
            program_id = cur.fetchone()[0]
            
            query = make_query_insert_and_returning_id("execution", ["program_id","category"], [program_id, category], "id")
            cur.execute(query, [program_id, category])
            execution_id = cur.fetchone()[0]
            conn.commit()
            return {
                "success": True,
                "execution_id": execution_id,
                "program_id": program_id,
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def run_transformation_to_mysite(self, job_id):
        print_flushed('run_transformation_to_mysite')
        try:
            print_flushed("Transformation to my site")
            #driver.run_from_db(sysinfo, False)
#'python pse_driver.py --c run_from_file --wf ./script_pse/'$script'
            print_flushed("python pse_driver.py --c transform_to_mysite --job_id %s" % str(job_id))
            subprocess.Popen("python pse_driver.py --c transform_to_mysite --job_id %s" % str(job_id), shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def update_mysite(self, job_id):
        print_flushed(job_id)
        try:
            print_flushed("Update to my site")
            #print_flushed("clear;python pse_driver.py --c update_to_mysite --job_id %s" % str(job_id))
            setting_manager = SettingsManager()
            setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
            settings = setting_manager.get_settings()
            graph_manager = GraphManager()
            graph_manager.init(settings)
            groupby_keys = graph_manager.get_groupby_key(job_id)
            cmd_update_mysite = "clear;python pse_driver.py --c update_to_mysite --job_id {} --groupbykey".format(job_id)  
            for idx in groupby_keys:
               cmd_update_mysite += " " + str(idx)
            print_flushed(cmd_update_mysite)
            #print_flushed("original: clear;python pse_driver.py --c update_to_mysite --job_id %s --groupbykey name description price" % str(job_id))
            subprocess.Popen(cmd_update_mysite, shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def update_targetsite(self, job_id):
        print_flushed(job_id)
        try:
            print_flushed("Upload / Update to target site")
            #for i in target_ids:
            #   target_id = i
            command_str = 'python cafe24_driver.py --cafe24_c run_onetime_from_ui --job_id {} --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue'.format(job_id)
            print_flushed(command_str)
            subprocess.Popen(command_str, shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def update_targetsite_test(self, job_id):
        print_flushed(job_id)
        try:
            print_flushed("Upload / Update to target site")
            #for i in target_ids:
            #   target_id = i
            command_str = 'python cafe24_driver.py --cafe24_c run_onetime_from_ui --job_id {} --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue'.format(job_id)
            print_flushed(command_str)
            subprocess.Popen(command_str, shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       





    def upload_targetsite(self, job_id, mpids):
        print_flushed(job_id)
        print_flushed(mpids)
        try:
            print_flushed("Update to my site")
            #driver.run_from_db(sysinfo, False)
#'python pse_driver.py --c run_from_file --wf ./script_pse/'$script'

            command_str = "clear;python cafe24_driver.py --cafe24_c run_mpid --cafe24_eid db --cafe24_label 3 --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue --cafe24_mall db --cafe24_code db --job_id {} --mpids ".format(job_id)
            for i in mpids:
               command_str += str(i) + " "
            print_flushed(command_str)
            subprocess.Popen("command_str", shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       





    def get_disk_usage(self, path):
        try:
            print_flushed("get disk usage")
            #driver.run_from_db(sysinfo, False)
            command_result = subprocess.Popen("df -h "+path+" | awk '{print $2, $3, $4, $5}'", stdout=subprocess.PIPE, universal_newlines=True, shell=True)
            result = {}
            check_key = 1
            key_list = []
            for stdout_line in iter(command_result.stdout.readline, ""):
                for idx, val in enumerate(str(stdout_line).split()):
                    if check_key == 1:
                      key_list.append(val) 
                    elif check_key == 2:
                      result[key_list[idx]] = val
                check_key = check_key + 1
            return {
                "success": True,
                "result" : result
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def activate_schedule(self, dag_id):
        try:
            try:
                dag_id = dag_id.split('_/_')[1]
            except:
                pass
            print_flushed("airflow dags unpause {}".format(dag_id))
            #driver.run_from_db(sysinfo, False)
            subprocess.Popen("airflow dags unpause  %s" % str(dag_id), shell=True)
            res = subprocess.Popen("airflow dags unpause  %s" % str(dag_id), stdout=subprocess.PIPE, universal_newlines=True,shell=True)
            is_success = False
            for stdout_line in iter(res.stdout.readline, ""):
                print("-stdout readline-")
                print(str(stdout_line).strip())
                is_success = ('Dag: {}, paused: False'.format(dag_id) == str(stdout_line).strip())
            if is_success == False:
                raise
            return {
                "success": is_success,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def deactivate_schedule(self, dag_id):
        try:
            try:
                dag_id = dag_id.split('_/_')[1]
            except:
                pass
            print_flushed("airflow dags pause {}".format(dag_id))
            #driver.run_from_db(sysinfo, False)
            res = subprocess.Popen("airflow dags pause  %s" % str(dag_id), stdout=subprocess.PIPE, universal_newlines=True,shell=True)
            is_success = False
            for stdout_line in iter(res.stdout.readline, ""):
                print("-stdout readline-")
                print(str(stdout_line).strip())
                is_success = ('Dag: {}, paused: True'.format(dag_id) == str(stdout_line).strip())
                #for idx, val in enumerate(str(stdout_line).split()):
                    #print_flushed(idx,val)
            if is_success == False:
                raise
            return {
                "success": is_success,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def delete_dag(self, dag_id):
        try:
            try:
                dag_id = dag_id.split('_/_')[1]
            except:
                pass
            print_flushed("airflow dags delete -y {}".format(dag_id))
            #driver.run_from_db(sysinfo, False)
            subprocess.Popen("airflow dags delete -y %s" % str(dag_id), shell=True)
            os.remove('/home/pse/airflow/dags/'+dag_id+'_scheduling_program.py')
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def add_worker(self, worker_ip, worker_port):
        try:
            setting_manager = SettingsManager()
            setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
            settings = setting_manager.get_settings()
            graph_manager = GraphManager()
            graph_manager.init(settings)
            worker_id = graph_manager.add_worker(worker_ip, worker_port)
            
            worker_name = 'Worker-' + str(worker_id)
            
            print_flushed("ssh -p {} pse@{} 'cd /home/pse/PSE-engine; /home/pse/.pyenv/shims/rq worker  --url redis://141.223.197.35:63790 -n {} -w engine.pse_worker.pseWorker --job-class engine.pse_job.pseJob real_queue'".format(worker_port, worker_ip, worker_name))
            subprocess.Popen("ssh -p {} pse@{} 'cd /home/pse/PSE-engine; /home/pse/.pyenv/shims/rq worker  --url redis://141.223.197.35:63790 -n {} -w engine.pse_worker.pseWorker --job-class engine.pse_job.pseJob real_queue'".format(worker_port, worker_ip, worker_name) , shell=True)
            
            return { "success": True }       
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def delete_worker(self, worker_name):
        try:
            setting_manager = SettingsManager()
            setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
            settings = setting_manager.get_settings()
            graph_manager = GraphManager()
            graph_manager.init(settings)
    
            res = graph_manager.get_workers_ip_and_port(worker_name)
            ip = res[0]
            port = res[1]

            redis_manager = RedisManager()
            redis_manager.connect(settings)
            rq = redis_manager.create_rq(redis_manager.get_connection(), 'real_queue')
            workers = Worker.all(queue=rq)
            for worker in workers:
               if worker.name == worker_name:
                  print_flushed("ssh -p {} pse@{} 'kill -8 {}'".format(port, ip, worker.pid))
                  subprocess.Popen("ssh -p {} pse@{} 'kill -8 {}'".format(port, ip, worker.pid) , shell=True)
 
            graph_manager.delete_worker(worker_name)
            #graph_manager.disconnect()
            return { "success": True }       

        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def get_workers(self, sleep):
        try:
            setting_manager = SettingsManager()
            setting_manager.setting("/home/pse/pse-driver/settings-driver.yaml")
            settings = setting_manager.get_settings()

            graph_manager = GraphManager()
            graph_manager.init(settings)

            redis_manager = RedisManager()
            redis_manager.connect(settings)
            rq = redis_manager.create_rq(redis_manager.get_connection(), 'real_queue')
            if type(sleep) != type(True): sleep = eval(sleep)
            if sleep == True:
               time.sleep(3)
            res = graph_manager.get_workers()
            workers = Worker.all(queue=rq)
            workers_list = []

            for data in res:
               wid = data[0] 
               wip = data[1] 
               wport = data[2]
               worker_id = 'Worker-'+str(wid) 
               for worker in workers:
                  if worker.name == worker_id:
                     workers_list.append([worker.name, str(wip)+':'+str(wport), worker.state])

            print_flushed(workers_list) 
            return {
                "success": True,
                "workers": workers_list
            }

        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def run_driver(self, wf, job_id):
        try:
            #print_flushed("python pse_driver.py --c run_from_db --wf {} --job_id {}".format(wf, job_id))
            subprocess.Popen("python pse_driver.py --c run_from_db --wf {} --job_id {}".format(wf, job_id), shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       





    def run_driver_old(self, execution_id):
        try:
            print_flushed("simple run")
            #driver.run_from_db(sysinfo, False)
            subprocess.Popen("python pse_driver.py run_from_execution %s" % str(execution_id), shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('wf')
        parser.add_argument('program')
        parser.add_argument('category')
        parser.add_argument('program_id')
        parser.add_argument('schema')
        parser.add_argument('schema_id')
        parser.add_argument('req_type')
        parser.add_argument('job_id')
        parser.add_argument('dag_id')
        parser.add_argument('worker_ip')
        parser.add_argument('worker_port')
        parser.add_argument('worker_name')
        parser.add_argument('sleep')
        parser.add_argument('path', required=False)
        parser.add_argument('mpids', required=False)
        parser.add_argument('target_ids', required=False)

        args = parser.parse_args()
        print_flushed(args)
        if args['req_type'] == "register_program_execution":
            return self.register_program_execution(args['program'], args['category'])
        elif args['req_type'] == "run_driver":
            return self.run_driver(args['wf'], args['job_id'])
        elif args['req_type'] == "run_transformation_to_mysite":
            return self.run_transformation_to_mysite(args['job_id'])
        elif args['req_type'] == "update_mysite":
            return self.update_mysite(args['job_id'])
        elif args['req_type'] == "update_targetsite":
            return self.update_targetsite(args['job_id'])
        elif args['req_type'] == "update_targetsite_test":
            return self.update_targetsite_test(args['job_id'])
        elif args['req_type'] == "upload_targetsite":
            return self.upload_targetsite(args['job_id'], args['mpids'])
        elif args['req_type'] == "get_disk_usage":
            return self.get_disk_usage(args['path'])
        elif args['req_type'] == "activate_schedule":
            return self.activate_schedule(args['dag_id'])
        elif args['req_type'] == "deactivate_schedule":
            return self.deactivate_schedule(args['dag_id'])
        elif args['req_type'] == "delete_dag":
            return self.delete_dag(args['dag_id'])
        elif args['req_type'] == "add_worker":
            return self.add_worker(args['worker_ip'], args['worker_port'])
        elif args['req_type'] == "delete_worker":
            return self.delete_worker(args['worker_name'])
        elif args['req_type'] == "get_workers":
            return self.get_workers(args['sleep'])
        else:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

app = Flask(__name__)
CORS(app)
api = Api(app)

api.add_resource(DriverManager, '/api/driver/')

if __name__ == '__main__':

    app.run(debug=True, host='0.0.0.0', port=5001)
