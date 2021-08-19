import sys
import os
import re
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
import json
import psycopg2
import numpy as np
from flask_cors import CORS
#from demo_test_execution import *
#from demo_rerun_module import *
import subprocess
import traceback
#import multiprocessing
import json
import decimal
import zlib
from datetime import date
from datetime import datetime
from datetime import timedelta
import requests
import string
from price_parser import Price

#from pse_driver import *

conn = None
try:
    conn = psycopg2.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")
except:
    print("fail connect to the database(conn)")

db_conn = None
try:
    db_conn = psycopg2.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")
    #db_conn = psycopg2.connect("dbname='pse' user='smlee' host='141.223.197.36' port='5432' password='smlee'")
except:
    print("fail connect to the database(db-conn)")


def is_hex_str(s):
    if s is None:
        return False
    else:
        return set(s).issubset(string.hexdigits)


class CategoryManager(Resource):
    def get_category(self):
        try:
            cur = conn.cursor()
            cur.execute("select category from category;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def save_category(self, category):
        try:
            cur = conn.cursor()
            sql = "update category set category = '"
            sql += category
            sql += "' where id = 1"
            cur.execute(sql)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('category')
        args = parser.parse_args()
        if args['req_type'] == 'save_category':
            return self.save_category(args['category']);
        if args['req_type'] == 'get_category':
            return self.get_category();
        return { "success": False }

class TransformManager(Resource):
    def get_transforms(self):
        try:
            cur = conn.cursor()
            cur.execute("select id, transform from transform order by id asc;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_columns_and_tree(self, transform_id):
        try:
            cur = conn.cursor()
            sql = "select columns_and_tree from transform "
            sql += "where id = "
            sql += transform_id + ";"
            cur.execute(sql)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       
  
    def delete_transform(self, transform_id):
        try:
            cur = conn.cursor()
            sql = 'delete from transform where id = '
            sql += transform_id + ';'
            cur.execute(sql)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }


    def add_transform(self):
        try:
            cur = conn.cursor()
            values=[""]
            sql = make_query_insert_and_returning_id("transform", ["transform"], values, "id")
            cur.execute(sql, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "output":result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def update_transform(self, transform, columns_and_tree, transform_id):
        try:
            cur = conn.cursor()
            columns_and_tree = columns_and_tree.replace("'", '"')
            sql = "update transform set transform = '"
            sql += transform
            sql += "', columns_and_tree = '"
            sql += columns_and_tree
            sql += "' where id = "
            sql += transform_id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('transform')
        parser.add_argument('transform_id')
        parser.add_argument('columns_and_tree')
        args = parser.parse_args()
        if args['req_type'] == 'add_transform':
            return self.add_transform();
        if args['req_type'] == 'delete_transform':
            return self.delete_transform(args['transform_id']);
        if args['req_type'] == 'update_transform':
            return self.update_transform(args['transform'],args['columns_and_tree'],args['transform_id']);
        if args['req_type'] == 'get_transforms':
            return self.get_transforms();
        if args['req_type'] == 'get_columns_and_tree':
            return self.get_columns_and_tree(args['transform_id']);
        return { "success": False }



class ObjectManager(Resource):
    def get_object_tree(self):
        try:
            cur = conn.cursor()
            cur.execute("select object_tree from object_tree;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def save_object_tree(self, object_tree):
        try:
            cur = conn.cursor()
            sql = "update object_tree set object_tree = '"
            sql += object_tree
            sql += "' where id = 1"
            cur.execute(sql)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('object_tree')
        args = parser.parse_args()
        if args['req_type'] == 'save_object_tree':
            return self.save_object_tree(args['object_tree']);
        if args['req_type'] == 'get_object_tree':
            return self.get_object_tree();
        return { "success": False }

class UserProgramManager(Resource):
    def get_user_program(self, job_id):
        try:
            cur = conn.cursor()
            #cur.execute("select id, site, description, program from user_program where job_id = "+str(job_id)+" or job_id = -1 order by id desc;")
            cur.execute("select id, site, description, program, job_id from user_program where job_id = {} or job_id < 0 order by id desc;".format(job_id))
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 != 0 and idx2 != 3 and idx2 != 4:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t

            conn.commit()
            return { "success": True, "output" : result }
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def get_last_user_program(self, job_id):
        try:
            query = "select exists (select 1 from user_program where job_id = {})".format(job_id)
            cur = conn.cursor()
            cur.execute(query)
         
            result = cur.fetchone()[0]
            is_template = not(result)
            if result == True:
               query = "select id, program, site, job_id from user_program where job_id = {} order by update_time desc, id desc limit 1;".format(job_id)
               cur.execute(query)
               result = cur.fetchone()
               lst = list(result)
               lst[2] = bytes.fromhex(lst[2]).decode()
               result = lst
            else:
               query = "select site from jobs where id = {}".format(job_id)
               cur.execute(query)
               result = cur.fetchone()[0]
               #<option value="1000">Blank</option>); -> -1001
               #<option value="0">Amazon US</option>); -> -1
               #<option value="1">Jomashop</option>); -> -2
               #<option value="2">Zalando</option>); -> -3
               #<option value="3">Rakuten</option>); -> -4
               #<option value="4">Ebay</option>); -> -5

               job_id = (int(result) * -1) - 1
               #if int(result) == 0:
               #   job_id = -1
               #elif int(result) == 1:
               #   job_id = -2
               #elif int(result) == 2:
               #   job_id = -3
               #elif int(result) == 3:
               #   job_id = -4

               query = "select id, program, site, job_id from user_program where job_id = {} order by id desc limit 1;".format(job_id)
               cur.execute(query)
               result = cur.fetchone()
               lst = list(result)
               lst[2] = bytes.fromhex(lst[2]).decode()
               result = lst
            #result[1] = bytes.fromhex(result[1]).decode()
             
            conn.commit()
            return { "success": True, "result": result, "is_template": is_template}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }


    def save_user_program(self, site, description, user_program, job_id):
        try:
            origin_site = site
            site = site.encode('UTF-8','strict').hex()
            description = description.encode('UTF-8','strict').hex()
            #user_program = user_program.encode('UTF-8','strict').hex()
           
            values = [site, description, user_program,job_id]

            query = make_query_insert_and_returning_id("user_program", ["site","description","program","job_id"], values, "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
         
            conn.commit()

            return { "success": True, "id": result, "title": origin_site }
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def update_user_program(self, user_program, wid):
        try:
            
            query = "update user_program set program = %s, update_time = now() where id = {}".format(wid)          
            cur = conn.cursor()
            cur.execute(query, [user_program])
            conn.commit()

            return { "success": True }
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('site')
        parser.add_argument('category')
        parser.add_argument('program')
        parser.add_argument('job_id')
        parser.add_argument('wid')
        args = parser.parse_args()
        print(args)
        if args['req_type'] == 'save_user_program':
            return self.save_user_program(args['site'],args['category'],args['program'],args['job_id']);
        elif args['req_type'] == 'update_user_program':
            return self.update_user_program(args['program'],args['wid']);
        elif args['req_type'] == 'get_user_program':
            return self.get_user_program(args['job_id']);
        elif args['req_type'] == 'get_last_user_program':
            return self.get_last_user_program(args['job_id']);
        return { "success": False }




class JsonExtendEncoder(json.JSONEncoder):
    """
        This class provide an extension to json serialization for datetime/date.
    """
    def default(self, o):
        """
            provide a interface for datetime/date
        """
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

def make_query_insert(table_name, colnames, values):
    sql = "INSERT INTO {} (".format(table_name)
    sql += ", ".join([colname for colname in colnames])
    sql += ") VALUES ( "
    sql += ",".join(["%s" for _ in range(len(values))])
    sql += ")"
    return sql

def make_query_insert_and_returning_id(table_name, colnames, values, id_name):
    sql = make_query_insert(table_name, colnames, values)
    sql += " RETURNING {}".format(id_name)
    return sql

class TaskManager(Resource):


    def get_input_of_tasks(self, task_ids):
        try:
            cur = conn.cursor()
            query = "select input from task where "
            task_id_list = task_ids.split(',')
            for _ in range(len(task_id_list)):
                query += " task.id = %s or"
            cur.execute(query % str(task_id_list))
            input_urls = cur.fetchall()[0]
            conn.commit()
            return {
                "success" : True,
                "input_urls" : input_urls,
                 }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) } 

    def get_failed_task(self, exec_id):
        try:
            cur = conn.cursor()
            #query = "select task_id, err_msg from failed_task_detail where task_id in (select t.id from node n, stage s, task t, (select max(n1.label) as max_label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = {}) as l where n.task_id = t.id and t.stage_id = s.id and s.execution_id = {} and n.label = l.max_label and t.status < 0) order by task_id asc;".format(exec_id, exec_id)
            #query = "select task_id, err_msg from failed_task_detail where task_id in (select t.id from node n, stage s, task t, (select n1.label as label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = {}) as l where n.task_id = t.id and t.stage_id = s.id and s.execution_id = {} and n.label = l.label and t.status < 0) order by task_id asc;".format(exec_id, exec_id)
            query = "select task_id, err_msg from failed_task_detail where task_id in (select id from task where status != -999999 and stage_id in (select id from stage where execution_id = {})) order by task_id asc;".format(exec_id)
            cur.execute(query)
            results = cur.fetchall()
            conn.commit()
            return {
                "success" : True,
                "result" : results,
                }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def get_failed_task_detail(self, node_id):
        try:
            cur = conn.cursor()
            query = "select err_msg from failed_task_detail where task_id in (select task_id from node where id = {});".format(node_id)
            cur.execute(query)
            results = cur.fetchone()[0]
            return {"success" : True, "result" : results}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       
    
    
    def get_succeed_task(self, task_id, tables):
        try:

            cur = conn.cursor()
           
            query = "select task.input, stage.level from task join stage on stage.id = task.stage_id where task.id = %s;"
            cur.execute(query % str(task_id))
            (input_url,level) = cur.fetchone()
          
            query = "select output from succeed_task_detail where task_id = %s;"
            cur.execute(query % str(task_id))
            output_url_list = cur.fetchall()[0]

            conn.commit()
            cur = db_conn.cursor()
            output_db=[]

            tables = json.loads(tables)
            for table in tables:
                query = "select * from %s where pse_task_id = %s"
                cur.execute(query % (str(table), str(task_id)))
                cols = [str(desc[0]) for desc in cur.description]
                rows = []
                for data in cur.fetchall():
                    row = [str(col) for col in data]
                    rows.append(row)
                db = {"tableName" : str(table), "cols": cols, "rows": rows}
                output_db.append(db)

            db_conn.commit()
            ouput_db = json.dumps(output_db)
            return {
                "success": True,
                "input_url" : input_url,
                "level" : level,
                "output_url_list" : output_url_list,
                "output_db" : output_db
                 }
        except:
            conn.rollback()
            db_conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get_stage(self, stage_id):
        try:
            cur = conn.cursor()

            query = "select "
            query += "count(*) FILTER (WHERE task.status = 1) as succeed, "
            query += "count(*) FILTER (WHERE task.status < 0) as failed, "
            query += "count(*) FILTER (WHERE task.status = 0) as unknown, "
            query += "count(*) FILTER (WHERE task.status = -1) as unknown_err, "
            query += "count(*) FILTER (WHERE task.status = -2) as redis_err, "
            query += "count(*) FILTER (WHERE task.status = -3) as grinplum_err, "
            query += "count(*) FILTER (WHERE task.status = -4) as psql_err, "
            query += "count(*) FILTER (WHERE task.status = -5) as selenium_err "
            query += "from task "
            query += "where task.stage_id = %s ;"
            cur.execute(query % str(stage_id))
            dataForTasks = cur.fetchall()[0]

            query = "select id, input from task where status = 1 and stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForSucceedTasks = cur.fetchall()

            query = "select id, input, status from task where status < 0 and stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForFailedTasks = cur.fetchall()
            conn.commit()
            return {
                "success": True,
                "dataForTasks" : dataForTasks,
                "dataForSucceedTasks" : dataForSucceedTasks,
                "dataForFailedTasks" :dataForFailedTasks}
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }



    def get_all_tasks_of_stage(self, stage_id):
        try:
            cur = conn.cursor()
            query = "select id, input from task where stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForAllTasks = cur.fetchall()
            conn.commit()
            return {
                "success": True,
                "allTasks" : dataForAllTasks
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get_stages(self, execution_id, num_of_stages):
        try:
            cur = conn.cursor()

            query = "select stage.id, stage.level, TO_CHAR(stage.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(stage.end_time, 'YYYY:HH24:MI:SS'), "
            query += "count(*) FILTER (WHERE task.status = 1) as succeed, "
            query += "count(*) FILTER (WHERE task.status < 0) as failed, "
            query += "count(*) FILTER (WHERE task.status = 0) as unknown "
            query += "from stage left join task on stage.id = task.stage_id "
            query += "where stage.execution_id = %s  "
            query += "group by stage.id, stage.level, stage.start_time, stage.end_time "
            query += "order by stage.level;"
            query = query % str(execution_id)
            cur.execute(query)
            stages = cur.fetchall()
            conn.commit()
            runningStageId = 1
            for stage in stages:
                if stage[3] == None: 
                    break
                runningStageId = runningStageId + 1

            for i in range (len(stages), int(num_of_stages)):
                stages.append([i+1, "", "", 0, 0, 0])
            return {
                "success": True,
                "RunningStageId" : runningStageId,
                "stageStatus" : stages
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('job_id')
        parser.add_argument('req_type')
        parser.add_argument('stage_id')
        parser.add_argument('level')
        parser.add_argument('num_of_stages')
        parser.add_argument('task_id')
        parser.add_argument('task_ids')
        parser.add_argument('tables')
        parser.add_argument('exec_id')
        parser.add_argument('node_id')
        args = parser.parse_args()
        if args['req_type'] == 'stages':
            return self.get_stages(args['execution_id'], args['num_of_stages'])
        if args['req_type'] == 'stage':
            return self.get_stage(args['stage_id'])
        if args['req_type'] == 'tasks':
            return self.get_all_tasks_of_stage(args['stage_id'])
        if args['req_type'] == 'succeed_task':
            return self.get_succeed_task(args['task_id'], args['tables'])
        if args['req_type'] == 'failed_task':
            return self.get_failed_task(args['exec_id'])
        if args['req_type'] == 'get_failed_task_detail':
            return self.get_failed_task_detail(args['node_id'])
        if args['req_type'] == 'input_of_tasks':
            return self.get_input_of_tasks(args['task_ids'])
        return { "success": False }


class FailedJobsManager(Resource):

    def get_num_failed_jobs_per_level(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select bfs_level, count(*) as num "
            query += "from log_failed_job "
            query += "where execution_id = %s "
            query += "group by bfs_level; "
            cur.execute(query % execution_id)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "num_failed_jobs_per_level" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_failed_jobs(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select idx, bfs_level, url "
            query += "from log_failed_job where execution_id = %s;"
            cur.execute(query, (execution_id))
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "failed_jobs" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_failed_jobs(self, execution_id, level):
        try:
            cur = conn.cursor()
            query = "select idx, url "
            query += "from log_failed_job "
            query += "where execution_id = %s and bfs_level = %s;"
            cur.execute(query, (execution_id, level))
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "failed_jobs" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_failed_job(self, job_id):
        try:
            cur = conn.cursor()
            query = "select bfs_level, url, err_msg "
            query += "from log_failed_job "
            query += "where idx = %s;"
            cur.execute(query % (job_id))
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "failed_job" : result }
        except:
            conn.rollback()
            return { "success": False }

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('job_id')
        parser.add_argument('req_type')
        parser.add_argument('level')
        args = parser.parse_args()
        if args['req_type'] == 'failed_jobs_per_level':
            return self.get_num_failed_jobs_per_level(args['execution_id'])
        if args['req_type'] == 'failed_jobs_of_level':
            return self.get_failed_jobs(args['execution_id'], args['level'])
        if args['req_type'] == 'failed_job':
            return self.get_failed_job(args['job_id'])
        return {}

class TesterOnServer(Resource):
    def test_job(self, program, schema, level, url):
        url = json.loads(url)[0]
        results = [] #run_test(schema, program, url, int(level))
        return{"stdout":"\n".join(results[0]), "err_msg": results[1]}

    def simple_test_job(self, program, schema, level, url):
        #test = Tester()
        url = json.loads(url)[0]
        program = json.loads(program)
        (msg_list, err_msg) = ([],"")#test.test(url, program, int(level)-1)
        #test.close()

        return {"stdout": "\n".join(msg_list), "err_msg": err_msg}

#    def test_job_realistic(self, program, schema, level, url):
#        pool = multiprocessing.Pool(processes = 1)
#        results = pool.map(test_job, [(program, schema, level, url)])
#        print(result)
#        return {}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('program')
        parser.add_argument('schema')
        parser.add_argument('level')
        parser.add_argument('url')
        args = parser.parse_args()
        if args['req_type'] == 'simple_test_job':
            return self.simple_test_job(args['program'], args['schema'], args['level'], args['url'])
        if args['req_type'] == 'test_job':
            return self.test_job(args['program'], args['schema'], args['level'], args['url'])
        return {}

class DBSchemasManager(Resource):
    def get_db_schemas(self):
        try:
            cur = conn.cursor()
            cur.execute("select id from db_schema;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "db_schemas" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_db_schema(self, db_schema_id):
        try:
            query = "select schema from db_schema where id = %s"
            values = (db_schema_id)
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "db_schema": result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_schema(self, db_schema):
        try:
            query = make_query_insert_and_returning_id("db_schema", ["schema"], [json.dumps(db_schema)], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "db_schema_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get(self):
        return self.get_schemas()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('db_schema_id')
        parser.add_argument('user_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_db_schema':
            return self.get_db_schema(args['db_schema_id']);
        return { "success": False }

class ProgramsManager(Resource):
    def get_programs(self):
        try:
            cur = conn.cursor()
            cur.execute("select id from program;")
            result = cur.fetchall()
            return { "success": True, "programs" : result }
        except:
            return { "success": False }


    def get_last_program(self):
        try:
            query = "select id, program from program order by id desc limit 1;"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "program_id": result[0], "program": result[1] }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }
   

    def get_program(self, program_id):
        try:
            query = "select program from program where id = %s"
            values = (program_id)
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "program" : result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def save_program(self, program):
        try:
            values = [program]
            query = make_query_insert("program", ["program"], values)
            cur = conn.cursor()
            cur.execute(query, values)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_program(self, program):
        try:
            query = make_query_insert_and_returning_id("program", ["program"], [json.dumps(program)], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "program_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get(self):
        return self.get_programs()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('program_id')
        parser.add_argument('user_id')
        parser.add_argument('program')
        args = parser.parse_args()
        if args['req_type'] == 'get_program':
            return self.get_program(args['program_id']);
        if args['req_type'] == 'get_last_program':
            return self.get_last_program();
        if args['req_type'] == 'save_program':
            return self.save_program(args['program']);
        return { "success": False }

class ExecutionsManager(Resource):
    #def get_executions(self):
    #    try:
    #        cur = conn.cursor()
    #        subquery =  "select exc.id as id, exc.program_id as program_id, exc.db_schema_id as db_schema_id, exc.start_time as start_time, exc.end_time as end_time, COALESCE(stage.level, 0) as current_stage "
    #        subquery += "from execution as exc left join stage on exc.id = stage.execution_id "
    #        query =  "select t.id, t.program_id, t.db_schema_id, TO_CHAR(t.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(t.end_time, 'YYYY:HH24:MI:SS'), MAX(current_stage) "
    #        query += "from (" + subquery + ") as t "
    #        query += "group by t.id, t.program_id, t.db_schema_id, t.start_time, t.end_time "
    #        query += "order by t.id desc; "
    #        cur.execute(query)
    #        result = cur.fetchall()
    #        conn.commit()
    #        return { "success": True, "executions" : result }
    #    except:
    #        conn.rollback()
    #        print(traceback.format_exc())
    #        return { "success": False, "traceback": str(traceback.format_exc()) }       
    def get_executions(self, job_id):
        try:
            cur = conn.cursor()
            
            query =  "select id, program_id, null, TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS'), null, num_success, num_fail, num_invalid, num_all from execution where job_id = {} order by id desc".format(job_id)
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
                if val[4] is None:
                  if timedelta(days=2) > (datetime.now() - datetime.strptime(val[3],'%Y-%m-%d %H:%M:%S')):
                    exec_id = str(val[0])

                    query =  'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = 1'.format(exec_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    result[idx] = result[idx] + (res[0],)

                    query =  'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and (status = -1 or status = -998 or status = -997)'.format(exec_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    result[idx] = result[idx] + (res[0],) 

                    query =  'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = -999'.format(exec_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    result[idx] = result[idx] + (res[0],) 

                    query =  'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {})'.format(exec_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    result[idx] = result[idx] + (res[0],) 
                  else: # after 2days
                    exec_id = str(val[0])

                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = 1'.format(exec_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[6] = res[0]
                    t = tuple(lst)
                    result[idx] = t
                   
                    query = "update execution set num_success = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()


                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and (status = -1 or status = -997 or status = -998)'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {} and status = 1'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[7] = res[0]
                    t = tuple(lst)
                    result[idx] = t
         
                    query = "update execution set num_fail = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()

                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = -999'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {} and status = 1'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[8] = res[0]
                    t = tuple(lst)
                    result[idx] = t
                   
                    query = "update execution set num_invalid = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()


                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {})'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {}'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[9] = res[0]
                    t = tuple(lst)
                    result[idx] = t

                    query = "update execution set num_all = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()
                else:
                  if val[6] is None:
                    exec_id = str(val[0])
                    #query = 'select max(id) from stage where execution_id = {}'.format(exec_id)
                    #cur.execute(query)
                    #stage_id= cur.fetchone()[0]


                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = 1'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {} and status = 1'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[6] = res[0]
                    t = tuple(lst)
                    result[idx] = t
                   
                    query = "update execution set num_success = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()


                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and (status = -1 or status = -997 or status = -998)'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {} and status = 1'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[7] = res[0]
                    t = tuple(lst)
                    result[idx] = t
         
                    query = "update execution set num_fail = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()

                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {}) and status = -999'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {} and status = 1'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[8] = res[0]
                    t = tuple(lst)
                    result[idx] = t
                   
                    query = "update execution set num_invalid = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()


                    query = 'select count(*) from task where stage_id in (select max(id) from stage where execution_id = {})'.format(exec_id)
                    #query = 'select count(id) from task where stage_id = {}'.format(stage_id)
                    cur.execute(query)
                    res= cur.fetchone()
                    lst = list(result[idx])
                    lst[9] = res[0]
                    t = tuple(lst)
                    result[idx] = t
                    query = "update execution set num_all = {} where id = {}".format(res[0], val[0])
                    cur.execute(query)
                    conn.commit()
            conn.commit()
            return { "success": True, "executions" : result }
        except:
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def get_executions_category(self,category):
        try:
            cur = conn.cursor()
            subquery =  "select exc.id as id, exc.program_id as program_id, exc.category as category, exc.start_time as start_time, exc.end_time as end_time, COALESCE(stage.level, 0) as current_stage "
            subquery += "from execution as exc left join stage on exc.id = stage.execution_id "
            query =  "select t.id, t.program_id,t.category, TO_CHAR(t.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(t.end_time, 'YYYY:HH24:MI:SS'), MAX(current_stage) "
            query += "from (" + subquery + ") as t "
            query += "where category = '"+category+"' group by t.id, t.program_id, t.category,  t.start_time, t.end_time "
            query += "order by t.id desc; "
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "executions" : result }
        except:
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def get_last_execution(self):
        try:
            cur = conn.cursor()
            query = "select ex.id, pr.program, sc.schema "
            query += "from execution ex, program pr, db_schema sc "
            query += "where ex.program_id = pr.id and ex.db_schema_id = sc.id "
            query += "order by ex.id desc limit 1;"
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "execution" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_execution(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select pr.program "
            query += "from execution ex, program pr "
            query += "where ex.id = %s and ex.program_id = pr.id;"
            cur.execute(query % execution_id)
            result = cur.fetchone()
            #result = json.dumps(parsed, indent=4, sort_keys=True)
            conn.commit()
            return { "success": True, "execution" : result }
        except:
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_execution(self, program_id, db_schema_id):
        try:
            query = make_query_insert_and_returning_id("execution", ["program_id", "db_schema_id"], [program_id, db_schema_id], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "execution_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get_succeed_execution(self, execution_id, tables):
        try:
            cur = db_conn.cursor()
            tables = json.loads(tables)
            output_db=[]
            for table in tables:
                query = "select * from %s where pse_execution_id = %s "
                cur.execute(query % (str(table), str(execution_id)))
                cols = [str(desc[0]) for desc in cur.description]
                rows = []
                for data in cur.fetchall():
                    row = [str(col) for col in data]
                    rows.append(row)
                db = {"tableName" : str(table), "cols": cols, "rows": rows}
                output_db.append(db)

            db_conn.commit()
            ouput_db = json.dumps(output_db)
            return {
                "success": True,
                "output_db" : output_db
                 }
        except:
            conn.rollback()
            db_conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def check_error(self, user_id):
        try:
            query = "select id from jobs where user_id = '{}' and label != ''".format(user_id)
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()
            job_ids = []
            for i in result:
              job_ids.append(i[0])

            job_id_str = '('
            for job_id in job_ids:
              job_id_str += str(job_id) + ', '
            job_id_str = job_id_str[0:-2] + ')'

            query = "select label from jobs where id in (select job_id from check_is_error where is_error = 1 and job_id in " + job_id_str +")"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()


            query = "update check_is_error set is_error = 0 where is_error = 1 and job_id in " + job_id_str
            cur = conn.cursor()
            cur.execute(query)

            conn.commit()
            return { "success": True, "result": result }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }

    def get_data(self, exec_id):
        try:
            #query = "select node_id, key, value from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id = {}))) and key != 'html' order by node_id asc, id asc;".format(exec_id)
            query = "select node_id, key, value from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id = {}))) order by node_id asc, id asc;".format(exec_id)
            #query = "select node_id, key, value from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id = {}))) and key != 'html' order by node_id asc limit 3;".format(exec_id)
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()
            res_dictionary = {}
            for idx in result:
                if idx[0] not in res_dictionary:
                    res_dictionary[idx[0]] = []
                value = ''
                if isinstance(idx[2], dict):
                   value = json.dumps(idx[2])
                else:
                   value = idx[2]
                if value is None:
                   value = ''
                res_dictionary[idx[0]].append([idx[1], value])

            conn.commit()
            return { "success": True, "result": res_dictionary }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }

    def get_latest_progress(self, job_id):
        try:
            query = "select num_expected_all, num_expected_success from execution where job_id = {} order by id desc limit 1;".format(job_id)
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()
            return { "success": True, "result": result }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }




    def get(self):
        return self.get_executions()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('program_id')
        parser.add_argument('category')
        parser.add_argument('db_schema_id')
        parser.add_argument('req_type')
        parser.add_argument('tables')
        parser.add_argument('job_id')
        parser.add_argument('user_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_execution':
            return self.get_execution(args['execution_id'])
        elif args['req_type'] == 'get_executions':
            return self.get_executions(args['job_id'])
        elif args['req_type'] == 'get_executions_category':
            return self.get_executions_category(args['category'])
        elif args['req_type'] == 'get_last_execution':
            return self.get_last_execution()
        elif args['req_type'] == 'get_scrapped_data_of_execution':
            return self.get_succeed_execution( args['execution_id'], args['tables'])
        elif args['req_type'] == 'check_error':
            return self.check_error(args['user_id'])
        elif args['req_type'] == 'get_data':
            return self.get_data(args['execution_id'])
        elif args['req_type'] == 'get_latest_progress':
            return self.get_latest_progress(args['job_id'])
        return {}


class AccountManager(Resource):
    def get_auth(self, userId, password):
        try:
            cur = conn.cursor()
            query = "select exists (select 1 from account where user_id = "
            query += "'" + userId + "'"
            query += "and password = "
            query += "'" + password + "');"
            cur.execute(query)
            result = cur.fetchone()[0]
            conn.commit()
            if result == True:
                query = "select is_dev, normal_user_id from account where user_id = '{}'".format(userId)
                cur.execute(query)
                res = cur.fetchone()
                is_dev = res[0]
                user_id = res[1]
                if is_dev == True:
                   return {"success": True, "auth": True, 'is_dev': is_dev, "normal_user_id": user_id}
                else:
                   return {"success": True, "auth": True, 'is_dev': is_dev}
            else:
                return {"success": True, "auth": False}
        except:
            conn.rollback()
            return {"success": False}

    def sign_up(self, userId, password):
        try:
            cur = conn.cursor()
            query = "insert into account(user_id, password, is_dev) values('{}','{}',false)".format(userId, password)
            cur.execute(query)
            conn.commit()
            query = "insert into account(user_id, password, is_dev, normal_user_id) values('{}','{}',true, '{}')".format(userId + '_dev', password+'_dev', userId)
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return {"success": False}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('password')
        parser.add_argument('new_user_id')
        parser.add_argument('new_password')
        args = parser.parse_args()
        if args['req_type'] == 'get_auth':
            return self.get_auth(args['user_id'], args['password'])
        elif args['req_type'] == 'sign_up':
            return self.sign_up(args['new_user_id'], args['new_password'])
        return { "success": False }

class ProjectManager(Resource):
    def get_project_list(self, user_id):
        try:
            cur = conn.cursor()
            query = "select project_id, project_name from project where user_id = "
            query += "'" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return result
        except:
            conn.rollback()
            return -1

    def make_new_project(self, user_id):
        try:
            cur = conn.cursor()
            query = "insert into project (user_id, project_name) values "
            query += "('" + user_id + "', 'New Project');"
            cur.execute(query)
            conn.commit()
            return True
        except:
            conn.rollback()
            return False

    def remove_project(self, project_id):
        try:
            cur = conn.cursor()
            query = "delete from project where project_id = '" + project_id + "';"
            cur.execute(query)
            conn.commit()
            return True
        except:
            conn.rollback()
            return False

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('password')
        parser.add_argument('project_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_project_list':
            return self.get_project_list(args['user_id'])
        elif args['req_type'] == 'make_new_project':
            return self.make_new_project(args['user_id'])
        elif args['req_type'] == 'remove_project':
            return self.remove_project(args['project_id'])

class GroupManager(Resource): # added by mwseo
    def get_group_list(self, user_id):
        try:
            cur = conn.cursor()
            query = "select json_agg(json_build_object('id', id, 'label', label)) from groups "
            query += "where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}
    def make_new_group(self, group_name, user_id):
        try:
            cur = conn.cursor()
            query = "insert into group_job (type) values ('group') returning id;"
            cur.execute(query)
            result = cur.fetchall()
            group_id = str(result[0][0])
            cur = conn.cursor()
            query = "insert into groups values (" + group_id
            query += ", '" + group_name + "', '" + user_id + "');"
            cur.execute(query)
            conn.commit()
            return {"success": True, "group_id": group_id}
        except:
            conn.rollback()
            return {"success": False}
    def remove_group(self, group_id):
        try:
            cur = conn.cursor()
            query = "delete from groups where id = '" + group_id + "';"
            cur.execute(query)
            conn.commit()
            # Now, remove_group function only removes the group itself.
            # automatically by using foreign key in db system? or I need to add?
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('group_id')
        parser.add_argument('group_name')
        args = parser.parse_args()
        if args['req_type'] == 'get_group_list':
            return self.get_group_list(args['user_id'])
        elif args['req_type'] == 'make_new_group':
            return self.make_new_group(args['group_name'], args['user_id'])
        elif args['req_type'] == 'remove_group':
            return self.remove_group(args['group_id'])

class JobManager(Resource): # added by mwseo
    def get_job_list(self, user_id):
        try:
            cur = conn.cursor()
            query = "select json_agg(json_build_object('id', id, 'parentId', parent_id, 'label', label) order by lower(label) asc) from jobs "
            query += "where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False, "result": str(traceback.format_exc())}

    def get_job_info(self, job_id):
        try:
            cur = conn.cursor()
            query = "select country from jobs where id = " + job_id + ";"
            cur.execute(query)
            country = cur.fetchone()[0]

            query = "select url from jobs where id = '" + job_id + "';"
            cur.execute(query)
            url = cur.fetchone()[0]

            query = "select count(mpid) from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4".format(job_id)
            cur.execute(query)
            cnt_mpid = cur.fetchone()[0]

            query = "select COALESCE(to_char(start_time, 'YYYY-MM-DD'), '') AS last_update from sm_history where job_id = '" + job_id + "' order by id desc limit 1;"
            cur.execute(query)
            result = cur.fetchone()
    
            if result == None:
               query = "select COALESCE(to_char(last_update, 'YYYY-MM-DD'), '') AS last_update from jobs where id = '" + job_id + "';"
               cur.execute(query)
               result = cur.fetchone()
            last_update = result[0]
            return {"success": True, "result": (country, url, cnt_mpid, last_update)}
        except:
            conn.rollback()
            return {"success": False}

     

    def get_count_lastupdate(self, job_id):
        try:
            cur = conn.cursor()
            query = "select count from jobs where id = '" + job_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}


    def get_url(self, job_id):
        try:
            cur = conn.cursor()
            query = "select url from jobs where id = '" + job_id + "';"
            cur.execute(query)
            result = cur.fetchone()[0]
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}


    def set_url(self, job_id, url):
        try:
            cur = conn.cursor()
            query = "update jobs set url = '{}' where id = {};".format(url, job_id)
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}



    def get_num_of_product_in_job(self, job_id):
        try:
            cur = conn.cursor()
            query = "select count(mpid) from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4".format(job_id)
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_lastupdate(self, job_id):
        try:
            cur = conn.cursor()
            query = "select COALESCE(to_char(start_time, 'YYYY-MM-DD'), '') AS last_update from execution where job_id = '" + job_id + "';"
            #query = "select last_update from jobs where id = '" + job_id + "';"
            cur.execute(query)
            result = cur.fetchone()
    
            if result == None:
               query = "select COALESCE(to_char(last_update, 'YYYY-MM-DD'), '') AS last_update from jobs where id = '" + job_id + "';"
               cur.execute(query)
               result = cur.fetchone()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}

    def get_country(self, job_id):
        try:
            cur = conn.cursor()
            query = "select country from jobs where id = " + job_id + ";"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}

    def make_new_job(self, user_id, parent_id, url,job_label, job_country, site):
        try:
            cur = conn.cursor()
            query = "insert into group_job (type) values ('job') returning id;"
            cur.execute(query)
            result = cur.fetchall()
            jobId = str(result[0][0])
            #job_label = job_label.encode('UTF-8','strict').hex()
            jobLabel = job_label
            cur = conn.cursor()
            query = "insert into jobs values (" + jobId
            if parent_id is None:
               query = "select min(id) from groups where user_id like '{}';".format(user_id)
               cur.execute(query)
               parent_id = cur.fetchone()[0]
            
            query += ", " + parent_id + ", '" + jobLabel + "', '" + user_id
            query += "', 0, now(), '" + url + "', '" + job_country +"', " + site+") returning id, label, country;"
            cur.execute(query)
            result = cur.fetchall()[0]
            conn.commit()

            query = "insert into job_current_working(job_id, crawling_working, mysite_working, targetsite_working ) values({},'Nothing running','Nothing running','Nothing running')".format(jobId)
            cur.execute(query)
            conn.commit()

            query = "insert into job_id_to_site_code(job_id) values({})".format(jobId)
            cur.execute(query)
            conn.commit()


            return {"success": True, "result": result}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return {"success": False}

    def copy_exist_job(self, job_id, job_label, user_id):
        try:
            cur = conn.cursor()
   
            # Get new job id
            query = "insert into group_job (type) values ('job') returning id;"
            cur.execute(query)
            result = cur.fetchall()
            new_job_id = str(result[0][0])
            new_job_label = job_label

            query = "insert into jobs(id, parent_id, label, user_id, count, last_update, url, country, site) (select {}, parent_id, '{}', user_id, 0, now(), url, country, site from jobs where id = {}) returning id, label, country, parent_id, url;".format(new_job_id, new_job_label, job_id)
            cur.execute(query)
            result = cur.fetchall()[0]

            query = "insert into job_current_working(job_id, crawling_working, mysite_working, targetsite_working ) values({},'Nothing running','Nothing running','Nothing running')".format(new_job_id)
            cur.execute(query)

            query = "insert into job_id_to_site_code(job_id) values({})".format(new_job_id)
            cur.execute(query)
             
            # user program
            query = "insert into user_program(site, description, program, job_id)  (select site, description, program, {} from user_program where job_id = {} order by id asc);".format(new_job_id, job_id)
            cur.execute(query)

            #my site job compare key
            query = "insert into mysite_compare_key(job_id, key_name)  (select {}, key_name from mysite_compare_key where job_id = {});".format(new_job_id, job_id)
            cur.execute(query)

            #targetsite_job_configuration
            query = "insert into targetsite_job_configuration(job_id, targetsite_id, targetsite_label, targetsite_url, t_category, transformation_program_id, cid, cnum, exchange_rate, tariff_rate, vat_rate, tariff_threshold, margin_rate, delivery_company, shipping_cost, max_num_items, default_weight, checked)  (select {}, targetsite_id, targetsite_label, targetsite_url, t_category, transformation_program_id, cid, cnum, exchange_rate, tariff_rate, vat_rate, tariff_threshold, margin_rate, delivery_company, shipping_cost, max_num_items, default_weight, checked from targetsite_job_configuration where job_id = {});".format(new_job_id, job_id)
            cur.execute(query)

            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return {"success": False}




    def remove_job(self, job_id):
        try:
            cur = conn.cursor()
            query = "delete from jobs where id = '" + job_id + "';"
            cur.execute(query)
            query = "delete from group_job where id = '" + job_id + "';"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('job_id')
        parser.add_argument('parent_id')
        parser.add_argument('url')
        parser.add_argument('job_label')
        parser.add_argument('job_country')
        parser.add_argument('site')
        args = parser.parse_args()
        if args['req_type'] == 'get_job_list':
            return self.get_job_list(args['user_id'])
        elif args['req_type'] == 'get_job_info':
            return self.get_job_info(args['job_id'])
        elif args['req_type'] == 'get_count_lastupdate':
            return self.get_count_lastupdate(args['job_id'])
        elif args['req_type'] == 'get_lastupdate':
            return self.get_lastupdate(args['job_id'])
        elif args['req_type'] == 'get_url':
            return self.get_url(args['job_id'])
        elif args['req_type'] == 'set_url':
            return self.set_url(args['job_id'], args['url'])
        elif args['req_type'] == 'get_country':
            return self.get_country(args['job_id'])
        elif args['req_type'] == 'make_new_job':
            return self.make_new_job(args['user_id'], args['parent_id'], args['url'], args['job_label'], args['job_country'], args['site'])
        elif args['req_type'] == 'remove_job':
            return self.remove_job(args['job_id'])
        elif args['req_type'] == 'copy_exist_job':
            return self.copy_exist_job(args['job_id'], args['job_label'], args['user_id'])
        elif args['req_type'] == 'get_num_of_product_in_job':
            return self.get_num_of_product_in_job(args['job_id'])

class TargetSiteManager(Resource): # added by mwseo
    def get_target_sites(self, user_id):
        try:
            cur = conn.cursor()
            query = "select id, name, url, gateway from targetsite where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 != 0:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False}
    def make_new_target_site(self, user_id, label, url, gateway):
        try:
            cur = conn.cursor()
            label = label.encode('UTF-8','strict').hex()
            url = url.encode('UTF-8','strict').hex()
            gateway = gateway.encode('UTF-8','strict').hex()
            query = "insert into targetsite (user_id, name, gateway, url, registration_date) values ('"
            query += user_id + "', '" + label + "','" +gateway+"', '" + url + "', now()) returning id;"
            cur.execute(query)
            result = cur.fetchall()[0]
            conn.commit()
            return {"success": True, "result":result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return {"success": False}
    def edit_target_site(self, id, label, url, gateway):
        try:
            cur = conn.cursor()
            label = label.encode('UTF-8','strict').hex()
            url = url.encode('UTF-8','strict').hex()
            gateway = gateway.encode('UTF-8','strict').hex()
            query = "update targetsite set name = '" + label + "', url = '" + url + "', gateway = '"+gateway+"' where id = " + id + ";"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def delete_target_site(self, id):
        try:
            cur = conn.cursor()
            query = "delete from targetsite where id = " + id + ";"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}

    def get_err_msg(self, mt_history_id):
        try:
            cur = conn.cursor()
            query = "select id, mpid, err_msg from failed_target_site_detail where mt_history_id = {}".format(mt_history_id)
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 2:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }


    def get_history(self, job_id):
        try:
            cur = conn.cursor()
            query = "select id, TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS'), targetsite, sm_history_id from mt_history where job_id = {} order by id desc".format(job_id)
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 3:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result": result}
        except:
            conn.rollback()
            return { "success": False }

    def get_latest_progress(self, job_id):
        try:
            cur = conn.cursor()
            query = "select num_expected_success, num_expected_all from mt_history where job_id = {} order by id desc limit 1".format(job_id)
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "result": result}
        except:
            conn.rollback()
            return { "success": False }




    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('label')
        parser.add_argument('id')
        parser.add_argument('url')
        parser.add_argument('gateway')
        parser.add_argument('mt_history_id')
        parser.add_argument('job_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_target_sites':
            return self.get_target_sites(args['user_id'])
        elif args['req_type'] == 'make_new_target_site':
            return self.make_new_target_site(args['user_id'], args['label'], args['url'],args['gateway'])
        elif args['req_type'] == 'edit_target_site':
            return self.edit_target_site(args['id'], args['label'], args['url'],args['gateway'])
        elif args['req_type'] == 'delete_target_site':
            return self.delete_target_site(args['id'])
        elif args['req_type'] == 'get_err_msg':
            return self.get_err_msg(args['mt_history_id'])
        elif args['req_type'] == 'get_history':
            return self.get_history(args['job_id'])
        elif args['req_type'] == 'get_latest_progress':
            return self.get_latest_progress(args['job_id'])

class DeliveryManager(Resource): # added by mwseo
    def get_delivery_companies(self, user_id):
        try:
            cur = conn.cursor()
            query = "select id, name from delivery_companies where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 1:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False}
    def make_new_delivery_company(self, user_id, label):
        try:
            cur = conn.cursor()
            label = label.encode('UTF-8','strict').hex()
            query = "insert into delivery_companies (user_id, name) values ('"
            query += user_id + "', '" + label + "');"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def edit_delivery_company(self, id, label):
        try:
            cur = conn.cursor()
            label = label.encode('UTF-8','strict').hex()
            query = "update delivery_companies set name = '" + label + "' where id = " + id + ";"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def get_delivery_countries(self, user_id):
        try:
            cur = conn.cursor()
            query = "select country_code from delivery_countries;"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return {"success": True, "result": result}
        except:
            conn.rollback()
            return {"success": False}
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('label')
        parser.add_argument('id')
        args = parser.parse_args()
        if args['req_type'] == 'get_delivery_companies':
            return self.get_delivery_companies(args['user_id'])
        elif args['req_type'] == 'make_new_delivery_company':
            return self.make_new_delivery_company(args['user_id'], args['label'])
        elif args['req_type'] == 'edit_delivery_company':
            return self.edit_delivery_company(args['id'], args['label'])
        elif args['req_type'] == 'get_delivery_countries':
            return self.get_delivery_countries(args['user_id'])


class TVRateManager(Resource):
    def get_rate(self, user_id):
        try:
            cur = conn.cursor()
            query = "select id, category, tariff_rate, vat_rate from tariff_vat_rate where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()

            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 1:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False}
    def get_rate_using_category(self, user_id,category):
        try:
            cur = conn.cursor()

            category = category.encode('UTF-8','strict').hex()
            query = "select id, tariff_rate, vat_rate from tariff_vat_rate where user_id = '" + user_id + "' and category = '" + category + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False}

    def add_rate(self, user_id, category,  tariff_rate, vat_rate):
        try:
            cur = conn.cursor()
            category = category.encode('UTF-8','strict').hex()
            query = "insert into tariff_vat_rate (user_id, category, tariff_rate, vat_rate) values ('"
            query += user_id + "', '" + category + "', '" + tariff_rate + "', '" + vat_rate + "');"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def update_rate(self, id, category, tariff_rate, vat_rate):
        try:
            cur = conn.cursor()
            category = category.encode('UTF-8','strict').hex()
            query = "update tariff_vat_rate set category = '"+ category + "', tariff_rate = " + tariff_rate + ", vat_rate = " + vat_rate + " where id = " + id + ";"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def delete_rate(self, id):
        try:
            cur = conn.cursor()
            query = "delete from tariff_vat_rate where id = " + id + ";"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('id')
        parser.add_argument('user_id')
        parser.add_argument('category')
        parser.add_argument('tariff_rate')
        parser.add_argument('vat_rate')
        args = parser.parse_args()
        if args['req_type'] == 'get_rate':
            return self.get_rate(args['user_id'])
        elif args['req_type'] == 'get_rate_using_category':
            return self.get_rate_using_category(args['user_id'], args['category'])
        elif args['req_type'] == 'add_rate':
            return self.add_rate(args['user_id'], args['category'], args['tariff_rate'], args['vat_rate'])
        elif args['req_type'] == 'update_rate':
            return self.update_rate(args['id'], args['category'], args['tariff_rate'], args['vat_rate'])
        elif args['req_type'] == 'delete_rate':
            return self.delete_rate(args['id'])


class MySiteCategoryTreeManager(Resource):
    def get_category_tree(self, user_id):
        try:
            cur = conn.cursor()
            query = "select category_tree from my_site_category_tree where user_id = '" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def add_category_tree(self, category_tree, user_id):
        try:
            cur = conn.cursor()
            sql = "insert into my_site_category_tree (category_tree, user_id) values ('"
            sql += category_tree + "', '" + user_id + "');"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def update_category_tree(self, category, user_id):
        try:
            cur = conn.cursor()
            sql = "select count(*) from my_site_category_tree where user_id = '{}'".format(user_id)
            cur.execute(sql)
            if cur.fetchone()[0] == 0:
               self.add_category_tree(category, user_id)
            else:
               sql = "update my_site_category_tree set category_tree = '"
               sql += category
               sql += "' where user_id ='"+ user_id+ "';"
               cur.execute(sql)
               conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('category_tree')
        parser.add_argument('user_id')
        args = parser.parse_args()
        if args['req_type'] == 'update_category_tree':
            return self.update_category_tree(args['category_tree'], args['user_id']);
        elif args['req_type'] == 'add_category_tree':
            return self.add_category_tree(args['category_tree'], args['user_id']);
        elif args['req_type'] == 'get_category_tree':
            return self.get_category_tree(args['user_id']);
        return { "success": False }





class CategoryTreeManager(Resource):
    def get_category_tree(self, user_id, targetsite_id):
        try:
            cur = conn.cursor()
            query = "select category_tree from category_tree where user_id = '" + user_id + "'and targetsite_id ='"+ targetsite_id +"';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def add_category_tree(self, category_tree, user_id, targetsite_id):
        try:
            cur = conn.cursor()
            sql = "insert into category_tree (category_tree, user_id, targetsite_id) values ('"
            sql += category_tree + "', '" + user_id + "', " + targetsite_id + ");"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def update_category_tree(self, category, targetsite_id):
        try:
            cur = conn.cursor()
            sql = "update category_tree set category_tree = '"
            sql += category
            sql += "' where targetsite_id ="+ targetsite_id+ ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('category_tree')
        parser.add_argument('user_id')
        parser.add_argument('targetsite_id')
        args = parser.parse_args()
        if args['req_type'] == 'update_category_tree':
            return self.update_category_tree(args['category_tree'], args['targetsite_id']);
        elif args['req_type'] == 'add_category_tree':
            return self.add_category_tree(args['category_tree'], args['user_id'], args['targetsite_id']);
        elif args['req_type'] == 'get_category_tree':
            return self.get_category_tree(args['user_id'],args['targetsite_id']);
        return { "success": False }



class ShippingFeeManager(Resource):
    def get_shipping_fee(self, user_id, company_id,country):
        try:
            cur = conn.cursor()
            query = "select id, min_kg, max_kg, fee from shipping_fee where user_id = '" + user_id + "'and delivery_company_id ="+ company_id +" and country ='" + country+"';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def add_shipping_fee(self, user_id, company_id, min_kg, max_kg, fee,country):
        try:
            cur = conn.cursor()
            sql = "insert into shipping_fee (user_id, delivery_company_id, min_kg, max_kg, fee, country) values ('"
            sql += user_id + "', " + company_id + ", " + min_kg + ", " + max_kg + ", " + fee + ", '" + country +"');"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def add_shipping_fees(self, user_id, company_id, fee_json, country):
        try:
            cur = conn.cursor()
            fee_arr = json.loads(fee_json)

            sql = "delete from shipping_fee where user_id = '" + user_id +"' and delivery_company_id = " + company_id + " and country ='"+country+ "';"
            cur.execute(sql)
            conn.commit()
            
            sql = "insert into shipping_fee (user_id, delivery_company_id, min_kg, max_kg, fee,country) values"
            for val in fee_arr:
                sql += "('" + user_id + "', " + company_id + ", " + val['min_kg'] + ", " + val['max_kg'] + ", " + val['fee'] + ", '"+ country+"'),"
            cur.execute(sql[:-1] + ";")
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

 
    def delete_shipping_fee(self, id):
        try:
            cur = conn.cursor()
            sql = "delete from shipping_fee where id = " + id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def update_shipping_fee(self, id, min_kg, max_kg, fee):
        try:
            cur = conn.cursor()
            sql = "update shipping_fee set min_kg = "
            sql += min_kg + ", max_kg = " + max_kg +", fee = " + fee 
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('company_id')
        parser.add_argument('min_kg')
        parser.add_argument('max_kg')
        parser.add_argument('fee')
        parser.add_argument('fee_array')
        parser.add_argument('country')
        parser.add_argument('id')
        args = parser.parse_args()
        if args['req_type'] == 'update_shipping_fee':
            return self.update_shipping_fee(args['id'], args['min_kg'], args['max_kg'], args['fee'], args['country']);
        elif args['req_type'] == 'add_shipping_fee':
            return self.add_shipping_fee(args['user_id'], args['company_id'], args['min_kg'], args['max_kg'], args['fee'], args['country']);
        elif args['req_type'] == 'add_shipping_fees':
            return self.add_shipping_fees(args['user_id'], args['company_id'], args['fee_array'], args['country']);
        elif args['req_type'] == 'get_shipping_fee':
            return self.get_shipping_fee(args['user_id'],args['company_id'], args['country']);
        elif args['req_type'] == 'delete_shipping_fee':
            return self.delete_shipping_fee(args['id'], args['country']);
        return { "success": False }



#id serial, user_id varchar(50) references account (user_id), targetsite_id integer, category varchar(1024), exchange_rate float, tariff_rate float, vat_rate float, tariff_threshold float, margin_rate float, min_margin float
class PricingInformationManager(Resource):
    def get_pricing_information(self, user_id,job_id, targetsite_id, category):
        try:
            cur = conn.cursor()
            query = "select exchange_rate, tariff_rate, vat_rate, tariff_threshold, margin_rate, min_margin from pricing_information where user_id = '" + user_id + "' and targetsite_id = " + targetsite_id + " and category ='"+ category +"';"
            cur.execute(query)
            rows_count = cur.execute(query)
            if rows_count == None:
               return { "success": True, "result" : {} }
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def save_mpid_pricing_information(self, job_id, mpid, min_margin, margin_rate, min_price, shipping_cost):
        try:
            cur = conn.cursor()
            mpid = int(mpid)
            query = "delete from selected_mpid_pricing_information where job_id = {} and mpid = {}".format(job_id, mpid)
            cur.execute(query)
            conn.commit()
            query = "insert into selected_mpid_pricing_information(job_id, mpid, min_margin, margin_rate, min_price, shipping_cost) values({}, {}, {}, {}, {}, {})".format(job_id, mpid, min_margin, margin_rate, min_price, shipping_cost)
            cur.execute(query)
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_mpid_pricing_information(self, job_id, mpid):
        try:
            cur = conn.cursor()
            mpid = int(mpid)
            query = "select min_margin, margin_rate, min_price, shipping_cost from selected_mpid_pricing_information where job_id = {} and mpid = {}".format(job_id, mpid)
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }





    def add_shipping_fee(self, user_id, company_id, min_kg, max_kg, fee):
        try:
            cur = conn.cursor()
            sql = "insert into shipping_fee (user_id, delivery_company_id, min_kg, max_kg, fee) values ('"
            sql += user_id + "', " + company_id + ", " + min_kg + ", " + max_kg + ", " + fee + ");"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }      
 
    def update_shipping_fee(self, id, min_kg, max_kg, fee):
        try:
            cur = conn.cursor()
            sql = "update shipping_fee set min_kg = "
            sql += min_kg + ", max_kg = " + max_kg +", fee = " + fee 
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('targetsite_id')
        parser.add_argument('category')
        parser.add_argument('job_id')
        parser.add_argument('id')
        parser.add_argument('mpid')
        parser.add_argument('min_margin')
        parser.add_argument('margin_rate')
        parser.add_argument('min_price')
        parser.add_argument('min_shipping_cost')
        args = parser.parse_args()
        if args['req_type'] == 'update_shipping_fee':
            return self.update_shipping_fee(args['id'], args['min_kg'], args['max_kg'], args['fee']);
        elif args['req_type'] == 'add_pricing_information':
            return self.add_pricing_information(args['user_id'], args['_id'], args['min_kg'], args['max_kg'], args['fee']);
        elif args['req_type'] == 'get_pricing_information':
            return self.get_pricing_information(args['user_id'], args['job_id'], args['targetsite_id'],args['category']);
        elif args['req_type'] == 'delete_shipping_fee':
            return self.delete_shipping_fee(args['id']);
        elif args['req_type'] == 'save_mpid_pricing_information':
            return self.save_mpid_pricing_information(args['job_id'],args['mpid'],args['min_margin'],args['margin_rate'],args['min_price'],args['min_shipping_cost']);
        elif args['req_type'] == 'get_mpid_pricing_information':
            return self.get_mpid_pricing_information(args['job_id'],args['mpid']);
        return { "success": False }



class TransformationProgramManager(Resource):
    def get_transformation_program(self, user_id):
        try:
            cur = conn.cursor()
            query = "select id, program_label, transformation_program from transformation_program where user_id = '" + user_id +"';"
            cur.execute(query)
            conn.commit()
            result = cur.fetchall()
            
            for idx, val in enumerate(result):
               lst = list(val)
               lst[1] = bytes.fromhex(val[1]).decode()
               lst[2] = bytes.fromhex(val[2]).decode()
               t = tuple(lst)
               result[idx] = t

            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def add_transformation_program(self, user_id, program_label, transformation_program):
        try:
            cur = conn.cursor()
            transformation_program = transformation_program.encode('UTF-8','strict').hex()
            program_label = program_label.encode('UTF-8','strict').hex()

            sql = "insert into transformation_program (user_id, program_label, transformation_program) values ('"
            sql += user_id + "', '" + program_label + "', '" + transformation_program + "');"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }      
 
    def update_transformation_program(self, id, program_label, transformation_program):
        try:
            cur = conn.cursor()
            transformation_program = transformation_program.encode('UTF-8','strict').hex()
            program_label = program_label.encode('UTF-8','strict').hex()
            sql = "update transformation_program set program_label = '"
            sql += program_label + "', transformation_program = '" + transformation_program +"'" 
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def delete_transformation_program(self, id):
        try:
            cur = conn.cursor()
            sql = "delete from transformation_program"
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('program_label')
        parser.add_argument('transformation_program')
        parser.add_argument('id')
        args = parser.parse_args()
        if args['req_type'] == 'update_transformation_program':
            return self.update_transformation_program(args['id'], args['program_label'], args['transformation_program']);
        elif args['req_type'] == 'add_transformation_program':
            return self.add_transformation_program(args['user_id'],args['program_label'], args['transformation_program']);
        elif args['req_type'] == 'get_transformation_program':
            return self.get_transformation_program(args['user_id']);
        elif args['req_type'] == 'delete_transformation_program':
            return self.delete_transformation_program(args['id']);
        return { "success": False }



class GatewayConfigurationManager(Resource):
    def get_configuration(self, user_id):
        try:
            cur = conn.cursor()
            query = "select id, configuration_label, configuration from gateway_configuration where user_id = '" + user_id +"';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def add_configuration(self, user_id, configuration_label, configuration):
        try:
            cur = conn.cursor()
            sql = "insert into gateway_configuration (user_id, configuration_label, configuration) values ('"
            sql += user_id + "', '" + configuration_label + "', '" + configuration + "');"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }      
 
    def update_configuration(self, id, configuration_label, configuration):
        try:
            cur = conn.cursor()
            sql = "update gateway_configuration set configuration_label = '"
            sql += configuration_label + "', configuration = '" + configuration +"'" 
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def delete_configuration(self, id):
        try:
            cur = conn.cursor()
            sql = "delete from gateway_configuration"
            sql += " where id ="+ id + ";"
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       


    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('configuration_label')
        parser.add_argument('configuration')
        parser.add_argument('id')
        args = parser.parse_args()
        if args['req_type'] == 'update_configuration':
            return self.update_configuration(args['id'], args['configuration_label'], args['configuration']);
        elif args['req_type'] == 'add_configuration':
            return self.add_configuration(args['user_id'],args['configuration_label'], args['configuration']);
        elif args['req_type'] == 'get_configuration':
            return self.get_configuration(args['user_id']);
        elif args['req_type'] == 'delete_configuration':
            return self.delete_configuration(args['id']);
        return { "success": False }



class ProductListManager(Resource):
    def get_product_description(self, job_id, mpid):
        try:
            cur = conn.cursor()
            query = "select key, value from job_description_source_view where mpid = {};".format(mpid)
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               lst = list(val)
               if is_hex_str(val[1]) == True:
                  if 'sha256' not in lst[0]: 
                     lst[1] = bytes.fromhex(val[1]).decode()
               t = tuple(lst)
               result[idx] = t
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_product_list(self, user_id, job_id, statu):
        try:
            cur = conn.cursor()
            statu = int(statu)
            
            if statu == -1:
               query = "select mpid, name, url, price, shipping_price, brand, weight, shipping_weight, shipping_price1, source_site_product_id, status, image_url, currency, stock, num_options, num_images from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4".format(job_id)
            else:
               query = "select mpid, name, url, price, shipping_price, brand, weight, shipping_weight, shipping_price1, source_site_product_id, status, image_url, currency, stock, num_options, num_images from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status = {};".format(job_id, statu)
            cur.execute(query)
            result = cur.fetchall()
            if len(result) == 0:
               result = []
            else:
               for idx, val in enumerate(result):
                  for idx2, val2 in enumerate(val):
                     if idx2 == 1 or idx2 == 3 or idx2 == 4 or idx2 == 5:
                        lst = list(result[idx])
                        if is_hex_str(val2) == False:
                           lst[idx2] = val2
                        else:
                           try:
                              lst[idx2] = bytes.fromhex(val2).decode()
                           except:
                              lst[idx2] = val2
                              pass
                        t = tuple(lst)
                        result[idx] = t
            query = "select mpid, node_id, -1 from failed_my_site_detail where sm_history_id in (select max(id) from sm_history where job_id = {})".format(job_id)
            cur.execute(query)
            result2 = cur.fetchall()
            for idx, val in enumerate(result2):
                query = "select value::text from node_property where key = 'name' and node_id = {}".format(val[1]) 
                cur.execute(query)
                name = cur.fetchone()[0][1:-1]
                if name == "" or name is None:
                    name = ""
                lst = list(result2[idx])
                lst.append(name)
                t = tuple(lst)
                result2[idx] = t
            if len(result2) != 0:
                result = result + result2
            result.sort(key = lambda x:x[0])
           
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_product_history(self, job_id):
        try:
            cur = conn.cursor()
            #query =  "select mpid, origin_product, cast(upload_time as text), converted_product from all_uploaded_product where job_id = 503 order by mpid desc, upload_time desc"
            query =  "select mpid, origin_product, cast(upload_time as text), converted_product from all_uploaded_product where job_id = {} order by mpid desc, upload_time desc".format(job_id)
            
            cur.execute(query)
            results = cur.fetchall()#[[mpid,dsfsdf]]
           
            for idx, result in enumerate(results):
                result = list(result)
                result[1] = json.loads(bytes.fromhex(result[1][1:-1]).decode())
                for key in ['num_options', 'num_images', 'id','m_category', 'tpid', 'groupby_key_sha256', 'source_site_product_id','url_sha256','name_sha256', 'html','smpid', 'cnum', 'shipping_fee', 'shipping_cost', 'brand', 'shipping_weight', 'weight', 'shpping_price']:
                    try:
                        result[1].pop(key)
                    except:
                        pass
                result[3] = json.loads(bytes.fromhex(result[3][1:-1]).decode())
                keys = result[3].keys()
                for key in list(keys):
                    if key not in ['product_name', 'targetsite_url']:
                        result[3].pop(key)
                results[idx] = tuple(result)
            return { "success": True, "result" : results }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }




    def get_product_history_name(self, job_id):
        try:
            cur = conn.cursor()
            #query =  "select mpid, origin_product, upload_time from all_uploaded_product where job_id = {}".format(job_id)
            #query =  "select mpid, converted_product from all_uploaded_product where job_id = 503 order by mpid asc, upload_time asc"
            query =  "select mpid, converted_product from all_uploaded_product where job_id = {} order by mpid asc, upload_time asc".format(job_id)
            
            cur.execute(query)
            results = cur.fetchall()
           
            for idx, result in enumerate(results):
                result = list(result)
                result[1] = json.loads(bytes.fromhex(result[1][1:-1]).decode())
                keys = result[1].keys()
                for key in list(keys):
                    if key not in ['product_name', 'targetsite_url']:
                        result[1].pop(key)
                results[idx] = tuple(result)
            return { "success": True, "result" : results }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }


    def get_product_history_targetsite(self, job_id, mpid):
        try:
            cur = conn.cursor()
            #query =  "select distinct targetsite_url from all_uploaded_product where job_id = 503 and mpid = {} order by targetsite_url asc".format(mpid)
            query =  "select distinct targetsite_url from all_uploaded_product where job_id = {} and mpid = {} order by targetsite_url asc".format(job_id, mpid)
            cur.execute(query)
            results = cur.fetchall()
            return { "success": True, "result" : results }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }


    def get_product_history_time(self, job_id, mpid, targetsite):
        try:
            cur = conn.cursor()
            #query =  "select mpid, origin_product, upload_time from all_uploaded_product where job_id = {}".format(job_id)
            #query =  "select mpid, cast(upload_time as text) from all_uploaded_product where job_id = 503 and mpid = {} and targetsite_url = '{}' order by mpid desc, upload_time desc".format(mpid, targetsite)
            query =  "select mpid, cast(upload_time as text) from all_uploaded_product where job_id = {} and mpid = {} and targetsite_url = '{}' order by mpid desc, upload_time desc".format(job_id, mpid, targetsite)
            
            cur.execute(query)
            results = cur.fetchall()#[[mpid,dsfsdf]]

            return { "success": True, "result" : results }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_product_history_detail(self, job_id, mpid, targetsite, upload_time):
        try:
            cur = conn.cursor()
            query =  "select origin_product, execution_id from all_uploaded_product where job_id = {} and mpid = {} and targetsite_url = '{}' and upload_time =  timestamp '{}' order by mpid desc, upload_time desc".format(job_id, mpid, targetsite, upload_time)
            cur.execute(query)
            result = cur.fetchone()
            results = {}
            tmp_result = json.loads(bytes.fromhex(result[0][1:-1]).decode())
            for key in ['sm_date', 'status', 'c_date', 'stock', 'image_url', 'url', 'name', 'price', 'currency', 'shipping_price', 'brand', 'weight', 'shipping_weight', 'source_site_product_id', 'option_value', 'pricing_information', 'brand', 'mpid']:
                if key == 'pricing_information':
                    if tmp_result.get('pricing_information','') != '' and (tmp_result.get('status','') != '3' or tmp_result.get('status','') != '0'):
                        for inner_key in tmp_result[key].keys():
                            results[inner_key] = tmp_result[key][inner_key]
                elif key == 'option_value':
                    tmp_result['option_value'] = tmp_result.get('option_value',"{}").replace("'",'"')
                    results[key] = json.loads(tmp_result['option_value'])
                else:
                    if key == 'name':
                        if tmp_result.get('Error','') != '':
                            results[key] = tmp_result.get(key, 'Error in logging upload product')
                        elif tmp_result.get('status','') == '3':
                            results[key] = tmp_result.get(key, 'Deleted')
                        else:
                            results[key] = tmp_result.get(key, '')
                    else: 
                        results[key] = tmp_result.get(key, '')
            exec_id = result[1] 
            query =  "select cast(start_time as text) from execution where id = {}".format(exec_id)
            cur.execute(query)
            crawling_time = cur.fetchone()[0]
            results['crawling_time'] = crawling_time
            return { "success": True, "result" : results }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }



    #def restore_mysite(self, job_id):
    #    try:
    #        cur = conn.cursor()
    #        query = "drop table if exists job"+job_id+"_source_view_latest;"
    #        cur.execute(query)
    #        query = "create table job"+job_id+"_source_view_latest as select * from job"+job_id+"_source_view_tmp_backup;"
    #        cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_source_view;"
    #        cur.execute(query)
    #        query = "create table job"+job_id+"_source_view as select * from job"+job_id+"_source_view_backup;"
    #        cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_option_source_view_latest;"
    #        cur.execute(query)
    #        query = "create table job"+job_id+"_option_source_view_latest as select * from job"+job_id+"_option_source_view_latest_backup;"
    #        cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_option_source_view;"
    #        cur.execute(query)
    #        query = "create table job"+job_id+"_option_source_view as select * from job"+job_id+"_option_source_view_backup;"
    #        cur.execute(query)
    #        conn.commit()
    #        
    #        return { "success": True}
    #    except:
    #        conn.rollback()
    #        print(str(traceback.format_exc()))
    #        return { "success": False }

    def delete_product(self, user_id, time):
        try:
            cur = conn.cursor()
            query = "select id from jobs where user_id = '{}';".format(user_id)
            cur.execute(query)
            result = cur.fetchall()

            time_condition = ''
            if int(time) == 1:
               time_condition = "now() - interval '7 days'";
            elif int(time) == 2:
               time_condition = "now() - interval '30 days'";
            elif int(time) == 3:
               time_condition = "now() - interval '90 days'";

            for res in result:
               job_id = res[0]
               # select * from job_source_view where sm_date >= timestamp '2020-09-10 20:00:00'; 
               query = "select mpid from job_id_and_mpid where job_id = {} and sm_date < {}".format(job_id, time_condition)
               cur.execute(query)
               result2 = cur.fetchall()
               for res in result2:
                  mpid = res[0]
                  query = "delete from job_source_view where mpid = ;".format(mpid)
                  cur.execute(query)
                  query = "delete from job_description_source_view where mpid = ;".format(mpid)
                  cur.execute(query)
                  query = "delete from job_thumbnail_source_view where mpid = ;".format(mpid)
                  cur.execute(query)
                  query = "delete from job_option_source_view where mpid = ;".format(mpid)
                  cur.execute(query)
            

            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_crawled_data_history(self, job_id):
        try:
            query = "select my_product_id, url from url_to_mpid where concat(\'\"\',url,\'\"\') in (select distinct value::text from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id in (select id from execution where job_id = {}) group by execution_id))) and key = 'url')".format(job_id)
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
        
            return { "success": True, "result": results }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }

    def get_crawled_summary(self, job_id):
        try:
            cur = conn.cursor()
            query = "select max(id) from execution where job_id = {}".format(job_id)
            cur.execute(query)
            execution_id = cur.fetchone()[0]
            query = "select max(id), min(id) from stage where execution_id = {}".format(execution_id)
            cur.execute(query)
            result = cur.fetchone() 
            max_s_id = result[0] 
            min_s_id = result[1] 

            query = "select t2.status, t3.value::text, t1.id from node as t1, (select id, status from task where stage_id in (select id from stage where execution_id = {} and id != {} and id != {})) as t2, node_property as t3 where t1.task_id = t2.id and t3.node_id = t1.id and t3.key = 'url' order by t1.id asc".format(execution_id,max_s_id, min_s_id)
            cur.execute(query)
            results = cur.fetchall() 
            return { "success": True, "result": results }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }


    def get_crawled_data(self, job_id):
        try:
            cur = conn.cursor()

            query = "select max(id) from execution where job_id = {}".format(job_id)
            cur.execute(query)
            execution_id = cur.fetchone()[0] # [[mpid], []]
            query = "select value::text, max(node_id) from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id = {}))) and key = 'url' group by value::text".format(execution_id)
            cur.execute(query)
            results = cur.fetchall() # [[mpid], []]
            new_results = []
            if len(results) != 0:
                node_ids = '('
                for res in results:
                    node_ids = node_ids + str(res[1]) + ', '
                node_ids = node_ids[0:-2] + ')'
                #query = "select t2.status, t3.value::text, t1.id from node as t1, (select id, status from task where stage_id in (select max(id) from stage where execution_id = {})) as t2, node_property as t3 where t1.task_id = t2.id and t3.node_id in {} and t3.node_id = t1.id and t3.key = 'name' and t2.status = 1 order by t1.id asc".format(execution_id, node_ids)
                query = "select t3.value::text, t1.id from node as t1, (select id, status from task where stage_id in (select max(id) from stage where execution_id = {})) as t2, node_property as t3 where t1.task_id = t2.id and t3.node_id in {} and t3.node_id = t1.id and t3.key = 'name' and t2.status = 1 order by t1.id asc".format(execution_id, node_ids)
                cur.execute(query)
                results = cur.fetchall() #[[status, name, node_id],[]]
                results_dictionary = {}
                for res in results:
                    node_id = res[1]
                    results_dictionary[node_id] = {'status': 1, 'name': res[0]} 

                query = "select t1.status, t2.id from task as t1, node as t2 where t1.id = t2.task_id and t2.id in {}".format(node_ids)
                cur.execute(query)
                results0 = cur.fetchall() #[[status, name, node_id],[]]
                for res in results0:
                    node_id = res[1]
                    if node_id in results_dictionary: 
                        results_dictionary[node_id]['status'] = res[0]
                    else:
                        results_dictionary[node_id] = {'status': res[0], 'name': '"-"'}
         

                #query = "select my_product_id, t2.node_id from url_to_mpid as t1, (select distinct value::text as url, node_id from node_property where node_id in {} and key = 'url') as t2 where concat(\'\"\',t1.url,\'\"\') = t2.url order by my_product_id asc".format(node_ids)
                query = "select my_product_id, t2.node_id from url_to_mpid as t1, (select distinct value::text as url, node_id from node_property where node_id in {} and key = 'url') as t2 where concat(\'\"\',t1.url,\'\"\') = t2.url or t1.url = t2.url order by my_product_id asc".format(node_ids)
                cur.execute(query)
                results1 = cur.fetchall() # [[mpid], []]
                for res in results1:
                    mpid = res[0]
                    node_id = res[1]
                    status = results_dictionary.get(node_id, {'status': -1})['status']
                    name = results_dictionary.get(node_id, {'name':'"-"'})['name']
                    name = name.encode('ascii').decode('unicode_escape')
                    if status == -997:
                        name = '"-"'
                    new_results.append([status, name, node_id, mpid])
            return { "success": True, "result": new_results }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }

    def get_product_detail(self, node_id):
        try:
            query = "select key, value from node_property where node_id = {} and key != 'html'".format(node_id)
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()
            currency = ''
            for idx, val in enumerate(result):
                if val[0] == 'price':
                    lst = list(result[idx])
                    lst[1] = Price.fromstring(val[1]).amount_float;
                    currency = Price.fromstring(val[1]).currency
                    t = tuple(lst)
                    result[idx] = t

            result.append(('currency', currency))
            return { "success": True, "result": result }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }




    def get_crawled_time(self, job_id, url):
        try:
            query = "select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id in (select id from execution where job_id = {}) group by execution_id))) and key = 'url' and value::text='\"{}\"' order by node_id desc;".format(job_id, url)
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            node_ids = '('
            for idx, value in enumerate(results):
                node_ids = node_ids + str(value[0]) + ', '
            node_ids = node_ids[:-2] + ')'

            query = "select execution_id from stage where id in (select stage_id from task where status = -1 and id in (select task_id from node where id in (select node_id from node_property where node_id in {} and key = 'url' and value::text='\"{}\"' order by node_id desc))) order by execution_id desc".format(node_ids, url)
            cur = conn.cursor()
            cur.execute(query)
            result_fail = cur.fetchall()
            result_fail_exec_id = []
            for idx, value in enumerate(result_fail):
               result_fail_exec_id.append(value[0])


            query = "select execution_id from stage where id in (select stage_id from task where status = -999 and id in (select task_id from node where id in (select node_id from node_property where node_id in {} and key = 'url' and value::text='\"{}\"' order by node_id desc))) order by execution_id desc".format(node_ids, url)
            cur = conn.cursor()
            cur.execute(query)
            result_invalid = cur.fetchall()
            result_invalid_exec_id = []
            for idx, value in enumerate(result_invalid):
               result_invalid_exec_id.append(value[0])
       
            query = "select execution_id, cast(start_time as text) from stage where id in (select stage_id from task where id in (select task_id from node where id in (select node_id from node_property where node_id in {} and key = 'url' and value::text='\"{}\"' order by node_id desc))) order by execution_id desc".format(node_ids, url)
            cur = conn.cursor()
            cur.execute(query)
            result_exec = cur.fetchall()
            for idx, value in enumerate(result_exec):
               result_exec_list = list(result_exec[idx])
               result_exec_list.append(results[idx][0])
               if value[0] in result_invalid_exec_id:
                  result_exec_list.append("Invalid")
               elif value[0] in result_fail_exec_id:
                  result_exec_list.append("Fail")
               else:
                  result_exec_list.append("Valid")
               result_exec[idx] = tuple(result_exec_list)
 
            return { "success": True, "result": result_exec }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False }




    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('job_id')
        parser.add_argument('url')
        parser.add_argument('statu')
        parser.add_argument('mpid')
        parser.add_argument('time')
        parser.add_argument('targetsite')
        parser.add_argument('upload_time')
        parser.add_argument('node_id')
        parser.add_argument('execution_id')
        args = parser.parse_args()
        print(args)
        if args['req_type'] == 'get_product_list':
            return self.get_product_list(args['user_id'], args['job_id'],args['statu']);
        elif args['req_type'] == 'get_product_history':
            return self.get_product_history(args['job_id']);
        elif args['req_type'] == 'get_product_history_name':
            return self.get_product_history_name(args['job_id']);
        elif args['req_type'] == 'get_product_history_targetsite':
            return self.get_product_history_targetsite(args['job_id'], args['mpid']);
        elif args['req_type'] == 'get_product_history_time':
            return self.get_product_history_time(args['job_id'], args['mpid'], args['targetsite']);
        elif args['req_type'] == 'get_product_history_detail':
            return self.get_product_history_detail(args['job_id'], args['mpid'], args['targetsite'], args['upload_time']);
        elif args['req_type'] == 'get_product_description':
            return self.get_product_description(args['job_id'], int(args['mpid']));
        elif args['req_type'] == 'restore_mysite':
            return self.restore_mysite(args['job_id']);
        elif args['req_type'] == 'delete_product':
            return self.delete_product(args['user_id'], args['time']);
        elif args['req_type'] == 'get_crawled_data_history':
            return self.get_crawled_data_history(args['job_id']);
        elif args['req_type'] == 'get_crawled_data':
            return self.get_crawled_data(args['job_id']);
        elif args['req_type'] == 'get_crawled_summary':
            return self.get_crawled_summary(args['job_id']);
        elif args['req_type'] == 'get_crawled_time':
            return self.get_crawled_time(args['job_id'], args['url']);
        elif args['req_type'] == 'get_product_detail':
            return self.get_product_detail(args['node_id']);
        return { "success": False }

class ProductOptionsManager(Resource):
    def get_product_options(self, user_id, job_id, pid):
        try:
            cur = conn.cursor()
            query = "select * from job_option_source_view where mpid = {} and (option_name != '6f7074696f6e5f6d6178747269785f76616c7565' and option_name != '6f7074696f6e5f6d61747269785f636f6c5f6e616d65' and option_name != '6f7074696f6e5f6d61747269785f726f775f6e616d65');".format(pid)
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 2 or idx2 == 3:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('job_id')
        parser.add_argument('pid')
        args = parser.parse_args()
        if args['req_type'] == 'get_product_options':
            return self.get_product_options(args['user_id'], args['job_id'],args['pid']);
        return { "success": False }


class ExchangeRateManager(Resource):
    def update_exchange_rate(self, user_id):
        try:
        
            url = "https://okbfex.kbstar.com/quics?page=C015690"
            
            payload = {}
            headers = {
              'Cookie': '_LOG_VSTRIDNFIVAL=KcvHSEGpQIOLu7ueb9edpQ; JSESSIONID=0000j3QBXZQlJjo7R0RgeoOwMff:KBF20201; QSID=22F1&&j3QBXZQlJjo7R0RgeoOwMff; _xm_webid_1_=1488676972; WMONID=V9Eootl8A2X'
            }
            
            response = requests.request("GET", url, headers=headers, data = payload)
            

            tree = html.fromstring(response.text)
            #exchange_rate_str = tree.xpath('//div[@id="showTable"]//tr[1]//td[5]/text()')
            #update_time = tree.xpath('//div[@class="hitday_area s7"]//span[@class="hitday"]/text()')
            nation_list = tree.xpath('//div[@id="showTable"]//tr//td[@scope="row"]//a/text()')
            exchange_rate_list = tree.xpath('//div[@id="showTable"]//tr//td[5]/text()') 
            update_time = tree.xpath('//div[@class="hitday_area s7"]//span[@class="hitday"]/text()')
             
            new_exchange_rate_list = []
            new_nation_list = []
            calculated_usd = 0
            amex_fees = 0.014
            uri_fees = 0.003
            for idx, val in enumerate(exchange_rate_list):
               if idx == 0:
                  exchange_rate_with_fees = float(val.replace(',','')) * (1 + amex_fees) * (1 + uri_fees)
               elif val != '-':
                  a = 1014*(float(val.replace(',','')))
                  exchange_rate_with_fees = float(a * 1.003 / 1000)
               new_exchange_rate_list.append(exchange_rate_with_fees)


            for idx, val in enumerate(nation_list):
               if idx %2 == 0:
                  new_nation_list.append(val.replace('\r\n','').strip())
                      
            nation_er_list = {}
            for idx, val in enumerate(new_nation_list):
               nation_er_list[new_nation_list[idx]] = new_exchange_rate_list[idx]
            exchange_rate_with_fees = 1300
            result = ""
            #조회기준고시회차 :&nbsp;2021.02.08 &nbsp;127회차&nbsp;&nbsp;적용시각 : 16:21:49
            update_time_str = update_time[0].split('\xa0')[1].split(' ')[0] + ' ' +update_time[0].split('\xa0')[4].split(' ')[2]
            conn.commit()
            query = "insert into exchange_rate(user_id, exchange_rate, update_time) values('"+user_id+"','"+ json.dumps(nation_er_list)+ "','"+update_time_str+ "')"
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def get_exchange_rate(self, user_id):
        try:

            cur = conn.cursor()
            query = "select exchange_rate, update_time from exchange_rate where user_id = '" + user_id + "' order by id desc limit 1"
            cur.execute(query)
            conn.commit()
            result = cur.fetchall()
 
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False }

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        args = parser.parse_args()
        if args['req_type'] == 'update_exchange_rate':
            return self.update_exchange_rate(args['user_id']);
        elif args['req_type'] == 'get_exchange_rate':
            return self.get_exchange_rate(args['user_id']);
        return { "success": False }



class MySiteManager(Resource):
    def save_to_mysite(self, arg):
        try:
            cur = conn.cursor()
            query = ""
            if(int(statu) == 0):
              query = "select * from products where user_id = '" + user_id +"' and job_id = "+job_id+ ";"
            else:
              query = "select * from products where user_id = '" + user_id +"' and job_id = "+job_id+ " and status = "+ statu+ ";"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_err_msg(self, sm_history_id):
        try:
            cur = conn.cursor()
            query = "select id, mpid, err_msg from failed_my_site_detail where sm_history_id = {}".format(sm_history_id)
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_err_msg_mpid(self, sm_history_id, mpid):
        try:
            cur = conn.cursor()
            query = "select id, mpid, err_msg from failed_my_site_detail where sm_history_id = {} and mpid = {}".format(sm_history_id, mpid)
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            print(result)
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }




    def get_column_name(self):
        try:
            cur = conn.cursor()
            query = "select column_name from information_schema.columns where table_name = 'job_source_view' order by column_name"
            cur.execute(query)
            result = cur.fetchall()
            col_names = []
            for i in result:
                if i[0] not in ['id', 'job_id', 'c_date', 'sm_date', 'url_sha256', 'name_sha256', 'image_url', 'num_options', 'num_images', 'm_category', 'm_category', 'tpid', 'groupby_key_sha256', 'name_translated', 'description_translated' , 'description_rendered', 'description1_rendered', 'description2_rendered', 'description1_translated', 'description2_translated']:
                    col_names.append(i[0])

            return { "success": True, "result" : col_names }
        except:
            conn.rollback()
            return { "success": False }



    def get_compare_key(self, job_id):
        try:
            cur = conn.cursor()
            query = "select id, key_name from mysite_compare_key where job_id = {}".format(job_id);
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 1:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }


    def add_compare_key(self, job_id, key_name):
        try:
            cur = conn.cursor()
            key_name = key_name.encode('UTF-8','strict').hex()
            query = "insert into mysite_compare_key(job_id, key_name) values({}, '{}')".format(job_id, key_name);
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "result": str(traceback.format_exc()) }


    def delete_compare_key(self, key_id):
        try:
            cur = conn.cursor()
            query = "delete from mysite_compare_key where id = {}".format(key_id)
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            return { "success": False }

    def get_translate_key(self, job_id):
        try:
            cur = conn.cursor()
            query = "select id, key_name from mysite_translate_key where job_id = {}".format(job_id);
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 1:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t
            conn.commit()
            return { "success": True, "result" : result }
        except:
            conn.rollback()
            return { "success": False }



    def add_translate_key(self, job_id, key_name):
        try:
            cur = conn.cursor()
            key_name = key_name.encode('UTF-8','strict').hex()
            query = "insert into mysite_translate_key(job_id, key_name) values({}, '{}')".format(job_id, key_name);
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            print(str(traceback.format_exc()))
            return { "success": False, "result": str(traceback.format_exc()) }


    def delete_translate_key(self, key_id):
        try:
            cur = conn.cursor()
            query = "delete from mysite_translate_key where id = {}".format(key_id)
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            return { "success": False }



    def get_history(self, job_id):
        try:
            cur = conn.cursor()
            #(id integer primary key generated always as identity, execution_id integer,  start_time timestamp, end_time timestamp, job_id integer);
            query = "select id, execution_id, TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS') from sm_history where job_id = {} order by id desc".format(job_id)
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            print(result)
            return { "success": True, "result": result}
        except:
            conn.rollback()
            return { "success": False }

    def get_latest_progress(self, job_id):
        try:
            cur = conn.cursor()
            query = "select num_expected_all, num_expected_success from sm_history where job_id = {} order by id desc limit 1".format(job_id)
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "result": result}
        except:
            conn.rollback()
            return { "success": False }



    def get_smhid(self, job_id):
        try:
            cur = conn.cursor()
            query = "select max(id) from sm_history where job_id = {}".format(job_id)
            cur.execute(query)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "result": result}
        except:
            conn.rollback()
            return { "success": False }




    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('arg')
        parser.add_argument('job_id')
        parser.add_argument('key_name')
        parser.add_argument('key_id')
        parser.add_argument('sm_history_id')
        parser.add_argument('mpid')
        args = parser.parse_args()
        print(args)
        if args['req_type'] == 'save_to_mysite':
            return self.save_to_mysite(args['arg']);
        elif args['req_type'] == 'get_compare_key':
            return self.get_compare_key(args['job_id']);
        elif args['req_type'] == 'get_translate_key':
            return self.get_translate_key(args['job_id']);
        elif args['req_type'] == 'add_compare_key':
            return self.add_compare_key(args['job_id'], args['key_name']);
        elif args['req_type'] == 'delete_compare_key':
            return self.delete_compare_key(args['key_id']);
        elif args['req_type'] == 'add_translate_key':
            return self.add_translate_key(args['job_id'], args['key_name']);
        elif args['req_type'] == 'delete_translate_key':
            return self.delete_translate_key(args['key_id']);
        elif args['req_type'] == 'get_history':
            return self.get_history(args['job_id']);
        elif args['req_type'] == 'get_err_msg':
            return self.get_err_msg(args['sm_history_id']);
        elif args['req_type'] == 'get_err_msg_mpid':
            return self.get_err_msg_mpid(args['sm_history_id'], args['mpid']);
        elif args['req_type'] == 'get_column_name':
            return self.get_column_name();
        elif args['req_type'] == 'get_latest_progress':
            return self.get_latest_progress(args['job_id']);
        elif args['req_type'] == 'get_smhid':
            return self.get_smhid(args['job_id']);
        return { "success": False }


#(job_id integer, start_date timestamp, end_date timestamp, period integer, m_category varchar(2048), targetsite_id varchar(2048), t_category varchar(2048), transformation_program_id integer)
class JobPropertiesManager(Resource):
    def save_job_properties(self, job_id, start_date, end_date, period, m_category, num_worker, num_thread):
        try:
            cur = conn.cursor()
            query = "delete from job_configuration where job_id = "+str(job_id)+";"
            cur.execute(query)

            query = "select id from targetsite_job_configuration where job_id = {}".format(job_id)
            cur.execute(query)
            results = cur.fetchall()
            for res in results:
               tsid = res[0]
               query = "insert into job_configuration(job_id, tsid, start_date, end_date, period, m_category, num_worker, num_thread ) values({}, {}, '{}', '{}',{}, '{}', {}, {})".format(job_id, tsid, str(start_date), str(end_date), period, m_category, num_worker, num_thread)
               cur.execute(query)

            #query = "insert into job_configuration(job_id, start_date, end_date, period, m_category, num_worker, num_thread ) values("+ str(job_id)+", '"+str(start_date) +"', '"+str(end_date) +"', "+str(period)+", '"+ str(m_category)+"', "+ str(num_worker)+","+str(num_thread)+");"
            #cur.execute(query)

            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def check_target_site(self, tjcid, checked):
        try:
            cur = conn.cursor()
            query = "update targetsite_job_configuration set checked = {} where id = {};".format(checked, tjcid)
            cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }


    def check_true_all_target_site(self, job_id):
        try:
            cur = conn.cursor()
            query = "update targetsite_job_configuration set checked = True where job_id = {};".format(job_id)
            cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def check_false_all_target_site(self, job_id):
        try:
            cur = conn.cursor()
            query = "update targetsite_job_configuration set checked = False where job_id = {};".format(job_id)
            cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def deregister_target_site(self, tjcid):
        try:
            cur = conn.cursor()
            query = "delete from targetsite_job_configuration where id = {};".format(tjcid)
            cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }



    def register_target_site(self, job_id, targetsite_id,targetsite_label, targetsite_url, t_category, transformation_id, exchange_rate, tariff_rate, vat_rate, margin_rate, min_margin,tariff_threshold, delivery_company, cid, cnum, max_items, default_weight):
        try:
            cur = conn.cursor()
            targetsite_label = targetsite_label.encode('UTF-8','strict').hex()
            targetsite_url = targetsite_url.encode('UTF-8','strict').hex()
            t_category = t_category.encode('UTF-8','strict').hex()

            query = "select count(*) from targetsite_job_configuration where targetsite_url = '{}' and t_category = '{}' and job_id = '{}'".format(targetsite_url, t_category, job_id)
            cur.execute(query)
            rows = cur.fetchone()
            result = rows[0]
            if int(result) >= 1:
              return { "success": True, "result": False}
            else:
              query = "insert into targetsite_job_configuration(job_id, targetsite_id, targetsite_label, targetsite_url, t_category, transformation_program_id, exchange_rate, tariff_rate, vat_rate, margin_rate, min_margin, tariff_threshold, delivery_company, default_weight, cid, cnum, max_num_items) values(" + str(job_id) + ", " + str(targetsite_id) + ", '" + str(targetsite_label) +  "', '" + str(targetsite_url) + "', '" +str(t_category) +"', "+ str(transformation_id)+", " + str(exchange_rate)+", "+str(tariff_rate)+", "+str(vat_rate)+", "+str(margin_rate)+", "+str(min_margin)+","+tariff_threshold+", '"+str(delivery_company)+"',"+default_weight+ ", "+ str(cid)+", "+str(cnum)+", " + str(max_items) + ");"
              cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }




    def update_target_site(self, rtid, targetsite_id,targetsite_label, targetsite_url, t_category, transformation_id, exchange_rate, tariff_rate, vat_rate, margin_rate, min_margin,tariff_threshold, delivery_company, cid, cnum, max_items, default_weight):
        try:
            cur = conn.cursor()
            query = "select targetsite_id from targetsite_job_configuration where id = {}".format(rtid)
            cur.execute(query)
            tid = cur.fetchone()[0]
            if (int(tid) != int(targetsite_id)):
               return { "success": True, "result": False}


            targetsite_label = targetsite_label.encode('UTF-8','strict').hex()
            targetsite_url = targetsite_url.encode('UTF-8','strict').hex()
            t_category = t_category.encode('UTF-8','strict').hex()

            query = "update targetsite_job_configuration set targetsite_id = {}, targetsite_label = '{}', targetsite_url = '{}', t_category = '{}', transformation_program_id = {}, exchange_rate = {}, tariff_rate = {}, vat_rate = {}, margin_rate = {}, min_margin = {}, tariff_threshold = {}, delivery_company = '{}', default_weight = {}, cid = {}, cnum = {}, max_num_items = {} where id = {}".format(targetsite_id, targetsite_label, targetsite_url, t_category, transformation_id, exchange_rate, tariff_rate, vat_rate, margin_rate, min_margin, tariff_threshold, delivery_company, default_weight, cid, cnum, max_items, rtid)
            cur.execute(query)
            conn.commit()
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }


    def get_registered_target_site(self, job_id):
        try:
            cur = conn.cursor()
            query = "select id, targetsite_label, targetsite_url, t_category, cnum, max_num_items, checked from targetsite_job_configuration where job_id = "+str(job_id)+" order by id asc;"
            cur.execute(query)
            result = cur.fetchall()
            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 1 or idx2 == 2 or idx2 == 3:
                     lst = list(result[idx])
                     if is_hex_str(val2) == False:
                        lst[idx2] = val2
                     else:
                        lst[idx2] = bytes.fromhex(val2).decode()
                     t = tuple(lst)
                     result[idx] = t

            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }


    def get_registered_schedules(self, user_id):
        try:
            cur = conn.cursor()
            query = "select job_id, start_date, end_date, period, activate, targetsites from registered_schedules where user_id = '{}';".format(user_id)
            cur.execute(query)
            result = cur.fetchall()

            for idx, val in enumerate(result):
               for idx2, val2 in enumerate(val):
                  if idx2 == 0:
                     lst = list(result[idx])
                     query = "select label from jobs where id = {}".format(val2)
                     cur.execute(query)
                     job_label = cur.fetchone()[0]
                     if is_hex_str(job_label) == True:
                       job_label = bytes.fromhex(job_label).decode()

                     query = "select parent_id from jobs where id = {}".format(val2)
                     cur.execute(query)
                     parent_id = cur.fetchone()[0]

                     query = "select label from groups where id = {}".format(parent_id)
                     cur.execute(query)
                     group_label = cur.fetchone()[0]
                     if is_hex_str(group_label) == False:
                       lst.append(group_label + " / " + job_label)
                     else:
                       lst.append( bytes.fromhex(group_label).decode() + " / " + job_label)

                     t = tuple(lst)
                     result[idx] = t
                  elif idx2 == 5:
                     lst = list(result[idx])
                     if val2 is None:
                        val2 = ' '.encode('UTF-8','strict').hex()
                     lst.append( bytes.fromhex(val2).decode())
                     t = tuple(lst)
                     result[idx] = t
            #self.make_airflow_script(job_id, start_date, end_date, period)
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }



    def delete_schedule(self, job_id):
        try:
            cur = conn.cursor()
            query = "delete from registered_schedules where job_id = {}".format(job_id)
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }



    def set_activate_schedule(self, job_id):
        try:
            cur = conn.cursor()
            query = "update registered_schedules set activate = true where job_id = {}".format(job_id)
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def set_deactivate_schedule(self, job_id):
        try:
            cur = conn.cursor()
            query = "update registered_schedules set activate = false where job_id = {}".format(job_id)
            cur.execute(query)
            conn.commit()
            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def load_job_properties(self, job_id):
        try:
            cur = conn.cursor()
            query = "select COALESCE(to_char(start_date, 'YYYY-MM-DD hh:mm:ss'), ''), COALESCE(to_char(end_date, 'YYYY-MM-DD hh:mm:ss'), ''), period, m_category, num_worker, num_thread from job_configuration where job_id = "+str(job_id)+" limit 1;"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }


    def load_pricing_information(self, job_id, tid, tc):
        try:
            cur = conn.cursor()
            if tc is None:
                return { "success": False, "result": "target site category is None"}
            tc = tc.encode('UTF-8','strict').hex()
            query = "select exchange_rate, tariff_rate, vat_rate, tariff_threshold, margin_rate, min_margin, delivery_company, default_weight, cnum, transformation_program_id, cid, max_num_items from targetsite_job_configuration where job_id = "+str(job_id)+" and targetsite_id = "+str(tid)+" and t_category = '"+str(tc)+"';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }




    def load_tmp_pricing_information(self, job_id):
        try:
            cur = conn.cursor()
            query = "select * from pricing_information where job_id = "+str(job_id)+";"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def set_job_working(self, job_id):
        try:
            cur = conn.cursor()
            query = "insert into job_current_working(job_id, crawling_working, mysite_working, targetsite_working ) values({},'Nothing running','Nothing running','Nothing running')".format(job_id)
            cur.execute(query)
            conn.commit()
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }



    def load_job_working(self, job_id):
        try:
            cur = conn.cursor()
            query = "select crawling_working, mysite_working, targetsite_working from job_current_working where job_id = {}".format(job_id)
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "result": result}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }

    def make_airflow_script(self, job_id, start_date, end_date, period, check_c, check_m, check_t, user_id):
        try:
            cur = conn.cursor()
            query = "select label from jobs where id = {};".format(job_id)
            cur.execute(query)

            job_label = cur.fetchone()[0]
            job_label = job_label.replace(' ','_')
            dag_id = job_label
            print("airflow delete_dag -y {}".format(dag_id))
            try:
               #driver.run_from_db(sysinfo, False)
               subprocess.Popen("airflow delete_dag -y %s" % str(dag_id), shell=True)
               os.remove('/home/pse/airflow/dags/'+dag_id+'_scheduling_program.py')
            except:
               pass

            query = "select id from user_program where job_id = {} order by id desc limit 1;".format(job_id)
            cur.execute(query)
            upid = cur.fetchone()[0]

            query = "select id from targetsite_job_configuration where job_id = {} and checked = True".format(job_id)
            cur.execute(query)
            tjcids = cur.fetchall()
            check_c = True if check_c == 'True' else False
            check_m = True if check_m == 'True' else False
            check_t = True if check_t == 'True' else False

            scheduling_code_for_airflow='''
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
import pendulum 
from datetime import datetime
from datetime import timedelta

local_tz = pendulum.timezone("Asia/Seoul")

start_date = "'''+start_date+'''"
end_date = "'''+end_date+'''"
period = "'''+str(period)+'''"
job_id = "'''+str(job_id)+'''"
start_year = start_date.split(' ')[0].split('-')[0]
start_month  = start_date.split(' ')[0].split('-')[1]
start_day  = start_date.split(' ')[0].split('-')[2]
start_h  = start_date.split(' ')[1].split(':')[0]
start_m  = start_date.split(' ')[1].split(':')[1]
start_s  = start_date.split(' ')[1].split(':')[2]

end_year = end_date.split(' ')[0].split('-')[0]
end_month  = end_date.split(' ')[0].split('-')[1]
end_day  = end_date.split(' ')[0].split('-')[2]
end_h  = end_date.split(' ')[1].split(':')[0]
end_m  = end_date.split(' ')[1].split(':')[1]
end_s  = end_date.split(' ')[1].split(':')[2]


#args = {'owner': 'pse', 'start_date': datetime(2020, 9, 10, 20, 49, 0, tzinfo=local_tz)}
args = {'owner': 'pse', 'start_date': datetime(int(start_year), int(start_month), int(start_day), int(start_h), int(start_m), int(start_s), tzinfo=local_tz) - timedelta(days=''' + str(period) + '''), 'end_date': datetime(int(end_year), int(end_month), int(end_day), int(end_h), int(end_m), int(end_s), tzinfo=local_tz)}

dag = DAG(dag_id="''' + job_label + '''",
           default_args=args,
           #schedule_interval= start_m + ' ' + start_h+' */'+period+' * *', #min, h, day, month, year
           schedule_interval= timedelta(days=''' + str(period) + '''),
           catchup=False)
'''
        
            if check_c == True:
              scheduling_code_for_airflow +='''
#crawling
t1 = BashOperator(task_id="'''+job_label+'''_crawling",
                  bash_command='python /home/pse/pse-driver/pse_driver.py --c run_from_db --wf '''+str(upid)+''' --job_id ''' + job_id + '''',
                  dag=dag)'''

            if check_m == True:
              query = 'select key_name from mysite_compare_key where job_id = {}'.format(job_id)
              cur.execute(query)
              conn.commit()
              tmp = cur.fetchall()
              groupby_key = ''
              for i in tmp:
                groupby_key += bytes.fromhex(i[0]).decode() + ' '
              scheduling_code_for_airflow +='''
              
# my site update / upload
t2 = BashOperator(task_id="'''+job_label+'''_StoM",
                  bash_command='python /home/pse/pse-driver/pse_driver.py --c update_to_mysite --job_id '''+job_id+''' --groupbykey '''+groupby_key + ''' ', dag=dag)

'''

            if check_t == True:
              for tjcid in tjcids:
                scheduling_code_for_airflow +='''
# target site upload
upload''' +str(tjcid[0]) +''' = BashOperator(task_id="'''+job_label+'''_MtoT'''+str(tjcid[0])+'''",
                  bash_command='python /home/pse/pse-driver/cafe24_driver.py --cafe24_c run_scheduled --job_id ''' + job_id + ''' --cafe24_host 127.0.0.1 --target_id ''' + str(tjcid[0]) + ''' --cafe24_port 6379 --cafe24_queue cafe24_queue', 
                  dag=dag)

'''


            upload_ids_string = '['
            for tjcid in tjcids:
              upload_ids_string += 'upload'+str(tjcid[0]) +', '
            upload_ids_string = upload_ids_string[:-2]
            upload_ids_string += ']'

            if check_c == True and check_m != True and check_t != True:
              scheduling_code_for_airflow +='''
t1 
'''
            elif check_c == True and check_m == True and check_t != True:
              scheduling_code_for_airflow +='''
t1 >> t2
'''
            elif check_c == True and check_m != True and check_t == True:
              scheduling_code_for_airflow +='''
t1 >> ''' + upload_ids_string +'''
'''
            elif check_c == True and check_m == True and check_t == True:
              scheduling_code_for_airflow +='''
t1 >> t2 >> ''' +upload_ids_string +'''
'''
            elif check_c != True and check_m != True and check_t != True:
              scheduling_code_for_airflow +='''

'''
            elif check_c != True and check_m == True and check_t != True:
              scheduling_code_for_airflow +='''
t2
'''
            elif check_c != True and check_m != True and check_t == True:
              scheduling_code_for_airflow +='''
t3
'''
            elif check_c != True and check_m == True and check_t == True:
              scheduling_code_for_airflow +='''
t2 >> ''' +upload_ids_string+'''
'''
            
            fname = '/home/pse/airflow/dags/'+job_label+'_scheduling_program.py'
            if os.path.exists(fname):
              os.remove(fname)
            with open(fname, 'w') as f:
                f.write(scheduling_code_for_airflow)

            query = "delete from registered_schedules where job_id = {}".format(job_id)
            cur.execute(query)
            conn.commit()

            query = "select targetsite_label from targetsite_job_configuration where job_id = {} and checked = True".format(job_id)
            cur.execute(query)
            targetsites = ""
            result = cur.fetchall()
            for idx, val in enumerate(result):
              for idx2, val2 in enumerate(val):
                 if idx2 == 0:
                    lst = list(result[idx])
                    if is_hex_str(val2) == True:
                       lst[idx2] = bytes.fromhex(val2).decode()
                       if idx == 0:
                         targetsites = targetsites + str(bytes.fromhex(val2).decode())
                       else:
                         targetsites = targetsites +", " + str(bytes.fromhex(val2).decode())
            targetsites = str(targetsites).encode('UTF-8','strict').hex()
            query = "insert into registered_schedules(user_id, job_id, start_date, end_date, period, targetsites) values('{}', {},'{}','{}',{}, '{}')".format(user_id, job_id, start_date, end_date, period, targetsites)
            cur.execute(query)
            conn.commit()

            return { "success": True}
        except:
            print(str(traceback.format_exc()))
            conn.rollback()
            return { "success": False }



    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('job_id')
        parser.add_argument('start_date')
        parser.add_argument('end_date')
        parser.add_argument('period')
        parser.add_argument('m_category')
        parser.add_argument('targetsite_id')
        parser.add_argument('targetsite_label')
        parser.add_argument('targetsite_url')
        parser.add_argument('t_category')
        parser.add_argument('transformation_program_id')
        parser.add_argument('exchange_rate')
        parser.add_argument('tariff_rate')
        parser.add_argument('vat_rate')
        parser.add_argument('margin_rate')
        parser.add_argument('minimum_margin')
        parser.add_argument('tariff_threshold')
        parser.add_argument('delivery_company')
        parser.add_argument('shipping_cost')
        parser.add_argument('num_worker')
        parser.add_argument('num_thread')
        parser.add_argument('cid')
        parser.add_argument('cnum')
        parser.add_argument('start_date')
        parser.add_argument('end_date')
        parser.add_argument('period')
        parser.add_argument('check_c')
        parser.add_argument('check_m')
        parser.add_argument('check_t')
        parser.add_argument('tjcid')
        parser.add_argument('rtid')
        parser.add_argument('user_id')
        parser.add_argument('max_items')
        parser.add_argument('default_weight')
        parser.add_argument('checked')
        args = parser.parse_args() #saveJobProperties
        if args['req_type'] == 'save_job_properties':
            return self.save_job_properties(args['job_id'], args['start_date'], args['end_date'], args['period'], args['m_category'], args['num_worker'], args['num_thread']);
        elif args['req_type'] == 'load_job_properties':
            return self.load_job_properties(args['job_id'])
        elif args['req_type'] == 'load_job_working':
            return self.load_job_working(args['job_id'])
        elif args['req_type'] == 'set_job_working':
            return self.set_job_working(args['job_id'])
        elif args['req_type'] == 'make_airflow_script':
            return self.make_airflow_script(args['job_id'], args['start_date'], args['end_date'], args['period'],args['check_c'], args['check_m'], args['check_t'], args['user_id'])
        elif args['req_type'] == 'load_pricing_information':
            return self.load_pricing_information(args['job_id'],args['targetsite_id'],args['t_category'])
        elif args['req_type'] == 'load_tmp_pricing_information':
            return self.load_tmp_pricing_information(args['job_id'])
        elif args['req_type'] == 'get_registered_target_site':
            return self.get_registered_target_site(args['job_id'])
        elif args['req_type'] == 'register_target_site':
            return self.register_target_site(args['job_id'], args['targetsite_id'], args['targetsite_label'], args['targetsite_url'], args['t_category'], args['transformation_program_id'], args['exchange_rate'], args['tariff_rate'], args['vat_rate'],args['margin_rate'], args['minimum_margin'],args['tariff_threshold'], args['delivery_company'], args['cid'], args['cnum'], args['max_items'], args['default_weight'])
        elif args['req_type'] == 'update_target_site':
            return self.update_target_site(args['rtid'], args['targetsite_id'], args['targetsite_label'], args['targetsite_url'], args['t_category'], args['transformation_program_id'], args['exchange_rate'], args['tariff_rate'], args['vat_rate'],args['margin_rate'], args['minimum_margin'],args['tariff_threshold'], args['delivery_company'], args['cid'], args['cnum'], args['max_items'], args['default_weight'])
        elif args['req_type'] == 'deregister_target_site':
            return self.deregister_target_site(args['tjcid'])
        elif args['req_type'] == 'check_target_site':
            return self.check_target_site(args['tjcid'],args['checked'])
        elif args['req_type'] == 'get_registered_schedules':
            return self.get_registered_schedules(args['user_id'])
        elif args['req_type'] == 'set_activate_schedule':
            return self.set_activate_schedule(args['job_id'])
        elif args['req_type'] == 'set_deactivate_schedule':
            return self.set_deactivate_schedule(args['job_id'])
        elif args['req_type'] == 'delete_schedule':
            return self.delete_schedule(args['job_id'])
        elif args['req_type'] == 'check_false_all_target_site':
            return self.check_false_all_target_site(args['job_id'])
        elif args['req_type'] == 'check_true_all_target_site':
            return self.check_true_all_target_site(args['job_id'])
        return { "success": False }



app = Flask(__name__)
CORS(app)
api = Api(app)

api.add_resource(TaskManager, '/api/db/task')
api.add_resource(FailedJobsManager, '/api/db/failedjobs')
api.add_resource(DBSchemasManager, '/api/db/dbschemas')
api.add_resource(ProgramsManager, '/api/db/programs')
api.add_resource(ExecutionsManager, '/api/db/executions')
api.add_resource(UserProgramManager, '/api/db/userprogram')
api.add_resource(CategoryManager, '/api/db/category')
api.add_resource(ObjectManager, '/api/db/object')
api.add_resource(TransformManager, '/api/db/transform')
api.add_resource(AccountManager, '/api/db/account')
api.add_resource(ProjectManager, '/api/db/project')
#api.add_resource(UserProgramTempManager, '/api/db/userprogramtemp')

api.add_resource(GroupManager, '/api/db/group')
api.add_resource(JobManager, '/api/db/job')
api.add_resource(TargetSiteManager, '/api/db/targetsite')
api.add_resource(DeliveryManager, '/api/db/delivery')
api.add_resource(TVRateManager, '/api/db/tvrate')
api.add_resource(CategoryTreeManager, '/api/db/categorytree')
api.add_resource(MySiteCategoryTreeManager, '/api/db/mysitecategorytree')
api.add_resource(ShippingFeeManager, '/api/db/shippingfee')
api.add_resource(PricingInformationManager, '/api/db/pricinginformation')
api.add_resource(TransformationProgramManager, '/api/db/transformationprogram')
api.add_resource(GatewayConfigurationManager, '/api/db/gatewayconfiguration')
api.add_resource(ProductListManager, '/api/db/productlist')
api.add_resource(ProductOptionsManager, '/api/db/producoptions')
api.add_resource(ExchangeRateManager, '/api/db/exchangerate')
api.add_resource(MySiteManager, '/api/db/mysite')
api.add_resource(JobPropertiesManager, '/api/db/jobproperties')





if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
