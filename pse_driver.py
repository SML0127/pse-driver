import time
import copy
import os
import time
import psycopg2
import sys
import traceback
import argparse
import json
import re

from datetime import datetime, timedelta
from price_parser import Price
from yaml import load, Loader
from urllib.parse import urlparse
from functools import partial

from managers.redis_manager import *
from managers.settings_manager import *
from managers.log_manager import LogManager
from managers.graph_manager import GraphManager

from driver_components.dag_scheduler import *
from driver_components.task_scheduler import *

print_flushed = partial(print, flush=True)

class PseDriver():

    def __init__(self):
        pass

    def init(self):
        self.setting_manager = SettingsManager()
        self.setting_manager.setting(
            "/home/pse/pse-driver/settings-driver.yaml")
        settings = self.setting_manager.get_settings()
        self.graph_manager = GraphManager()
        self.graph_manager.init(settings)
        self.log_manager = LogManager()
        self.log_manager.init(settings)

    def close(self):
        self.graph_manager.disconnect()
        self.log_manager.close()

    def none_to_blank(self, str):
        if not str or str is None or str == None or str == "None":
            return ''
        else:
            return str

    def init_program(self, program):
        try:
            program['usr_msg'] = []
            program['err_msg'] = []
            program['lm'] = self.log_manager
            program['rm'] = RedisManager()
            program['rm'].connect(self.setting_manager.get_settings())
            url = program['ops'][0].get('url', '')
            netloc = urlparse(url).netloc
            # if 'amazon.com' in netloc:
            #    program['rm'].create_rq(program['rm'].get_connection(), 'amazon_us_queue')
            # elif 'amazon.co.uk' in netloc:
            #    program['rm'].create_rq(program['rm'].get_connection(), 'amazon_uk_queue')
            # elif 'amazon.co.de' in netloc:
            #    program['rm'].create_rq(program['rm'].get_connection(), 'amazon_de_queue')
            # else:
            #    program['rm'].create_rq(program['rm'].get_connection(), program['queue'])
            program['rm'].create_rq(
                program['rm'].get_connection(), program['queue'])
        except:
            raise

    def load_category_from_file(self, fname):
        return json.load(open(fname))

    def load_program_from_file(self, fname):
        return json.load(open(fname))

    def load_program_from_db(self, program_id):
        return self.log_manager.load_program(program_id)

    def load_category_from_db(self, category_id):
        return self.log_manager.load_category(category_id)

    def save_program_from_file_to_db(self, args):
        program = self.load_program_from_file(args.wf)
        if args.url != None:
            program['ops'][0]['url'] = args.url
        if args.max_page != None:
            program['ops'][1]['max_num_tasks'] = args.max_page
        program_id = self.log_manager.save_program(str(args.wfn), args.wf)
        print_flushed("Program {} is saved: id - {}".format(str(args.wfn), program_id))
        return program, program_id

    def save_category_from_file_to_db(self, args):
        category = self.load_category_from_file(args.ct)
        category_id = self.log_manager.save_category(str(args.ctn), args.ct)
        print_flushed("Category {} is saved: id - {}".format(str(args.ctn), category_id))
        return category, category_id

    def run(self, program, eid, job_id):
        try:
            self.init_program(program)
            program['lm'] = self.log_manager
            program['execution_id'] = eid
            program['job_id'] = job_id
            dag_scheduler = DagScheduler()
            dag_scheduler.run(program)
        except Exception as e:
            print_flushed("-------Raised Exception in DRIVER-------")
            print_flushed(e)
            print_flushed("---------------------------------------")
            print_flushed("--------------STACK TRACE--------------")
            print_flushed(str(traceback.format_exc()))
            print_flushed("---------------------------------------")
            raise e

    def rerun(self, program, previous_eid, eid):
        try:
            self.init_program(program)
            program['lm'] = self.log_manager
            program['execution_id'] = eid
            dag_scheduler = DagScheduler()
            dag_scheduler.rerun(program, previous_eid)
        except Exception as e:
            print_flushed("-------Raised Exception in DRIVER-------")
            print_flushed(e)
            print_flushed("---------------------------------------")
            print_flushed("--------------STACK TRACE--------------")
            print_flushed(str(traceback.format_exc()))
            print_flushed("---------------------------------------")
            raise e

    def register_execution(self, prog_name, program, category):
        program_id = self.log_manager.save_program(prog_name, program)

    def run_from_file(self, args):
        #category, cid = self.save_category_from_file_to_db(args)
        program, pid = self.save_program_from_file_to_db(args)
        job_id = -1
        if args.job_id is not None:
            job_id = args.job_id
        try:
            eid = self.log_manager.start_execution(pid, 0, job_id)
            self.run(program, eid, job_id)
        except Exception as e:
            self.log_manager.end_execution(
                eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def run_from_db(self, args):
        pid = args.wf
        program = self.load_program_from_db(pid)
        job_id = -1
        if args.job_id is not None:
            job_id = args.job_id
            eid = self.log_manager.start_execution(pid, 0, job_id)
        try:
            self.run(program, eid, job_id)
            self.log_manager.end_execution(eid, {"status": 1})
        except Exception as e:
            self.log_manager.end_execution(
                eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        return eid

    def run_execution(self, args):
        program = self.log_manager.load_program_of_execution(args.eid)
        try:
            self.run(program, args.eid)
        except Exception as e:
            self.log_manager.end_execution(
                args.eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(args.eid, {"status": 1})
        return args.eid

    def rerun_execution_from_db(self, args):
        program = self.load_program_from_db(args.wf)
        eid = self.log_manager.start_execution(
            args.wf, args.eid, args.ct, args.cno)
        try:
            self.rerun(program, args.eid, eid)
        except Exception as e:
            self.log_manager.end_execution(
                eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def rerun_execution_from_file(self, args):
        category, cid = self.save_category_from_file_to_db(args)
        program, pid = self.save_program_from_file_to_db(args)
        eid = self.log_manager.start_execution(pid, args.eid, cid, args.cno)
        try:
            self.rerun(program, args.eid, eid)
        except Exception as e:
            self.log_manager.end_execution(
                eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def test(self, args):
        try:
            print_flushed(args)
            for arg in args.groupbykey:
                print_flushed(arg)
        except:
            print_flushed(traceback.format_exc())
            raise
        return

    #def transform_to_mysite(self, args, sm_history_id):
    #    num = 0
    #    num_out_of_stock = 0
    #    try:
    #        exec_id, c_date = self.log_manager.get_lastest_execution_id_using_job_id(
    #            args.job_id)
    #        c_date = datetime.strptime(
    #            str(c_date), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    #        node_ids = self.graph_manager.find_nodes_of_execution(exec_id)
    #        print_flushed('Execution id: ', exec_id)
    #        print_flushed("(Transform to my site) # of items: ", len(node_ids))
    #        print_flushed('Groupby key: ', args.groupbykey)

    #        time_gap = timedelta(hours=9)
    #        cur_time = datetime.utcnow() + time_gap
    #        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #        self.graph_manager.re_log_to_job_current_mysite_working(
    #            '{}\n[Running] Upload 0 items to mysite'.format(cur_time), args.job_id)
    #        mpid = 0
    #        for node_id in node_ids:
    #            try:
    #                node_properties = self.graph_manager.get_node_properties(
    #                    node_id)
    #                node_properties['c_date'] = c_date
    #                mpid = node_properties['mpid']
    #                result = self.transform_node_property_for_mysite(
    #                    node_id, node_properties)
    #                if result is False:
    #                    num_out_of_stock = num_out_of_stock + 1
    #                    continue
    #                self.graph_manager.insert_node_property_to_mysite(
    #                    args.job_id, result, args.groupbykey)
    #                num = num + 1
    #                if num % 50 == 0:
    #                    cur_time = datetime.utcnow() + time_gap
    #                    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #                    self.graph_manager.log_to_job_current_mysite_working(
    #                        '\n{}\n[Running] Upload {} items to mysite'.format(cur_time, num), args.job_id)
    #                    print_flushed("Current # of inserted items to mysite : ", num)
    #            except:
    #                err_msg = '================================ Operator ============================== \n'
    #                err_msg += ' Transform data from source to my site \n\n'
    #                err_msg += '================================ STACK TRACE ============================== \n' + \
    #                    str(traceback.format_exc())
    #                self.graph_manager.log_err_msg_of_my_site(
    #                    sm_history_id, mpid, err_msg)
    #        cur_time = datetime.utcnow() + time_gap
    #        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')

    #    except:
    #        err_msg = '================================ Operator ============================== \n'
    #        err_msg += ' Logging in transforming data from source to my site \n\n'
    #        err_msg += '================================ STACK TRACE ============================== \n' + \
    #            str(traceback.format_exc())
    #        self.graph_manager.log_err_msg_of_my_site(
    #            sm_history_id, -1, err_msg)

    #    try:
    #        print_flushed(
    #            '====================== Delete duplicated items in mysite ====================')
    #        duplicate_num, d0, d1, d2, d3 = self.graph_manager.set_status_for_duplicated_data(
    #            args.job_id)
    #        cur_time = datetime.utcnow() + time_gap
    #        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #        self.graph_manager.log_to_job_current_mysite_working(
    #            '\n{}\n[Finished] Upload {} items to mysite, Duplicate {} items, Out of stock {} itmes'.format(cur_time, num, duplicate_num, num_out_of_stock), args.job_id)
    #        self.graph_manager.log_to_job_current_mysite_working(
    #            '\nDuplicate {} items in up-to-date. \nDuplicate {} items in updated.\nDuplicate {} items in new.\nDuplicate {} items in deleted'.format(d0, d1, d2, d3), args.job_id)
    #        print_flushed("All # of inserted items to mysite : ", num)
    #        print_flushed('End')
    #    except:
    #        err_msg = '================================ Operator ============================== \n'
    #        err_msg += ' Delete duplicated product item \n\n'
    #        err_msg += '================================ STACK TRACE ============================== \n' + \
    #            str(traceback.format_exc())
    #        self.graph_manager.log_err_msg_of_my_site(
    #            sm_history_id, -2, err_msg)
    #    return


    def update_to_mysite(self, args):
        sm_history_id = -1
        err_op = 'Logging before update my site'
        try:
            # Calculate update start date
            utcnow = datetime.utcnow()
            time_gap = timedelta(hours=9)
            kor_time = utcnow + time_gap
            start_date = datetime.strptime(
                str(kor_time), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")

            # Get lastest exeuction id and crawling date
            exec_id, c_date = self.log_manager.get_lastest_execution_id_using_job_id(
                args.job_id)
            c_date = datetime.strptime(
                str(c_date), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
            
            # Logging update start date
            history_id = self.graph_manager.insert_sm_history(
                exec_id, start_date, args.job_id)
            sm_history_id = history_id
            self.graph_manager.update_last_sm_date_in_job_configuration(
                start_date, args.job_id)
            print_flushed('Execution id: ', exec_id)

            # Get duplicate check keys of job
            groupby_keys_array = self.graph_manager.get_groupby_key(args.job_id)
            groupby_keys = [] 
            for idx in groupby_keys_array:
                groupby_keys.append(str(idx))
            args.groupbykey = groupby_keys

            # Get crawled item from jobs
            node_ids = self.graph_manager.find_nodes_of_execution(exec_id)
            print_flushed('Groupby key: ', args.groupbykey)
            print_flushed(
                '====================== Transform to mysite schema ======================')
            print_flushed('============  Delete deleted items of job id = {}  ============='.format(args.job_id))

            # Delete deleted item from my site
            err_op = 'Delete deleted product in my site'
            self.graph_manager.delete_deleted_product_in_mysite(args.job_id)
            cur_time = datetime.utcnow() + time_gap
            cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
            self.graph_manager.re_log_to_job_current_mysite_working(
                '\n{}\n[Running]  Update mysite'.format(cur_time), args.job_id)

            mpid = 0
            num = 0
            log_mpid = ''
            log_url = ''
            num_out_of_stock = 0
            print_flushed("# of items: ", len(node_ids))

            for node_id in node_ids:
                try:
                    node_properties = self.graph_manager.get_node_properties(node_id)
                    node_properties['c_date'] = c_date
                    log_url = node_properties.get('url', '')
                    mpid = node_properties['mpid']
                    log_mpid = mpid
                
                    # Transform data schema for mysite
                    result = self.transform_node_property_for_mysite(
                        node_id, node_properties)
                    if result is False:
                        num_out_of_stock = num_out_of_stock + 1
                        continue
                    num = num + 1
                    if num % 50 == 0:
                        cur_time = datetime.utcnow() + time_gap
                        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
                        self.graph_manager.log_to_job_current_mysite_working(
                            '\n{}\n[Running] Insert and Update {} items to mysite'.format(cur_time, num), args.job_id)
                        print_flushed("Current # of inserted items to mysite : ", num)
                    
                    # Insert and Update   
                    self.graph_manager.insert_node_property_to_mysite(
                        args.job_id, mpid, result, args.groupbykey, sm_history_id)
              

                except:
                    err_msg = '================================ Operator ================================ \n'
                    err_msg += 'Update my site' + '\n\n'
                    err_msg += '================================ My site product id ================================ \n'
                    err_msg += 'My site product id: ' + \
                        str(log_mpid) + '\n\n'
                    err_msg += '================================ Source site URL ================================ \n'
                    err_msg += 'URL: ' + log_url + '\n\n'
                    err_msg += '================================ STACK TRACE ============================== \n' + \
                        str(traceback.format_exc())
                    print(str(traceback.format_exc()))
                    self.graph_manager.log_err_msg_of_my_site(
                        sm_history_id, mpid, err_msg)

            print_flushed("# of inserted items to mysite : ", num)
            self.graph_manager.log_to_job_current_mysite_working(
                '\n{}\n[Finished] Insert and Update {} items to mysite'.format(cur_time, num), args.job_id)
            if num_out_of_stock != 0:
                self.graph_manager.log_to_job_current_mysite_working(
                    '\n{}\n[Finished] Do not insert {} items to mysite because of out of stock (before update)'.format(cur_time, num_out_of_stock), args.job_id)
            print_flushed(
                '====================== Set status of deleted items in mysite ====================')
            num_deleted = self.graph_manager.set_status_for_deleted_in_mysite(args.job_id, c_date)
            print_flushed("# of delete items in mysite: ", num_deleted)
            err_op = 'Delete duplicated item'
            print_flushed(
                '====================== Delete duplicated items in mysite ====================')
            num_duplicated = self.graph_manager.set_status_for_duplicated_data(
                args.job_id)
            print_flushed("# of duplicated items in mysite: ", num_duplicated)
            self.graph_manager.log_to_job_current_mysite_working(
                '\nDuplicated {} items.(Do not show duplicated items)'.format(num_duplicated), args.job_id)
            print_flushed(
                '====================== Finish mysite update / upload ====================')
            

        except:
            err_msg = '================================ Operator ================================ \n'
            err_msg += ' ' + err_op  + '\n\n'
            err_msg += '================================ STACK TRACE ============================== \n' + \
                str(traceback.format_exc())
            self.graph_manager.log_err_msg_of_my_site(
                sm_history_id, -1, err_msg)

            print_flushed(traceback.format_exc())
            raise
        utcnow = datetime.utcnow()
        time_gap = timedelta(hours=9)
        kor_time = utcnow + time_gap
        end_date = datetime.strptime(
            str(kor_time), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
        self.graph_manager.update_sm_history(end_date, sm_history_id)
        return

    #def update_to_mysite_old(self, args):
    #    sm_history_id = -1
    #    err_op = 'Logging update my site'
    #    try:
    #        # Calculate update start date
    #        utcnow = datetime.utcnow()
    #        time_gap = timedelta(hours=9)
    #        kor_time = utcnow + time_gap
    #        start_date = datetime.strptime(
    #            str(kor_time), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")

    #        # Get lastest exeuction id and crawling date
    #        exec_id, c_date = self.log_manager.get_lastest_execution_id_using_job_id(
    #            args.job_id)
    #        c_date = datetime.strptime(
    #            str(c_date), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    #        
    #        # Logging update start date
    #        history_id = self.graph_manager.insert_sm_history(
    #            exec_id, start_date, args.job_id)
    #        sm_history_id = history_id
    #        is_update = self.log_manager.check_existing_source_view_using_job_id(
    #            args.job_id)
    #        print_flushed("Is update ? ", is_update)

    #        # Get duplicate check keys of job
    #        groupby_keys_array = self.graph_manager.get_groupby_key(args.job_id)
    #        groupby_keys = [] 
    #        for idx in groupby_keys_array:
    #            groupby_keys.append(str(idx))
    #        args.groupbykey = groupby_keys


    #        self.graph_manager.update_last_sm_date_in_job_configuration(
    #            start_date, args.job_id)
    #        if is_update == True:  # Update
    #            print_flushed('Execution id: ', exec_id)
    #            node_ids = self.graph_manager.find_nodes_of_execution(exec_id)
    #          
    #            print_flushed('Groupby key: ', groupby_keys)
    #            print_flushed(
    #                '====================== Transform to mysite schema ======================')
    #            cur_time = datetime.utcnow() + time_gap
    #            cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #            print_flushed('============  Delete deleted items of job id = {}  ============='.format(
    #                args.job_id))
    #            self.graph_manager.delete_stock_zero_in_mysite(args.job_id)
    #            self.graph_manager.re_log_to_job_current_mysite_working(
    #                '\n{}\n[Running]  Update mysite'.format(cur_time), args.job_id)
    #            mpid = 0
    #            num = 0
    #            log_mpid = ''
    #            log_url = ''
    #            num_out_of_stock = 0
    #            print_flushed("# of items: ", len(node_ids))

    #            for node_id in node_ids:
    #                try:
    #                    node_properties = self.graph_manager.get_node_properties(
    #                        node_id)
    #                    node_properties['c_date'] = c_date
    #                    log_url = node_properties.get('url', '')
    #                    mpid = node_properties['mpid']
    #                    log_mpid = mpid

    #                    # Transform data schema for mysite
    #                    result = self.transform_node_property_for_mysite(
    #                        node_id, node_properties)
    #                    if result is False:
    #                        num_out_of_stock = num_out_of_stock + 1
    #                        continue
    #                    num = num + 1
    #                    if num % 50 == 0:
    #                        cur_time = datetime.utcnow() + time_gap
    #                        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #                        self.graph_manager.log_to_job_current_mysite_working(
    #                            '\n{}\n[Running] Insert {} items to mysite (before update)'.format(cur_time, num), args.job_id)
    #                        print_flushed("Current # of inserted items to mysite : ", num)

    #                    # Insert and Update   
    #                    self.graph_manager.insert_node_property_to_tmp_mysite(
    #                        args.job_id, result, args.groupbykey)
    #                except:
    #                    err_msg = '================================ Operator ================================ \n'
    #                    err_msg += 'Update my site' + '\n\n'
    #                    err_msg += '================================ My site product id ================================ \n'
    #                    err_msg += 'My site product id: ' + \
    #                        str(log_mpid) + '\n\n'
    #                    err_msg += '================================ Source site URL ================================ \n'
    #                    err_msg += 'URL: ' + log_url + '\n\n'
    #                    err_msg += '================================ STACK TRACE ============================== \n' + \
    #                        str(traceback.format_exc())
    #                    self.graph_manager.log_err_msg_of_my_site(
    #                        sm_history_id, mpid, err_msg)
    #            # update
    #            self.graph_manager.log_to_job_current_mysite_working(
    #                '\n{}\n[Running] Insert {} items to mysite (before update)'.format(cur_time, num), args.job_id)
    #            if num_out_of_stock != 0:
    #                self.graph_manager.log_to_job_current_mysite_working(
    #                    '\n{}\n[Running] Do not insert {} items to mysite because of out of stock (before update)'.format(cur_time, num_out_of_stock), args.job_id)
    #            print_flushed("Current # of inserted items to mysite : ", num)
    #            print_flushed('====================== Update mysite ====================')
    #            self.graph_manager.update_mysite(args.job_id, sm_history_id)
    #            print_flushed(
    #                '====================== Delete duplicated items in mysite ====================')
    #            err_op = 'Delete duplicated product item'
    #            duplicate_num, d0, d1, d2, d3 = self.graph_manager.set_status_for_duplicated_data(
    #                args.job_id)
    #            self.graph_manager.log_to_job_current_mysite_working(
    #                '\nDuplicated {} items.\nDuplicate {} items in up-to-date. \nDuplicate {} items in new.\nDuplicate {} items in updated.\nDuplicate {} items in deleted'.format(duplicate_num, d0, d1, d2, d3), args.job_id)
    #        elif is_update == False:  # Upload
    #            print_flushed('============  Delete deleted items of job id = {}  ============='.format(
    #                args.job_id))
    #            err_op = 'Delete stock zero product item'
    #            self.graph_manager.delete_stock_zero_in_mysite(args.job_id)
    #            self.transform_to_mysite(args, sm_history_id)

    #    except:
    #        err_msg = '================================ Operator ================================ \n'
    #        err_msg += ' ' + err_op  + '\n\n'
    #        err_msg += '================================ STACK TRACE ============================== \n' + \
    #            str(traceback.format_exc())
    #        self.graph_manager.log_err_msg_of_my_site(
    #            sm_history_id, -1, err_msg)

    #        print_flushed(traceback.format_exc())
    #        raise
    #    utcnow = datetime.utcnow()
    #    time_gap = timedelta(hours=9)
    #    kor_time = utcnow + time_gap
    #    end_date = datetime.strptime(
    #        str(kor_time), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    #    self.graph_manager.update_sm_history(end_date, sm_history_id)
    #    return

    def transform_node_property_for_mysite(self, node_id, node_properties):
        try:
            utcnow = datetime.utcnow()
            time_gap = timedelta(hours=9)
            kor_time = utcnow + time_gap
            cleaner_dquotation = re.compile('\"')
            cleaner_quotation = re.compile('\'')
            result = {}
            result_for_source_view = {}
            result_for_source_view['my_product_id'] = node_properties['mpid']
            # 0 = up-to=date, 1 = updated, 2 = new, 3 = deleted, 4 = duplicated
            result_for_source_view['status'] = 2  # new
            result_for_source_view['c_date'] = node_properties['c_date']
            result_for_source_view['sm_date'] = datetime.strptime(
                str(kor_time), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
            result_for_source_view['url'] = node_properties.get('url', '')
            result_for_source_view['brand'] = node_properties.get('brand', '')
            if node_properties.get('name', None) is None:
                print_flushed("There is no crawled product name. URL, mpid :",
                      node_properties['url'], node_properties['mpid'])
                return False
            name = re.sub(cleaner_dquotation, '', node_properties['name'])
            name = re.sub(cleaner_quotation, '', name)
            result_for_source_view['name'] = name
            if node_properties.get('price','') == '' or node_properties.get('price','') is None:
                print_flushed("Price is null :", node_properties['url'])
                return False
            result_for_source_view['price'] = node_properties.get('price','').encode('UTF-8','strict').hex()
            if abs(Price.fromstring(node_properties.get('price','')).amount_float - 0) < 0.00001:
                print_flushed("Price is zero :", node_properties['url'])
                return False
            #result_for_source_view['price_currency'] = Price.fromstring(node_properties.get('price','')).currency
            #result_for_source_view['price_amount'] = Price.fromstring(node_properties.get('price','')).amount_float
            result_for_source_view['stock'] = node_properties.get('stock', '999')
            if self.graph_manager.check_string_is_int(result_for_source_view['stock']) == True:
                if int(result_for_source_view['stock']) == 0:
                    print_flushed("Out of stock :", node_properties['url'])
                    return False
            elif (result_for_source_view['stock'] in [None, '']) or ('in stock' not in str(result_for_source_view['stock']).lower()):
                print_flushed("Out of stock :", node_properties['url'])
                return False
            if 'in stock' in str(result_for_source_view['stock']).lower():
                result_for_source_view['stock'] = 999
            result_for_source_view['shipping_price'] = self.none_to_blank(str(node_properties.get('shipping_price',''))).encode('UTF-8','strict').hex()
            result_for_source_view['weight'] = node_properties.get('weight','')
            result_for_source_view['shipping_weight'] = node_properties.get('shipping_weight','')
            result_for_source_view['source_site_product_id'] = node_properties.get('source_site_product_id','')
            desc = re.sub(cleaner_dquotation, '', self.none_to_blank(str(node_properties.get('description', "")))  )
            desc = re.sub(cleaner_quotation, '', desc)
            result_for_source_view['description'] = desc 
            desc_ren = re.sub(cleaner_dquotation, '', self.none_to_blank(str(node_properties.get('description_rendered', ""))))
            desc_ren = re.sub(cleaner_quotation, '', desc_ren)
            result_for_source_view['description_rendered'] = desc_ren 

            desc1 = re.sub(cleaner_dquotation, '',self.none_to_blank(str(node_properties.get('description1', ""))))
            desc1 = re.sub(cleaner_quotation, '', desc1)
            result_for_source_view['description1'] = desc1 
            desc1_ren = re.sub(cleaner_dquotation, '',self.none_to_blank(str(node_properties.get('description1_rendered', ""))))
            desc1_ren = re.sub(cleaner_quotation, '', desc1_ren)
            result_for_source_view['description1_rendered'] = desc1_ren 

            desc2 = re.sub(cleaner_dquotation, '',self.none_to_blank(str(node_properties.get('description2', ""))))
            desc2 = re.sub(cleaner_quotation, '', desc2)
            result_for_source_view['description2'] = desc2 
            desc2_ren = re.sub(cleaner_dquotation, '',self.none_to_blank(str(node_properties.get('description2_rendered', ""))))
            desc2_ren = re.sub(cleaner_quotation, '', desc2_ren)
            result_for_source_view['description2_rendered'] = desc2_ren 
            # "null" -> return None
            if 'jomashop' in result_for_source_view['url'] and (result_for_source_view['stock'] == 'null' or result_for_source_view['stock'] is None):
                print_flushed("Out of stock :", node_properties['url'])
                return False
            elif result_for_source_view['stock'] == 'null' or result_for_source_view['stock'] is None:
                result_for_source_view['stock'] = '999'
            # if isinstance(node_properties['images'], str) == True:
            #   node_properties['images'] = [node_properties['images']]
            if len(node_properties.get('images', [])) == 0:
                print_flushed("There is no crawled image. URL :",
                      node_properties['url'])
                # return False
                #node_properties['images'] = [node_properties['front_image']]
                #result_for_source_view['image_url'] = node_properties['images'][0]
            else:
                result_for_source_view['image_url'] = node_properties['images'][0]
            result_for_source_view['num_images'] = len(
                node_properties.get('images', []))

            result_for_thumbnail = {}
            result_for_thumbnail['my_product_id'] = node_properties['mpid']
            result_for_thumbnail['image_urls'] = node_properties.get(
                'images', [])

            result_for_option = {}
            result_for_option['my_product_id'] = node_properties['mpid']
            result_for_option['option_names'] = []
            result_for_option['option_values'] = {}
            result_for_option['list_price'] = node_properties['price'].encode('UTF-8','strict').hex()
            result_for_option['price'] = node_properties['price'].encode('UTF-8','strict').hex()
            result_for_option['stock'] = node_properties.get('stock', None)
            if result_for_option['stock'] == 'null' or result_for_option['stock'] is None:
                result_for_option['stock'] = '999'
            result_for_option['stock_status'] = 1
            result_for_option['msg'] = None

            result_for_desc = {}

            num_option = 0
            mysite_columns = self.graph_manager.get_mysite_column_name()
            for key in sorted(node_properties.keys()):
                if key not in mysite_columns and key not in ['option_list', 'option_matrix', 'images']:
                    result_for_desc[key] = node_properties.get(key, None)
                elif key in ['option_list', 'option_matrix']:
                    if node_properties.get(key, None) != {}:
                        if key == 'option_list':
                            option_list = node_properties.get(key, None)
                            for option_name in option_list.keys():
                                num_option = num_option + 1
                                result_for_option['option_names'].append(option_name)
                                result_for_option['option_values'][option_name] = option_list[option_name]
                        elif key == 'option_matrix':
                            option_matrix = node_properties.get(key, None)
                            for option_name in option_matrix.keys():  # first one = row, second one = col
                                if option_name != 'option_maxtrix_value':
                                    num_option = num_option + 1
                                    result_for_option['option_names'].append(option_name)
                                    result_for_option['option_values'][option_name] = option_matrix[option_name]
                            result_for_option['option_names'].append(
                                'option_maxtrix_value')
                            result_for_option['option_values']['option_maxtrix_value'] = option_matrix['option_maxtrix_value']
            result_for_source_view['num_options'] = num_option

            result['result_for_source_view'] = result_for_source_view
            result['result_for_thumbnail'] = result_for_thumbnail
            result['result_for_option'] = result_for_option
            result['result_for_desc'] = result_for_desc
        except:
            print_flushed(node_properties['url'], node_properties['mpid'])
            raise
            return False
        return result

    def execute(self):
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
            parser.add_argument('--max_page', required=False)
            parser.add_argument('--job_id', required=False)
            parser.add_argument('--groupbykey', nargs='+', required=False)
            args, unknown = parser.parse_known_args()
            print_flushed(args)
            if args.c == 'run_execution':
                return self.run_execution(args)
            elif args.c == 'rerun_execution_from_db':
                return self.rerun_execution_from_db(args)
            elif args.c == 'rerun_execution_from_file':
                return self.rerun_execution_from_file(args)
            elif args.c == 'run_from_file':
                return self.run_from_file(args)
            elif args.c == 'run_from_db':
                return self.run_from_db(args)
            elif args.c == 'save_workflow':
                return self.save_program_from_file_to_db(args)
            elif args.c == 'save_category':
                return self.save_category_from_file_to_db(args)
            elif args.c == 'update_to_mysite':
                return self.update_to_mysite(args)
        except Exception as e:
            print_flushed(str(traceback.format_exc()))
            raise e


if __name__ == "__main__":
    driver = PseDriver()
    driver.init()
    try:
        driver.execute()
    except Exception as e:
        print_flushed(str(traceback.format_exc()))
        pass
    driver.close()
