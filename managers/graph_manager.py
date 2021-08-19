import psycopg2
import traceback
import json
import hashlib
import urllib.request
import cfscrape
import pathlib
import sys
import string
import time
from io import BytesIO
from PIL import Image
import requests
import re
from price_parser import Price
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
from functools import partial
from googletrans import Translator
print_flushed = partial(print, flush=True)

def is_hex_str(s):
    if s is None:
        return False
    else:
        return set(s).issubset(string.hexdigits)


class GraphManager():

    def __init__(self):
        self.gp_conn = None
        self.gp_cur = None
        self.pg_conn = None
        self.pg_cur = None

    def init(self, settings):
        try:
            self.conn_info = settings['graph_db_conn_info']
            self.pg_conn = psycopg2.connect(self.conn_info)
            self.gp_conn = psycopg2.connect(self.conn_info)
            self.pg_conn.autocommit = True
            self.gp_conn.autocommit = True
            self.pg_cur = self.pg_conn.cursor()
            self.gp_cur = self.pg_cur
        except Exception as e:
            print_flushed(str(traceback.format_exc()))
            raise e

    def connect(self, pg_info, gp_info=''):
        try:
            self.pg_conn = psycopg2.connect(pg_info)
            self.pg_cur = self.pg_conn.cursor()
            if gp_info != '' and pg_info != gp_info:
                self.gp_conn = psycopg2.connect(gp_info)
                self.gp_cur = self.gp_conn.cursor()
            else:
                self.gp_conn = self.pg_conn
                self.gp_cur = self.pg_cur
        except Exception as e:
            print_flushed(str(traceback.format_exc()))
            raise e

    def disconnect(self):
        self.gp_conn.commit()
        self.pg_conn.commit()
        self.gp_conn.close()
        self.pg_conn.close()
        pass

    def create_db(self):
        try:
            query = 'create table node (id bigserial primary key, parent_id bigint, task_id bigint, label integer);'
            self.pg_cur.execute(query)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e
        try:
            query = 'create table node_property(id bigserial primary key, node_id bigint, key varchar(1048), value json);'
            self.gp_cur.execute(query)
            self.gp_conn.commit()
        except Exception as e:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def drop_db(self):
        try:
            query = 'drop table if exists node;'
            self.pg_cur.execute(query)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e
        try:
            query = 'drop table if exists node_property;'
            self.gp_cur.execute(query)
            self.gp_conn.commit()
        except Exception as e:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def create_node(self, task_id, parent_id, label):
        try:
            query = 'insert into node(task_id, parent_id, label) '
            query += 'values(%s, %s, %s)'
            query += 'returning id;'
            self.pg_cur.execute(
                query, (str(task_id), str(parent_id), str(label)))
            result = self.pg_cur.fetchone()[0]
            self.pg_conn.commit()
            return result
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def find_nodes_of_execution(self, exec_id):
        try:
            #query = 'select n.id from node n, stage s, task t, (select max(n1.label) as max_label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = '+str(
            #    exec_id)+') as l where t.status = 1 and n.task_id = t.id and t.stage_id = s.id and s.execution_id = '+str(exec_id)+' and n.label = l.max_label;'
            query = 'select id from node where task_id in (select id from task where stage_id in (select max(id) from stage where execution_id = {}))'.format(exec_id);
            self.pg_cur.execute(query)
            result = self.pg_cur.fetchall()
            self.pg_conn.commit()
            return list(map(lambda x: x[0], result))
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def set_status_for_duplicated_data(self, job_id):
        try:
            query = 'select count(*) from job_source_view where id in (select id from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()[0]
            self.gp_conn.commit()

            query = 'update job_source_view set status = 4 where id in (select id from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()


            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    def set_status_for_deleted_in_mysite(self, job_id, c_date):
        try:
            query = "update job_source_view set status = 3, sm_date = now() where c_date != '{}' and mpid in (select mpid from job_id_and_mpid where job_id = {}); select count(*) from job_source_view where status = 3 and mpid in (select mpid from job_id_and_mpid where job_id = {})".format(c_date, job_id, job_id)
            self.gp_cur.execute(query)
            num_set_deleted = self.gp_cur.fetchone()[0]
            self.gp_conn.commit()
            return num_set_deleted
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise



    def delete_deleted_product_in_mysite(self, job_id):
        try:
            query = "select mpid from job_source_view where status = 3 and mpid not in (select mpid from tpid_mapping) and mpid in (select mpid from job_id_and_mpid where job_id = {}) ".format(job_id)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            deleted_mpids = [int(row[0]) for row in rows]
            print_flushed("# of deleted (except uploaded items): ",len(deleted_mpids))

            if len(deleted_mpids) != 0:
                mpids_str = '('
                for mpid in deleted_mpids:
                    mpids_str += str(mpid) + ', '
                mpids_str = mpids_str[0:-2] + ')'

                query_thumbnail = "delete from job_thumbnail_source_view where mpid in " + mpids_str
                query_option = "delete from job_option_source_view where mpid in " + mpids_str
                query_desc = "delete from job_description_source_view where mpid in " + mpids_str
                query_origin = "delete from job_source_view where mpid in " + mpids_str
                
                query_job_id_and_mpid = "delete from job_id_and_mpid where mpid in " + mpids_str

                query = "BEGIN; " + query_origin + ';' + query_desc + ';' + query_thumbnail + '; '+ query_option + ';' + query_job_id_and_mpid + ';COMMIT;'
                self.gp_cur.execute(query)
                self.gp_conn.commit()

                return
        except:
            self.gp_conn.rollback()
            raise


    def insert_product_to_mysite(self, job_id, value, groupbykeys):
        try:
            #start = time.time()
            query = 'INSERT INTO job_source_view (mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options, num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered, shipping_price1, description_sha256)'
            query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); COMMIT'

            mpid = self.none_to_blank(
                str(value['result_for_source_view']['my_product_id']))
            status = self.none_to_blank(
                str(value['result_for_source_view']['status']))
            c_date = self.none_to_blank(
                str(value['result_for_source_view']['c_date']))
            sm_date = self.none_to_blank(
                str(value['result_for_source_view']['sm_date']))
            url = self.none_to_blank(
                str(value['result_for_source_view']['url']))
            url_sha256 = str(hashlib.sha256(
                url.encode('UTF-8', 'strict')).hexdigest())
            name_origin = self.none_to_blank(
                str(value['result_for_source_view']['name']))
            name = str(name_origin.encode('UTF-8', 'strict').hex())
            name_sha256 = str(hashlib.sha256(
                name_origin.encode('UTF-8', 'strict')).hexdigest())
            price = self.none_to_blank(
                str(value['result_for_source_view']['price']))
            if value['result_for_source_view']['stock'] == 'null' or value['result_for_source_view']['stock'] is None:
                print_flushed(
                    "Out ot stock. mpid = {}, url = {}".format(mpid, url))
                return False
            stock = self.check_stock(value['result_for_source_view'])
            if stock == '0':
                print_flushed(
                    "Out ot stock. mpid = {}, url = {}".format(mpid, url))
                return False
            image_url = self.none_to_blank(
                str(value['result_for_source_view'].get('image_url', '')))
            num_options = self.none_to_blank(
                str(value['result_for_source_view']['num_options']))
            num_images = self.none_to_blank(
                str(value['result_for_source_view']['num_images']))
            source_site_product_id = self.none_to_blank(str(value['result_for_source_view']['source_site_product_id']))
            brand = self.none_to_blank(str(value['result_for_source_view'].get('brand', '')))
            weight = self.none_to_blank(str(value['result_for_source_view'].get('weight', '')))
            shipping_weight = self.none_to_blank(str(value['result_for_source_view'].get('shipping_weight', '')))
            shipping_price = self.none_to_blank(str(value['result_for_source_view'].get('shipping_price', '')))
            shipping_price1 = self.none_to_blank(
                str(value['result_for_source_view'].get('shipping_price1', '')))
            description = self.none_to_blank(
                str(value['result_for_source_view'].get('description', ''))).encode('UTF-8').hex()

            description_sha256 = str(hashlib.sha256(self.none_to_blank(str(value['result_for_source_view'].get('description', ''))).encode()).hexdigest())
                
            description1 = self.none_to_blank(
                str(value['result_for_source_view'].get('description1', ''))).encode('UTF-8').hex()
            description2 = self.none_to_blank(
                str(value['result_for_source_view'].get('description2', ''))).encode('UTF-8').hex()
            description_rendered = self.none_to_blank(
                str(value['result_for_source_view'].get('description_rendered', ''))).encode('UTF-8').hex()
            description1_rendered = self.none_to_blank(
                str(value['result_for_source_view'].get('description1_rendered', ''))).encode('UTF-8').hex()
            description2_rendered = self.none_to_blank(
                str(value['result_for_source_view'].get('description2_rendered', ''))).encode('UTF-8').hex()


            groupbykeys = sorted(groupbykeys)
            groupby_key = ''
            mysite_columns = self.get_mysite_column_name()
            for key in groupbykeys:
                if key not in mysite_columns and key not in ['option_list', 'option_matrix', 'images']:
                    groupby_key += value['result_for_desc'].get(key, '')
                else:
                    groupby_key += self.none_to_blank(
                        str(value['result_for_source_view'][key]))
            groupby_key = str(groupby_key)
            groupby_key_sha256 = str(hashlib.sha256(
                groupby_key.encode()).hexdigest())
            self.gp_cur.execute(query, (mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options, num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered, shipping_price1, description_sha256))
            self.gp_conn.commit()
            #print('I1: ', time.time() - start)

            #start = time.time()
            query = "select id, key_name from mysite_translate_key where job_id = {}".format(job_id);
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchall()
            option_translate = False
            if result is not None:
                translate_keys = []
                for idx, val in enumerate(result):
                   for idx2, val2 in enumerate(val):
                      if idx2 == 1:
                         if is_hex_str(val2) == False:
                            translate_keys.append(val2)
                         else:
                            translate_keys.append(bytes.fromhex(val2).decode())
                if len(translate_keys) != 0:
                    translator = Translator()
                    if val != 'option':
                        for idx, val in translate_keys:
                            translated_text = ''
                            try:
                                translated_text = translator.translate(value['result_for_source_view'][val], src='ja', dest="ko").text
                            except:
                                pass
                            if translated_text != '':
                                query = 'update job_soure_view set {}_translated = {} where mpid = {}; COMMIT;'.format(val, translated_text, mpid)
                                self.gp_cur.execute(query)
                                self.gp_conn.commit()
                    else:
                        option_translate = True
            #print('I2: ', time.time() - start)

            #start = time.time()
            # insert data to description_source_view
            # (job_id integer, mpid integer, key varchar(2048), value text)
            query = 'INSERT INTO job_description_source_view (mpid, key, value) '
            query += 'VALUES (%s, %s, %s); COMMIT;'
            #mpid = int(value['result_for_desc']['my_product_id'])
            for key in value['result_for_desc'].keys():
                if value['result_for_desc'][key] is dict:
                    desc_value = str(json.dumps(value['result_for_desc'][key]))
                    desc_value = desc_value.encode('UTF-8').hex()
                    self.gp_cur.execute(query, (mpid, key, desc_value))
                elif value['result_for_desc'][key] is list:
                    desc_value = str(repr(value['result_for_desc'][key]))
                    desc_value = desc_value.encode('UTF-8').hex()
                    self.gp_cur.execute(query, (mpid, key, desc_value))
                else:
                    desc_value = str(value['result_for_desc'][key])
                    desc_value = desc_value.encode('UTF-8').hex()
                    self.gp_cur.execute(query, (mpid, key, desc_value))
            self.gp_conn.commit()
            #print('I3: ', time.time() - start)

            #start = time.time()
            # insert data to option_source_view
            query = 'INSERT INTO job_option_source_view (mpid, option_name, option_value, list_price, price, stock, stock_status, msg, option_name_translated, option_value_translated) '
            query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s); COMMIT;'
            mpid = str(value['result_for_option']['my_product_id'])
            for option_name in value['result_for_option']['option_names']:
                option_name_translated = ''
                if option_translate == True:
                    try:
                        option_name_translated = translator.translate(option_name, src='ja', dest="ko").text
                    except:
                        pass
                for option_val in value['result_for_option']['option_values'][option_name]:
                    list_price = self.none_to_blank(
                        str(value['result_for_option']['list_price']))
                    price = self.none_to_blank(
                        str(value['result_for_option']['price']))
                    stock = self.check_stock(value['result_for_option'])
                    stock_status = self.none_to_blank(
                        str(value['result_for_option']['stock_status']))
                    msg = self.none_to_blank(
                        str(value['result_for_option']['msg']))
                    option_val_translated = ''
                    if option_translate == True:
                        try:
                            option_val_translated = translator.translate(option_val, src='ja', dest="ko").text
                        except:
                            pass
                    #self.gp_cur.execute(query, (job_id, mpid, str((option_name.encode().hexdigest())), str((option_val.encode('UTF-8').hexdigest())), list_price, price, stock, stock_status, msg))
                    self.gp_cur.execute(query, (mpid, option_name.encode().hex(), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg, option_name_translated.encode('UTF-8').hex(), option_val_translated.encode('UTF-8').hex()))
            self.gp_conn.commit()

            # insert data to thumbnail_source_view
            # (mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))
            query = 'INSERT INTO job_thumbnail_source_view (mpid, image_url, image_url_sha256)'
            query += 'VALUES (%s, %s, %s); COMMIT'
            mpid = str(value['result_for_option']['my_product_id'])
            for img_url in value['result_for_thumbnail']['image_urls']:
                self.gp_cur.execute(query, (mpid, str(img_url), str(hashlib.sha256(img_url.encode()).hexdigest())))
            self.gp_conn.commit()
            #print('I4: ', time.time() - start)

        except:
           self.gp_conn.rollback()
           print_flushed('?????????????????')
           print_flushed(str(traceback.format_exc()))
           raise       
        return True

    def insert_node_property_to_mysite(self, job_id, mpid, new_product, groupbykeys, sm_history_id):
        try:
            # insert data to source_view
            job_id = str(job_id)

            #start = time.time()
            query = "select count(mpid) from job_id_and_mpid where mpid = {};".format(mpid)
            self.gp_cur.execute(query)
            is_exist = self.gp_cur.fetchone()[0]
            #print('First: ', time.time() - start)

            # New item -> insert
            if int(is_exist) == 0:
                # Insert to my site
                #start = time.time()
                query = "BEGIN;insert into job_id_and_mpid(job_id, mpid) values({}, {});COMMIT".format(job_id, mpid)
                self.gp_cur.execute(query)
                self.gp_conn.commit()
                #print('Second: ', time.time() - start)
                start = time.time()
                res = self.insert_product_to_mysite(job_id, new_product, groupbykeys)
                #print('Third: ', time.time() - start)
                if res != True: 
                    #start = time.time()
                    query = "BEGIN;delete from job_id_and_mpid where job_id = {} and mpid = {}; COMMIT;".format(job_id, mpid)
                    self.gp_cur.execute(query)
                    self.gp_conn.commit()
                    #print('Fourth: ', time.time() - start)
                    #start = time.time()
                    query = "select count(*) from job_id_and_mpid where mpid = {};".format(mpid)
                    self.gp_cur.execute(query)
                    is_exist = self.gp_cur.fetchone()[0]
                    #print('Fifth: ', time.time() - start)
                    if int(is_exist) == 0:
                        #start = time.time()
                        query = "BEGIN;delete from job_source_view where mpid = {}; delete from job_description_source_view where mpid = {};delete from job_thumbnail_source_view where mpid = {};delete from job_option_source_view where mpid = {};COMMIT;".format(mpid, mpid, mpid, mpid)
                        self.gp_cur.execute(query)
                        self.gp_conn.commit()
                        #print('Sixth: ', time.time() - start)

            # Existing item -> update
            else:
                # If current stock is zero => change staet as deleted
                if new_product['result_for_source_view']['stock'] == 'null' or new_product['result_for_source_view']['stock'] is None or new_product['result_for_source_view']['stock'] == '':
                    print_flushed(
                        "Out ot stock. mpid = {}, url = {}".format(mpid, url))
                    query = "update job_source_view set status = 3 where mpid = {};COMMIT".format(
                        mpid)
                    return
                stock = self.check_stock(new_product['result_for_source_view'])
                if stock == '0':
                    print_flushed(
                        "Out ot stock. mpid = {}, url = {}".format(mpid, url))
                    query = "update job_source_view set status = 3 where mpid = {};COMMIT".format(
                        mpid)
                    return

                # new_product = {result_for_source_view : {my_product_id:1, ..}, result_for_thumbnail: {} ...}
                # old_product = {mpid: 1,name: ~~,  ...}
                old_product = self.get_node_properties_from_mysite_for_update(job_id, mpid)
                self.update_mysite(job_id, old_product, new_product, mpid, sm_history_id, groupbykeys)

        except:
            self.gp_conn.rollback()
            raise

    def update_mysite(self, job_id, old_product, new_product, mpid, sm_history_id, groupbykeys):
        try:
            time_gap = timedelta(hours=9)
            # 0 = up to date 1 = changed 2 = New 3 = Deleted
            # new_product = {result_for_source_view : {my_product_id:1, ..}, result_for_thumbnail: {} ...}
            # old_product = {mpid: 1,name: ~~,  ...}
            up_to_date = 0
            changed = 0
            new_item = 0
            deleted = 0
            prd_in_other_job = 0
            new_but_out_of_stock = 0
            max_update_chunk = 30
            update_chunk = 0
            log_mpid = 0
            try:
                prd = {}
                # New version
                prd['prev_price'] = str(old_product.get('price', 0))
                prd['new_price'] = str(new_product['result_for_source_view'].get('price', 0))
                prd['v1_url'] = str(old_product.get('url', ''))
                prd['v2_url'] = str(new_product['result_for_source_view'].get('url', ''))
                prd['prev_stock'] = str(old_product.get('stock', 0))
                prd['new_stock'] = str(new_product['result_for_source_view'].get('stock', 0))
                prd['prev_num_options'] = str(old_product.get('num_options', 0))
                prd['new_num_options'] = str(new_product['result_for_source_view'].get('num_options', 0))
                prd['prev_option_name_list'] = sorted(list(old_product.get('option_name', {})))
                prd['new_option_name_list'] = sorted(new_product['result_for_option'].get('option_names', []))
                tmp_option_value_list = list(old_product.get('option_value', {}).values())
                prd['prev_option_value_list'] = sorted([str(item) for sublist in tmp_option_value_list for item in sublist])
                
                tmp_option_value_list = list(new_product['result_for_option'].get('option_values', {}).values())
                prd['new_option_value_list'] = sorted([str(item) for sublist in tmp_option_value_list for item in sublist])

                prd['prev_image_url_sha256_list'] = sorted(old_product.get('images', []))
                prd['new_image_url_sha256_list'] = sorted(new_product['result_for_thumbnail'].get('image_urls', []))

                prd['prev_description_sha256'] = str(old_product.get('description_sha256', ''))
                prd['new_description_sha256'] = str(hashlib.sha256(new_product['result_for_source_view'].get('description', '').encode()).hexdigest())
 

                log_mpid = mpid
                new_prd_mpid = mpid
                #new_prd_mpid = product[11]
                prd['prev_name'] = old_product.get('name', '')
                prd['new_name'] = new_product['result_for_source_view'].get('name', '').encode().hex()
                #print_flushed('{} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'.format(mpid)) 
                #print_flushed(prd) 
                is_changed = False
                is_new = False
                #tpid = self.get_tpid(job_id, targetsite_url, mpid)
                # temp
                tpid = 1

                # New / Up-to-date / Changed / Deleted
                # 1) Deleted 2) Up-to-date 3) Changed
                # Deleted: Stock -> 0
                if prd['new_stock'] == 0:
                    #print_flushed('@@@@@ New stock zero') 
                    c_date = new_product['result_for_source_view'].get('c_date', '')
                    query = "update job_source_view set status = 3, stock = 0, c_date = '{}', sm_date = now() where mpid = {}".format(mpid, c_date)
                    self.pg_cur.execute(query)
                    self.pg_conn.commit()
                else:
                    #print_flushed('@@@@@ new stock not zero') 
                    is_name_unchanged = (prd['prev_name'] == prd['new_name'])
                    is_price_unchanged = (prd['prev_price'] == prd['new_price'])
                    is_stock_unchanged = (prd['prev_stock'] == prd['new_stock'])
                    is_num_options_unchanged = (prd['prev_num_options'] == prd['prev_num_options'])
                    is_option_name_unchanged = (prd['prev_option_name_list'] == prd['new_option_name_list'])
                    is_option_value_unchanged = (prd['prev_option_value_list'] == prd['new_option_value_list'])
                    is_image_url_unchanged = (prd['prev_image_url_sha256_list'] == prd['new_image_url_sha256_list'])
                    is_description_unchanged = (prd['prev_description_sha256'] == prd['new_description_sha256'])
                    #print_flushed('-------------------------------------------------------------------------------------------------------------------')
                    #print_flushed('-------------------------------------------------------------------------------------------------------------------')
                    #print_flushed(prd)
                    #print_flushed('-------------------------------------------------------------------------------------------------------------------')
                    #print_flushed(is_name_unchanged, is_price_unchanged, is_stock_unchanged, is_num_options_unchanged, is_option_name_unchanged, is_image_url_unchanged, is_description_unchanged)
                    # 3) Changed
                    if False in [is_name_unchanged, is_price_unchanged, is_stock_unchanged, is_num_options_unchanged, is_option_name_unchanged, is_image_url_unchanged, is_description_unchanged]:
                        #print_flushed("@@@@@ Changed")
                        query = 'BEGIN; delete from job_source_view where mpid = {}; delete from job_description_source_view where mpid = {}; delete from job_thumbnail_source_view where mpid = {}; delete from job_option_source_view where mpid = {}; COMMIT;'.format(mpid, mpid, mpid, mpid)
                        self.pg_cur.execute(query)
                        self.pg_conn.commit()
                        #print_flushed("-----INSERT---S")
                        new_product['result_for_source_view']['status'] = 1
                        self.insert_product_to_mysite(job_id, new_product, groupbykeys)
                        #print_flushed("-----INSERT---E")

                    # 2) Up-to-date
                    else:
                        print_flushed('Up-to-date') 
                        c_date = new_product['result_for_source_view'].get('c_date', '')
                        query = "update job_source_view set status = 0, c_date = '{}', sm_date = now() where mpid = {};COMMIT;".format(c_date, mpid)
                        self.pg_cur.execute(query)
                        self.pg_conn.commit()


                update_chunk += 1
                if update_chunk == max_update_chunk:
                    cur_time = datetime.utcnow() + time_gap
                    cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
                    self.log_to_job_current_mysite_working(
                        '\n{}\n[Running] My site update\n'.format(cur_time), job_id)
                    update_chunk = 0
            except:
                err_msg = '================================ STACK TRACE ============================== \n' + \
                    str(traceback.format_exc())
                print_flushed(str(traceback.format_exc()))
                self.log_err_msg_of_my_site(sm_history_id, log_mpid, err_msg)

            cur_time = datetime.utcnow() + time_gap
            cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
            #self.log_to_job_current_mysite_working('\n{}\n[Finished] \nUp-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(cur_time, up_to_date, changed, new_item, deleted), job_id)
            #self.log_to_job_current_mysite_working(
            #    '\n{}\n[Finished] My site update\n'.format(cur_time), job_id)

            #query = "select count(*) from job_source_view where job_id = {} and status = 0".format(job_id)
            #self.pg_cur.execute(query)
            #num_up_to_date = self.pg_cur.fetchone()[0]

            #query = "select count(*) from job_source_view where job_id = {} and status = 1".format(job_id)
            #self.pg_cur.execute(query)
            #num_updated = self.pg_cur.fetchone()[0]

            #query = "select count(*) from job_source_view where job_id = {} and status = 2".format(job_id)
            #self.pg_cur.execute(query)
            #num_new = self.pg_cur.fetchone()[0]

            #query = "select count(*) from job_source_view where job_id = {} and status = 3".format(job_id)
            #self.pg_cur.execute(query)
            #num_deleted = self.pg_cur.fetchone()[0]
            #self.log_to_job_current_mysite_working('Up-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(
            #    num_up_to_date, num_updated, num_new, num_deleted), job_id)

            #query = "insert into job_update_statistics_history(job_id, up_to_date, changed, new_item, deleted) values({}, {}, {}, {}, {})".format(
            #    job_id, num_up_to_date, num_updated, num_new, num_deleted)
            #self.pg_cur.execute(query)
            #self.pg_conn.commit()

        except:
            self.pg_conn.rollback()
            raise
        finally:
            self.pg_conn.commit()
            return



    #def update_mysite_old(self, job_id, sm_history_id):
    #    try:
    #        query = "create view job"+job_id+"_update_view as (select v1.price as prev_price, v2.price new_price, v1.url as v1_url, v2.url as v2_url, v1.stock as prev_stock, v2.stock as new_stock, v1.num_options as prev_num_options, v2.num_options as new_num_options, v1.job_id as job_id1, v2.job_id as job_id2, v1.mpid as mpid, v2.mpid as prd2, v1.name_sha256 as prev_name, v2.name_sha256 as new_name from job_source_view as v1 full outer join job"+job_id+"_source_view_latest as v2 on v1.url_sha256 = v2.url_sha256)"
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()
    #        query = "select * from job"+job_id+"_update_view"
    #        self.pg_cur.execute(query)
    #        result = self.pg_cur.fetchall()
    #        self.pg_conn.commit()
    #        time_gap = timedelta(hours=9)
    #        # 0 = up to date 1 = changed 2 = New 3 = Deleted

    #        # temp
    #        up_to_date = 0
    #        changed = 0
    #        new_item = 0
    #        deleted = 0
    #        prd_in_other_job = 0
    #        new_but_out_of_stock = 0
    #        print_flushed("# of product to compare: ", len(result))
    #        max_update_chunk = 30
    #        update_chunk = 0
    #        log_mpid = 0
    #        try:
    #            for product in result:
    #                # New
    #                prd = {}
    #                prd['prev_price'] = product[0] if product[0] is not None else 0
    #                prd['new_price'] = product[1] if product[1] is not None else 0
    #                prd['v1_url'] = product[2] if product[2] is not None else ''
    #                prd['v2_url'] = product[3] if product[3] is not None else ''
    #                prd['prev_stock'] = product[4] if product[4] is not None else '0'
    #                prd['new_stock'] = product[5] if product[5] is not None else '0'
    #                prd['prev_num_options'] = product[6] if product[6] is not None else 0
    #                prd['new_num_options'] = product[7] if product[7] is not None else 0
    #                prd['job_id1'] = product[8] if product[8] is not None else -1
    #                prd['job_id2'] = product[9] if product[9] is not None else -1
    #                mpid = product[10]
    #                log_mpid = mpid
    #                new_prd_mpid = product[11]
    #                prd['prev_name'] = product[12] if product[12] is not None else ''
    #                prd['new_name'] = product[13] if product[13] is not None else ''
    #                is_changed = False
    #                is_new = False
    #                #tpid = self.get_tpid(job_id, targetsite_url, mpid)
    #                # temp
    #                tpid = 1
    #                # different job_id, job_id1 == -1 job_id2 != -1
    #                if prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id1') == -1:
    #                    # Deleted
    #                    if prd.get('new_stock') == '0':
    #                        new_but_out_of_stock += 1
    #                    else:
    #                        new_item += 1
    #                        is_new = True
    #                        query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered from job" + \
    #                            job_id + \
    #                                "_source_view_latest where url = '{}');".format(
    #                                    prd['v2_url'])
    #                        #query = "insert into job_source_view select * from job"+job_id+"_source_view_latest where url = '{}'".format(prd['v2_url'])
    #                        self.pg_cur.execute(query)
    #                        self.pg_conn.commit()

    #                # different job_id, job_id2 == -1 -> job_id1 is same job -> deleted
    #                elif prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id2') == -1 and int(prd.get('job_id1')) == int(job_id):

    #                    query = "select status from job_source_view where mpid = {} and job_id = {}".format(
    #                        mpid, job_id)
    #                    self.pg_cur.execute(query)
    #                    status = self.pg_cur.fetchone()[0]
    #                    if status == 3:
    #                        up_to_date += 1
    #                    else:
    #                        deleted += 1
    #                        query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(
    #                            prd['v1_url'])
    #                        self.pg_cur.execute(query)
    #                        self.pg_conn.commit()

    #                # different job_id, job_id2 == -1 -> job_id1 is other job
    #                elif prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id2') == -1 and int(prd.get('job_id1')) != int(job_id):
    #                    prd_in_other_job += 1

    #                elif (prd.get('job_id1') == prd.get('job_id2') and prd.get('job_id1') != -1):
    #                    # Deleted
    #                    if prd.get('v1_url') != '' and prd.get('v2_url') == '':
    #                        query = "select status from job_source_view where mpid = {} and job_id = {}".format(
    #                            mpid, job_id)
    #                        self.pg_cur.execute(query)
    #                        status = self.pg_cur.fetchone()[0]
    #                        if status == 3:
    #                            up_to_date += 1
    #                        else:
    #                            deleted += 1
    #                            query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(
    #                                prd['v1_url'])
    #                            self.pg_cur.execute(query)
    #                            self.pg_conn.commit()
    #                    # Deleted
    #                    # elif prd.get('prev_stock') != '0' and  prd.get('new_stock') == '0':
    #                    elif prd.get('new_stock') == '0':
    #                        query = "select status from job_source_view where mpid = {} and job_id = {}".format(
    #                            mpid, job_id)
    #                        self.pg_cur.execute(query)
    #                        status = self.pg_cur.fetchone()[0]
    #                        if status == 3:
    #                            up_to_date += 1
    #                        else:
    #                            deleted += 1
    #                            query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(
    #                                prd['v1_url'])
    #                            self.pg_cur.execute(query)
    #                            self.pg_conn.commit()
    #                    elif prd.get('prev_name') != prd.get('new_name'):
    #                        is_changed = True
    #                    # Changed: num option 0 -> larger than 1
    #                    elif prd.get('prev_num_options', 0) == 0 and prd.get('new_num_options', 0) >= 1:
    #                        query = "select option_name, option_value, price, stock from job_option_source_view where job_id = {} and mpid = {}".format(
    #                            job_id, mpid)
    #                        self.pg_cur.execute(query)
    #                        res1 = self.pg_cur.fetchall()
    #                        res1_list = []
    #                        for idx, val in enumerate(res1):
    #                            res1_list.append(
    #                                (val[0], val[1], val[2], val[3]))

    #                        query = "select option_name, option_value, price, stock from job" + \
    #                            job_id + \
    #                                "_option_source_view_latest where mpid = {}".format(
    #                                    mpid)
    #                        self.pg_cur.execute(query)
    #                        res2 = self.pg_cur.fetchall()
    #                        res2_list = []
    #                        for idx, val in enumerate(res2):
    #                            res2_list.append(
    #                                (val[0], val[1], val[2], val[3]))
    #                        is_changed = True
    #                    # Changed: num option larger than 1 -> 0
    #                    elif prd.get('prev_num_options', 0) >= 1 and prd.get('new_num_options', 0) == 0:
    #                        is_changed = True
    #                    # Changed: both of num option larger than 1 but diff
    #                    elif prd.get('prev_num_options', 0) >= 1 and prd.get('new_num_options', 0) >= 1 and (prd.get('prev_num_options', 0) != prd.get('new_num_options', 0)):
    #                        is_changed = True
    #                    # both of num option larger than 1 and same -> check each option
    #                    elif prd.get('prev_num_options', 0) >= 1 and prd.get('new_num_options', 0) >= 1 and (prd.get('prev_num_options', 0) == prd.get('new_num_options', 0)):
    #                        query = "select option_name, option_value, price, stock from job_option_source_view where job_id = {} and mpid = {}".format(
    #                            job_id, mpid)
    #                        self.pg_cur.execute(query)
    #                        res1 = self.pg_cur.fetchall()
    #                        res1_list = []
    #                        for idx, val in enumerate(res1):
    #                            res1_list.append(
    #                                (val[0], val[1], val[2], val[3]))

    #                        query = "select option_name, option_value, price, stock from job" + \
    #                            job_id + \
    #                                "_option_source_view_latest where mpid = {}".format(
    #                                    mpid)
    #                        self.pg_cur.execute(query)
    #                        res2 = self.pg_cur.fetchall()
    #                        res2_list = []
    #                        for idx, val in enumerate(res2):
    #                            res2_list.append(
    #                                (val[0], val[1], val[2], val[3]))

    #                        is_same = True

    #                        if is_same == False:
    #                            is_changed = True
    #                        else:
    #                            query = "select image_url_sha256 from job_thumbnail_source_view where mpid = {} and job_id = {}".format(
    #                                mpid, job_id)
    #                            self.pg_cur.execute(query)
    #                            res1 = self.pg_cur.fetchall()
    #                            res1_list = []
    #                            for idx, val in enumerate(res1):
    #                                res1_list.append(val[0])

    #                            query = "select image_url_sha256 from job"+job_id + \
    #                                "_thumbnail_source_view_latest where mpid = {}".format(
    #                                    mpid)
    #                            self.pg_cur.execute(query)
    #                            res2 = self.pg_cur.fetchall()
    #                            res2_list = []
    #                            for idx, val in enumerate(res2):
    #                                res2_list.append(val[0])

    #                            is_same_img_list = True
    #                            if sorted(res1_list) != sorted(res2_list):
    #                                is_same_img_list = False

    #                            if is_same_img_list == False:
    #                                is_changed = True
    #                            elif is_same_img_list == True:
    #                                query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(
    #                                    mpid, 'description_sha256', job_id)
    #                                self.pg_cur.execute(query)
    #                                res = self.pg_cur.fetchone()
    #                                res1_desc = ''
    #                                if res is not None:
    #                                    res1_desc = res[0]

    #                                query = "select value from job"+job_id + \
    #                                    "_description_source_view_latest where mpid = {} and key = '{}'".format(
    #                                        mpid, 'description_sha256')
    #                                self.pg_cur.execute(query)
    #                                res = self.pg_cur.fetchone()
    #                                res2_desc = ''
    #                                if res is not None:
    #                                    res2_desc = res[0]

    #                                if res1_desc != res2_desc:
    #                                    is_changed = True
    #                                # New
    #                                elif (prd.get('prev_price') == 0 and prd.get('new_price') != 0):
    #                                    new_item += 1
    #                                    is_new = True
    #                                    query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered from job" + \
    #                                        job_id + \
    #                                            "_source_view_latest where url = '{}');".format(
    #                                                prd['v2_url'])
    #                                    self.pg_cur.execute(query)
    #                                    self.pg_conn.commit()
    #                                    query = "update job_source_view set status = 2 where url = '{}'".format(
    #                                        prd['v2_url'])
    #                                    self.pg_cur.execute(query)
    #                                    self.pg_conn.commit()
    #                                # Deleted
    #                                elif (prd.get('prev_price') != 0 and prd.get('new_price') == 0):
    #                                    query = "select status from job_source_view where mpid = {} and job_id = {}".format(
    #                                        mpid, job_id)
    #                                    self.pg_cur.execute(query)
    #                                    status = self.pg_cur.fetchone()[0]
    #                                    if status == 3:
    #                                        up_to_date += 1
    #                                    else:
    #                                        deleted += 1
    #                                        query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(
    #                                            prd['v2_url'])
    #                                        self.pg_cur.execute(query)
    #                                        self.pg_conn.commit()
    #                                # Updated
    #                                elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') != prd.get('new_price')):
    #                                    is_changed = True
    #                                # Same price
    #                                elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') == prd.get('new_price')):
    #                                    # stock changed
    #                                    if (prd.get('new_stock') != prd.get('prev_stock')):
    #                                        is_changed = True
    #                                    # up to date
    #                                    else:
    #                                        up_to_date += 1
    #                                        query = "update job_source_view set status = 0 where url = '{}'".format(
    #                                            prd['v2_url'])
    #                                        self.pg_cur.execute(query)
    #                                        self.pg_conn.commit()

    #                    # both of num option 0
    #                    elif prd.get('prev_num_options', 0) == 0 and prd.get('new_num_options', 0) == 0:
    #                        query = "select image_url_sha256 from job_thumbnail_source_view where mpid = {} and job_id = {}".format(
    #                            mpid, job_id)
    #                        self.pg_cur.execute(query)
    #                        res1 = self.pg_cur.fetchall()
    #                        res1_list = []
    #                        for idx, val in enumerate(res1):
    #                            res1_list.append(val[0])

    #                        query = "select image_url_sha256 from job"+job_id + \
    #                            "_thumbnail_source_view_latest where mpid = {}".format(
    #                                mpid)
    #                        self.pg_cur.execute(query)
    #                        res2 = self.pg_cur.fetchall()
    #                        res2_list = []
    #                        for idx, val in enumerate(res2):
    #                            res2_list.append(val[0])

    #                        is_same_img_list = True
    #                        if sorted(res1_list) != sorted(res2_list):
    #                            is_same_img_list = False

    #                        if is_same_img_list == False:
    #                            is_changed = True
    #                        else:
    #                            query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(
    #                                mpid, 'description_sha256', job_id)
    #                            self.pg_cur.execute(query)
    #                            res = self.pg_cur.fetchone()
    #                            res1_desc = ''
    #                            if res is not None:
    #                                res1_desc = res[0]

    #                            query = "select value from job"+job_id + \
    #                                "_description_source_view_latest where mpid = {} and key = '{}'".format(
    #                                    mpid, 'description_sha256')
    #                            self.pg_cur.execute(query)
    #                            res = self.pg_cur.fetchone()
    #                            res2_desc = ''
    #                            if res is not None:
    #                                res2_desc = res[0]

    #                            if res1_desc != res2_desc:
    #                                query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(
    #                                    mpid, 'description', job_id)
    #                                self.pg_cur.execute(query)
    #                                res = self.pg_cur.fetchone()
    #                                res1_desc = ''
    #                                if res is not None:
    #                                    res1_desc = res[0]
    #                                res1_desc = res[0]

    #                                query = "select value from job"+job_id + \
    #                                    "_description_source_view_latest where mpid = {} and key = '{}'".format(
    #                                        mpid, 'description')
    #                                self.pg_cur.execute(query)
    #                                res = self.pg_cur.fetchone()
    #                                res2_desc = ''
    #                                if res is not None:
    #                                    res2_desc = res[0]
    #                                res2_desc = res[0]

    #                                is_changed = True
    #                            # New
    #                            elif (prd.get('prev_price') == 0 and prd.get('new_price') != 0):
    #                                new_item += 1
    #                                is_new = True
    #                                query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered from job" + \
    #                                    job_id + \
    #                                        "_source_view_latest where url = '{}');".format(
    #                                            prd['v2_url'])
    #                                self.pg_cur.execute(query)
    #                                self.pg_conn.commit()
    #                                query = "update job_source_view_latest set status = 2 where url = '{}'".format(
    #                                    prd['v2_url'])
    #                                self.pg_cur.execute(query)
    #                                self.pg_conn.commit()
    #                            # Deleted
    #                            elif (prd.get('prev_price') != 0 and prd.get('new_price') == 0):
    #                                query = "select status from job_source_view where mpid = {} and job_id = {}".format(
    #                                    mpid, job_id)
    #                                self.pg_cur.execute(query)
    #                                status = self.pg_cur.fetchone()[0]
    #                                if status == 3:
    #                                    up_to_date += 1
    #                                else:
    #                                    deleted += 1
    #                                    query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(
    #                                        prd['v2_url'])
    #                                    self.pg_cur.execute(query)
    #                                    self.pg_conn.commit()
    #                            # Updated
    #                            elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') != prd.get('new_price')):
    #                                is_changed = True
    #                            # Same price
    #                            elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') == prd.get('new_price')):
    #                                # stock changed
    #                                if (prd.get('new_stock') != prd.get('prev_stock')):
    #                                    is_changed = True
    #                                # up to date
    #                                else:
    #                                    up_to_date += 1
    #                                    query = "update job_source_view set status = 0 where url = '{}'".format(
    #                                        prd['v2_url'])
    #                                    self.pg_cur.execute(query)
    #                                    self.pg_conn.commit()

    #                if is_changed == True:
    #                    changed += 1
    #                    query = 'delete from job_source_view where mpid = {}'.format(
    #                        mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()
    #                    query = "insert into job_source_view(job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered ) (select job_id, mpid, 1, c_date, sm_date, url, url_sha256, name, name_sha256, price, image_url, num_options, num_images, m_category, groupby_key_sha256, source_site_product_id, stock, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered from job"+job_id+"_source_view_latest where mpid = {});".format(mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    #self.insert_tpid_into_history_table(job_id, targetsite_url, mpid, tpid)
    #                    #self.update_tpid_into_mapping_table(job_id, targetsite_url, mpid, tpid)

    #                    query = 'delete from job_description_source_view where job_id = {} and mpid = {};COMMIT;'.format(
    #                        job_id, mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()
    #                    query = "insert into job_description_source_view select * from job" + \
    #                        job_id + \
    #                            "_description_source_view_latest where mpid = {};COMMIT;".format(
    #                                mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    query = 'delete from job_option_source_view where job_id = {} and mpid = {};COMMIT;'.format(
    #                        job_id, mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()
    #                    query = "insert into job_option_source_view select  * from job" + \
    #                        job_id + \
    #                            "_option_source_view_latest where mpid = {};COMMIT;".format(
    #                                mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    query = 'delete from job_thumbnail_source_view where job_id = {} and mpid = {};COMMIT;'.format(
    #                        job_id, mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()
    #                    query = "insert into job_thumbnail_source_view select * from job" + \
    #                        job_id + \
    #                            "_thumbnail_source_view_latest where mpid = {};COMMIT".format(
    #                                mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    query = "select id, key_name from mysite_translate_key where job_id = {}".format(job_id);
    #                    self.gp_cur.execute(query)
    #                    result = self.gp_cur.fetchall()
    #                    option_translate = False
    #                    if result is not None:
    #                        translate_keys = []
    #                        for idx, val in enumerate(result):
    #                           for idx2, val2 in enumerate(val):
    #                              if idx2 == 1:
    #                                 if is_hex_str(val2) == False:
    #                                    translate_keys.append(val2)
    #                                 else:
    #                                    translate_keys.append(bytes.fromhex(val2).decode())
    #                        if len(translate_keys) != 0:
    #                            translator = Translator()
    #                            if val != 'option':
    #                                for idx, val in translate_keys:
    #                                    translated_text = ''
    #                                    try:
    #                                        translated_text = translator.translate(value['result_for_source_view'][val], src='ja', dest="ko").text
    #                                    except:
    #                                        pass
    #                                    if translated_text != '':
    #                                        query = 'update job_soure_view set {}_translated = {} where job_id = {} and mpid = {}; COMMIT;'.format(val, translated_text, job_id, mpid)
    #                                        self.gp_cur.execute(query)
    #                                        self.gp_conn.commit()
    #                            else:
    #                                query = "select option_name, option_value from job_option_source_view where job_id = {} and mpid = {}".format(job_id, mpid)
    #                                self.gp_cur.execute(query)
    #                                result_option = self.gp_cur.fetchall()
    #                                if result_option is not None:
    #                                    for idx3, val3 in enumerate(result_option):
    #                                        for idx4, val4 in enumerate(val3):
    #                                            on_translated = ''
    #                                            ov_translated = ''
    #                                            if idx4 == 0:
    #                                                if is_hex_str(val4) == False:
    #                                                    try:
    #                                                        on_translated = translator.translate(val4, src='ja', dest="ko").text
    #                                                    except:
    #                                                        pass
    #                                                else:
    #                                                    try:
    #                                                        on_translated = translator.translate(bytes.fromhex(val4).decode(), src='ja', dest="ko").text
    #                                                    except:
    #                                                        pass
    #                                            elif idx4 == 1:
    #                                                if is_hex_str(val4) == False:
    #                                                    try:
    #                                                        ov_translated = translator.translate(val4, src='ja', dest="ko").text
    #                                                    except:
    #                                                        pass
    #                                                else:
    #                                                    try:
    #                                                        ov_translated = translator.translate(bytes.fromhex(val4).decode(), src='ja', dest="ko").text
    #                                                    except:
    #                                                        pass
    #                                        query = 'update job_soure_view set option_name_translated = {}, option_value_translated = {} where job_id = {} and mpid = {}; COMMIT;'.format(on_translated.encode('UTF-8').hex(), ov_translated.encode('UTF-8').hex(), job_id, mpid)
    #                                        self.gp_cur.execute(query)
    #                                        self.gp_conn.commit()


    #                elif is_new == True:
    #                    log_mpid = new_prd_mpid
    #                    query = "insert into job_description_source_view select * from job"+job_id + \
    #                        "_description_source_view_latest where mpid = {};COMMIT;".format(
    #                            new_prd_mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    query = "insert into job_option_source_view select  * from job"+job_id + \
    #                            "_option_source_view_latest where mpid = {};COMMIT:".format(
    #                            new_prd_mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()

    #                    query = "insert into job_thumbnail_source_view select * from job"+job_id + \
    #                        "_thumbnail_source_view_latest where mpid = {};COMMIT;".format(
    #                            new_prd_mpid)
    #                    self.pg_cur.execute(query)
    #                    self.pg_conn.commit()
    #                update_chunk += 1
    #            if update_chunk == max_update_chunk:
    #                cur_time = datetime.utcnow() + time_gap
    #                cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #                #self.log_to_job_current_mysite_working('\n{}\n[Running] \nUp-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(cur_time, up_to_date, changed, new_item, deleted), job_id)
    #                self.log_to_job_current_mysite_working(
    #                    '\n{}\n[Running] My site update\n'.format(cur_time), job_id)
    #                update_chunk = 0
    #        except:
    #            err_msg = '================================ STACK TRACE ============================== \n' + \
    #                str(traceback.format_exc())
    #            self.log_err_msg_of_my_site(sm_history_id, log_mpid, err_msg)

    #        cur_time = datetime.utcnow() + time_gap
    #        cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
    #        #self.log_to_job_current_mysite_working('\n{}\n[Finished] \nUp-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(cur_time, up_to_date, changed, new_item, deleted), job_id)
    #        self.log_to_job_current_mysite_working(
    #            '\n{}\n[Finished] My site update\n'.format(cur_time), job_id)

    #        query = "select count(*) from job_source_view where job_id = {} and status = 0".format(job_id)
    #        self.pg_cur.execute(query)
    #        num_up_to_date = self.pg_cur.fetchone()[0]

    #        query = "select count(*) from job_source_view where job_id = {} and status = 1".format(job_id)
    #        self.pg_cur.execute(query)
    #        num_updated = self.pg_cur.fetchone()[0]

    #        query = "select count(*) from job_source_view where job_id = {} and status = 2".format(job_id)
    #        self.pg_cur.execute(query)
    #        num_new = self.pg_cur.fetchone()[0]

    #        query = "select count(*) from job_source_view where job_id = {} and status = 3".format(job_id)
    #        self.pg_cur.execute(query)
    #        num_deleted = self.pg_cur.fetchone()[0]
    #        self.log_to_job_current_mysite_working('Up-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(
    #            num_up_to_date, num_updated, num_new, num_deleted), job_id)

    #        query = "insert into job_update_statistics_history(job_id, up_to_date, changed, new_item, deleted) values({}, {}, {}, {}, {})".format(
    #            job_id, num_up_to_date, num_updated, num_new, num_deleted)
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()

    #    except:
    #        self.pg_conn.rollback()
    #        raise
    #    finally:
    #        query = "drop view if exists job"+job_id+"_update_view"
    #        self.pg_cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_source_view"
    #        self.pg_cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_source_view_latest"
    #        self.pg_cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_option_source_view_latest"
    #        self.pg_cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_description_source_view_latest"
    #        self.pg_cur.execute(query)
    #        query = "drop table if exists job"+job_id+"_thumbnail_source_view_latest"
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()
    #        return


    def log_expected_num_detail(self, exec_id, cnt):
        try:
            query = 'update execution set num_expected_all = {} where id = {};COMMIT;'.format(cnt, exec_id)
            self.pg_cur.execute(query)
            return 
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e


    def log_expected_num_detail_success(self, exec_id, cnt):
        try:
            query = 'update execution set num_expected_success = num_expected_success + {} where id = {};COMMIT;'.format(cnt, exec_id)
            self.pg_cur.execute(query)
            return 
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def find_nodes_of_execution_with_label(self, exec_id, label):
        try:
            query = 'select n.id'
            query += ' from node n, stage s, task t'
            query += ' where n.task_id = t.id and t.stage_id = s.id and s.execution_id = %s and n.label = %s order by n.id asc'
            print_flushed(query % (str(exec_id), str(label)))
            self.pg_cur.execute(query, (str(exec_id), str(label)))
            result = self.pg_cur.fetchall()
            self.pg_conn.commit()
            return list(map(lambda x: x[0], result))
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def find_nodes_of_task_with_label(self, task_id, label):
        try:
            query = 'select id'
            query += ' from node n'
            query += ' where n.task_id = %s and n.label = %s;'
            self.pg_cur.execute(query, (str(task_id), str(label)))
            result = self.pg_cur.fetchall()
            self.pg_conn.commit()
            return list(map(lambda x: x[0], result))
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def find_n_hop_neighbors(self, node_id, labels):
        try:
            query = 'select n{}.id'.format(len(labels), len(labels))
            query += ' from node n1'
            for i in range(2, len(labels) + 1):
                query += ', node n{}'.format(i)
            query += ' where n1.parent_id = {} and n1.label = {}'.format(
                node_id, labels[0])
            for i in range(2, len(labels) + 1):
                query += ' and n{}.parent_id = n{}.id and n{}.label = {}'.format(
                    i, i-1, i, labels[i-1])
            query += ' order by n{}.id;'.format(len(labels), len(labels))
            self.pg_cur.execute(query)
            result = list(map(lambda x: x[0], self.pg_cur.fetchall()))
            self.pg_conn.commit()
            return result
        except Exception as e:
            self.pg_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def insert_node_property(self, nodeId, key, value):
        try:
            query = 'INSERT INTO node_property (node_id, key, value) '
            query += 'VALUES (%s, %s, %s)'
            value = json.dumps(value)
            self.gp_cur.execute(query, (str(nodeId), str(key), str(value)))
            self.gp_conn.commit()
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    #def insert_node_property_to_tmp_mysite(self, job_id, value, groupbykeys):
    #    try:
    #        # create tmp source_view
    #        job_id = str(job_id)
    #        query = 'create table if not exists job'+str(job_id)+'_source_view_latest (job_id integer, id integer primary key generated always as identity, mpid integer, status integer, c_date timestamp, sm_date timestamp, url text, url_sha256  varchar(64), name text, name_sha256 varchar(64), price text, stock text, image_url text, num_options integer, num_images integer, m_category text, tpid varchar(128), groupby_key_sha256 varchar(64), weight text, shipping_weight text, shipping_price text, brand text, source_site_product_id text, description text, description1 text, description2 text, description_rendered text, description1_rendered text, description2_rendered text);'
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()

    #        # create tmp description_source_view
    #        query = 'create table if not exists job' + \
    #            str(job_id)+'_description_source_view_latest (job_id integer, mpid integer, key text, value text)'
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()

    #        # create tmp option_source_view
    #        query = 'create table if not exists job' + \
    #            str(job_id)+'_option_source_view_latest (job_id integer, mpid integer, option_name text, option_value text, list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg text)'
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()

    #        # create tmp thumbnail_source_view
    #        query = 'create table if not exists job' + \
    #            str(job_id)+'_thumbnail_source_view_latest (job_id integer, mpid integer, image_url text, image_url_sha256 varchar(64))'
    #        self.pg_cur.execute(query)
    #        self.pg_conn.commit()

    #        # insert data to tmp source_view
    #        query = 'INSERT INTO job' + \
    #            str(job_id)+'_source_view_latest (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options, num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered) '
    #        query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    #        mpid = self.none_to_blank(
    #            str(value['result_for_source_view']['my_product_id']))
    #        status = self.none_to_blank(
    #            str(value['result_for_source_view']['status']))
    #        c_date = self.none_to_blank(
    #            str(value['result_for_source_view']['c_date']))
    #        sm_date = self.none_to_blank(
    #            str(value['result_for_source_view']['sm_date']))
    #        url = self.none_to_blank(
    #            str(value['result_for_source_view']['url']))
    #        url_sha256 = str(hashlib.sha256(
    #            url.encode('UTF-8', 'strict')).hexdigest())
    #        name_origin = self.none_to_blank(
    #            str(value['result_for_source_view']['name']))
    #        name = name_origin.encode('UTF-8', 'strict').hex()
    #        name_sha256 = str(hashlib.sha256(
    #            name_origin.encode('UTF-8', 'strict')).hexdigest())
    #        price = self.none_to_blank(
    #            str(value['result_for_source_view']['price']))
    #        if value['result_for_source_view']['stock'] == 'null' or value['result_for_source_view']['stock'] is None:
    #            print_flushed(
    #                "Out ot stock. mpid = {}, url = {}".format(mpid, url))
    #            return False
    #        stock = self.check_stock(str(value['result_for_source_view']))
    #        if stock == '0':
    #            print_flushed(
    #                "Out ot stock. mpid = {}, url = {}".format(mpid, url))
    #            return False
    #        image_url = self.none_to_blank(
    #            str(value['result_for_source_view'].get('image_url', '')))
    #        num_options = self.none_to_blank(
    #            str(value['result_for_source_view']['num_options']))
    #        num_images = self.none_to_blank(
    #            str(value['result_for_source_view']['num_images']))
    #        source_site_product_id = self.none_to_blank(
    #            str(value['result_for_source_view']['source_site_product_id']))
    #        brand = self.none_to_blank(
    #            str(value['result_for_source_view'].get('brand', '')))
    #        weight = self.none_to_blank(
    #            str(value['result_for_source_view'].get('weight', '')))
    #        shipping_weight = self.none_to_blank(
    #            str(value['result_for_source_view'].get('shipping_weight', '')))
    #        shipping_price = self.none_to_blank(
    #            str(value['result_for_source_view'].get('shipping_price', '')))
    #        description = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description', ''))).encode('UTF-8').hex()
    #        description1 = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description1', ''))).encode('UTF-8').hex()
    #        description2 = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description2', ''))).encode('UTF-8').hex()
    #        description_rendered = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description_rendered', ''))).encode('UTF-8').hex()
    #        description1_rendered = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description1_rendered', ''))).encode('UTF-8').hex()
    #        description2_rendered = self.none_to_blank(
    #            str(value['result_for_source_view'].get('description2_rendered', ''))).encode('UTF-8').hex()




    #        groupbykeys = sorted(groupbykeys)
    #        groupby_key = ''
    #        for key in groupbykeys:
    #            if key not in ['mpid', 'url', 'name', 'price', 'stock', 'brand', 'source_site_product_id', 'brand', 'weight', 'shipping_weight', 'shipping_price', 'description', 'description1', 'description2', 'description_rendered', 'description1_rendered', 'description2_rendered']:
    #                groupby_key += str(value['result_for_desc'].get(key, ''))
    #            else:
    #                groupby_key += str(value['result_for_source_view'].get(key,''))
    #        groupby_key = str(groupby_key)
    #        groupby_key_sha256 = str(hashlib.sha256(
    #            groupby_key.encode()).hexdigest())
    #        self.gp_cur.execute(query, (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options,
    #                            num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered))
    #        self.gp_conn.commit()

    #        # insert data to tmp description_source_view
    #        query = 'INSERT INTO job' + \
    #            str(job_id)+'_description_source_view_latest (job_id, mpid, key, value) '
    #        query += 'VALUES (%s,%s, %s, %s)'
    #        #mpid = str(value['result_for_desc']['my_product_id'])
    #        for key in value['result_for_desc']['key']:
    #            if key == 'description':
    #                attr = self.none_to_blank(
    #                    str(value['result_for_desc']['value'][key]))
    #                self.gp_cur.execute(query, (job_id, mpid, 'description_sha256', str(
    #                    hashlib.sha256(attr.encode()).hexdigest())))
    #            attr = self.none_to_blank(
    #                str(value['result_for_desc']['value'][key]))
    #            attr = attr.encode('UTF-8').hex()
    #            self.gp_cur.execute(query, (job_id, mpid, key, attr))
    #        self.gp_conn.commit()

    #        # insert data to tmp option_source_view
    #        query = 'INSERT INTO job' + \
    #            str(job_id)+'_option_source_view_latest (job_id, mpid, option_name, option_value, list_price, price, stock, stock_status, msg)'
    #        query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    #        mpid = str(value['result_for_option']['my_product_id'])
    #        for option_name in value['result_for_option']['option_names']:
    #            for option_val in value['result_for_option']['option_values'][option_name]:
    #                list_price = self.none_to_blank(
    #                    str(value['result_for_option']['list_price']))
    #                price = self.none_to_blank(
    #                    str(value['result_for_option']['price']))
    #                stock = self.check_stock(value['result_for_option'])
    #                stock_status = self.none_to_blank(
    #                    str(value['result_for_option']['stock_status']))
    #                msg = self.none_to_blank(
    #                    str(value['result_for_option']['msg']))
    #                self.gp_cur.execute(query, (job_id, mpid, option_name.encode().hex(
    #                ), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg))
    #        self.gp_conn.commit()

    #        # insert data to tmp option_source_view
    #        query = 'INSERT INTO job' + \
    #            str(job_id)+'_thumbnail_source_view_latest (job_id, mpid, image_url, image_url_sha256)'
    #        query += 'VALUES (%s, %s, %s, %s)'

    #        mpid = str(value['result_for_option']['my_product_id'])
    #        for img_url in value['result_for_thumbnail']['image_urls']:
    #            if img_url is not None:
    #                self.gp_cur.execute(query, (job_id, mpid, str(img_url), str(
    #                    hashlib.sha256(img_url.encode()).hexdigest())))
    #        self.gp_conn.commit()
    #    except:
    #        self.gp_conn.rollback()
    #        print_flushed(str(traceback.format_exc()))
    #        raise

   # def insert_node_property_to_mysite_old(self, job_id, value, groupbykeys):
   #     try:

   #         # insert data to source_view
   #         job_id = str(job_id)
   #         query = 'INSERT INTO job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options, num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered) '
   #         query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

   #         mpid = self.none_to_blank(
   #             str(value['result_for_source_view']['my_product_id']))
   #         status = self.none_to_blank(
   #             str(value['result_for_source_view']['status']))
   #         c_date = self.none_to_blank(
   #             str(value['result_for_source_view']['c_date']))
   #         sm_date = self.none_to_blank(
   #             str(value['result_for_source_view']['sm_date']))
   #         url = self.none_to_blank(
   #             str(value['result_for_source_view']['url']))
   #         url_sha256 = str(hashlib.sha256(
   #             url.encode('UTF-8', 'strict')).hexdigest())
   #         name_origin = self.none_to_blank(
   #             str(value['result_for_source_view']['name']))
   #         name = name_origin.encode('UTF-8', 'strict').hex()
   #         name_sha256 = str(hashlib.sha256(
   #             name_origin.encode('UTF-8', 'strict')).hexdigest())
   #         price = self.none_to_blank(
   #             str(value['result_for_source_view']['price']))
   #         if value['result_for_source_view']['stock'] == 'null' or value['result_for_source_view']['stock'] is None:
   #             print_flushed(
   #                 "Out ot stock. mpid = {}, url = {}".format(mpid, url))
   #             return False
   #         stock = self.check_stock(str(value['result_for_source_view']))
   #         if stock == '0':
   #             print_flushed(
   #                 "Out ot stock. mpid = {}, url = {}".format(mpid, url))
   #             return False
   #         image_url = self.none_to_blank(
   #             str(value['result_for_source_view'].get('image_url', '')))
   #         num_options = self.none_to_blank(
   #             str(value['result_for_source_view']['num_options']))
   #         num_images = self.none_to_blank(
   #             str(value['result_for_source_view']['num_images']))
   #         source_site_product_id = self.none_to_blank(
   #             str(value['result_for_source_view']['source_site_product_id']))
   #         brand = self.none_to_blank(
   #             str(value['result_for_source_view'].get('brand', '')))
   #         weight = self.none_to_blank(
   #             str(value['result_for_source_view'].get('weight', '')))
   #         shipping_weight = self.none_to_blank(
   #             str(value['result_for_source_view'].get('shipping_weight', '')))
   #         shipping_price = self.none_to_blank(
   #             str(value['result_for_source_view'].get('shipping_price', '')))
   #         description = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description', ''))).encode('UTF-8').hex()
   #         description1 = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description1', ''))).encode('UTF-8').hex()
   #         description2 = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description2', ''))).encode('UTF-8').hex()
   #         description_rendered = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description_rendered', ''))).encode('UTF-8').hex()
   #         description1_rendered = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description1_rendered', ''))).encode('UTF-8').hex()
   #         description2_rendered = self.none_to_blank(
   #             str(value['result_for_source_view'].get('description2_rendered', ''))).encode('UTF-8').hex()

 

   #         groupbykeys = sorted(groupbykeys)
   #         groupby_key = ''
   #         for key in groupbykeys:
   #             if key not in ['mpid', 'url', 'name', 'price', 'stock', 'brand', 'source_site_product_id', 'brand', 'weight', 'shipping_weight', 'shipping_price', 'description', 'description1', 'description2', 'description_rendered', 'description1_rendered', 'description2_rendered']:
   #                 groupby_key += str(value['result_for_desc']['value'].get(key, ''))
   #             else:
   #                 groupby_key += str(value['result_for_source_view'].get(key,''))
   #         groupby_key = str(groupby_key)
   #         groupby_key_sha256 = str(hashlib.sha256(
   #             groupby_key.encode()).hexdigest())
   #         self.gp_cur.execute(query, (job_id, mpid, status, c_date, sm_date, url, url_sha256, name, name_sha256, price, stock, image_url, num_options, num_images, groupby_key_sha256, source_site_product_id, brand, weight, shipping_price, shipping_weight, description, description1, description2, description_rendered, description1_rendered, description2_rendered))
   #         self.gp_conn.commit()
   #         

   #         query = "select id, key_name from mysite_translate_key where job_id = {}".format(job_id);
   #         self.gp_cur.execute(query)
   #         result = self.gp_cur.fetchall()
   #         option_translate = False
   #         if result is not None:
   #             translate_keys = []
   #             for idx, val in enumerate(result):
   #                for idx2, val2 in enumerate(val):
   #                   if idx2 == 1:
   #                      if is_hex_str(val2) == False:
   #                         translate_keys.append(val2)
   #                      else:
   #                         translate_keys.append(bytes.fromhex(val2).decode())
   #             if len(translate_keys) != 0:
   #                 translator = Translator()
   #                 if val != 'option':
   #                     for idx, val in translate_keys:
   #                         translated_text = ''
   #                         try:
   #                             translated_text = translator.translate(value['result_for_source_view'][val], src='ja', dest="ko").text
   #                         except:
   #                             pass
   #                         if translated_text != '':
   #                             query = 'update job_soure_view set {}_translated = {} where job_id = {} and mpid = {}; COMMIT;'.format(val, translated_text, job_id, mpid)
   #                             self.gp_cur.execute(query)
   #                             self.gp_conn.commit()
   #                 else:
   #                     option_translate = True

   #         # insert data to description_source_view
   #         # (job_id integer, mpid integer, key varchar(2048), value text)
   #         query = 'INSERT INTO job_description_source_view (job_id, mpid, key, value) '
   #         query += 'VALUES (%s, %s, %s, %s)'
   #         mpid = int(value['result_for_desc']['my_product_id'])
   #         for key in value['result_for_desc']['key']:
   #             if key == 'description':
   #                 attr = self.none_to_blank(
   #                     str(value['result_for_desc']['value'][key]))
   #                 self.gp_cur.execute(query, (job_id, str(mpid), 'description_sha256', str(
   #                     hashlib.sha256(attr.encode()).hexdigest())))
   #             attr = str(value['result_for_desc']['value'][key])
   #             attr = attr.encode('UTF-8').hex()
   #             self.gp_cur.execute(query, (job_id, mpid, key, attr))
   #         self.gp_conn.commit()

   #         # insert data to option_source_view
   #         # (job_id integer, mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048))
   #         query = 'INSERT INTO job_option_source_view (job_id, mpid, option_name, option_value, list_price, price, stock, stock_status, msg, option_name_translated, option_value_translated) '
   #         query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
   #         mpid = str(value['result_for_option']['my_product_id'])
   #         for option_name in value['result_for_option']['option_names']:
   #             option_name_translated = ''
   #             if option_translate == True:
   #                 try:
   #                     option_name_translated = translator.translate(option_name, src='ja', dest="ko").text
   #                 except:
   #                     pass
   #             for option_val in value['result_for_option']['option_values'][option_name]:
   #                 list_price = self.none_to_blank(
   #                     str(value['result_for_option']['list_price']))
   #                 price = self.none_to_blank(
   #                     str(value['result_for_option']['price']))
   #                 stock = self.check_stock(value['result_for_option'])
   #                 stock_status = self.none_to_blank(
   #                     str(value['result_for_option']['stock_status']))
   #                 msg = self.none_to_blank(
   #                     str(value['result_for_option']['msg']))
   #                 option_val_translated = ''
   #                 if option_translate == True:
   #                     try:
   #                         option_val_translated = translator.translate(option_val, src='ja', dest="ko").text
   #                     except:
   #                         pass
   #                 #self.gp_cur.execute(query, (job_id, mpid, str((option_name.encode().hexdigest())), str((option_val.encode('UTF-8').hexdigest())), list_price, price, stock, stock_status, msg))
   #                 self.gp_cur.execute(query, (job_id, mpid, option_name.encode().hex(
   #                 ), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg, option_name_translated.encode('UTF-8').hex(), option_val_translated.encode('UTF-8').hex()))
   #         self.gp_conn.commit()

   #         # insert data to thumbnail_source_view
   #         # (job_id integer, mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))
   #         query = 'INSERT INTO job_thumbnail_source_view (job_id, mpid, image_url, image_url_sha256)'
   #         query += 'VALUES (%s, %s, %s, %s)'
   #         mpid = str(value['result_for_option']['my_product_id'])
   #         for img_url in value['result_for_thumbnail']['image_urls']:
   #             self.gp_cur.execute(query, (job_id, mpid, str(img_url), str(
   #                 hashlib.sha256(img_url.encode()).hexdigest())))
   #         self.gp_conn.commit()


    def get_mpid_from_mysite_without_up_to_date(self, job_id, max_items):
        try:
            query = ""
            if int(max_items) == -1:
                query = "select mpid from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4 and status != 0".format(job_id)
            else:
                query = "select mpid from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4 and status != 0 limit {}".format(job_id, max_items)
            self.gp_cur.execute(query)
            tmp = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = []
            for i in tmp:
                result.append(i[0])
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_latest_sm_history_id(self, job_id):
        try:
            query = 'select max(id) from sm_history where job_id ={}'.format(
                job_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            row = list(row)[0]
            if row is None:
                return -1
            return row
        except:
            self.gp_conn.rollback()
            raise

    def get_mpid_from_mysite(self, job_id, max_items):
        try:
            query = ""
            if int(max_items) == -1:
                query = "select mpid from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4 ".format(job_id)
            else:
                query = "select mpid from job_source_view where mpid in (select mpid from job_id_and_mpid where job_id = {}) and status != 4 limit {}".format(job_id, max_items)
            self.gp_cur.execute(query)
            tmp = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = []
            for i in tmp:
                result.append(i[0])
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_groupby_key(self, job_id):
        try:
            query = 'select key_name from mysite_compare_key where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            tmp = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = []
            for i in tmp:
                result.append(bytes.fromhex(i[0]).decode())
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    def get_price(self):
        try:
            #query = "select option_value from job_option_source_view where job_id in (select id from jobs where parent_id in (select id from groups where label like '%Zalando%'));"
            query = "select price from job_source_view where job_id in (select id from jobs where parent_id in (select id from groups where label like '%Zalando%'));"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            result = []
            for row in rows:
                price = row[0]
                if is_hex_str(price) == True:
                    price = bytes.fromhex(price).decode()
                result.append(price)
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_options(self):
        try:
            query = "select option_value from job_option_source_view where job_id in (select id from jobs where parent_id in (select id from groups where label like '%Zalando%'));"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = []
            for row in rows:
                op_v = bytes.fromhex(row[0]).decode()
                result.append(op_v)
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_node_properties_from_mysite_for_update(self, job_id, mpid):
        try:
            query = "select column_name from information_schema.columns where table_name = 'job_source_view'"
            self.gp_cur.execute(query)
            col_names_tmp = self.gp_cur.fetchall()
            col_names = []
            for i in col_names_tmp:
                col_names.append(i[0])

            values = ''
            for name in col_names:
                values += str(name) + ', '
            values = values[0:-2]

            query = 'select '+values + \
                ' from job_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            col_values = self.gp_cur.fetchall()
            col_values = col_values[0]

            result = {}
            for i in range(0, len(col_names)):
                #if col_names[i] == 'name':
                #    result[col_names[i]] = bytes.fromhex(col_values[i]).decode()
                #else:
                result[col_names[i]] = col_values[i]

            query = 'select image_url from job_thumbnail_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            result['images'] = []
            for row in rows:
                result['images'].append(row[0])

            query = 'select key, value from job_description_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.pg_cur.fetchall()

            for row in rows:
                if 'sha256' not in row[0]:
                    result[row[0]] = bytes.fromhex(row[1]).decode()

            #query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_option_source_view'";
            # self.gp_cur.execute(query)
            #col_names = self.gp_cur.fetchall()
            # self.gp_conn.commit()

            query = 'select option_name, option_value, stock from job_option_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result['option_name'] = set()
            result['option_value'] = {}
            for row in rows:
                op_n = bytes.fromhex(row[0]).decode()
                op_v = bytes.fromhex(row[1]).decode()
                #op_v_stock = row[2]
                result['option_name'].add(op_n)
                if result['option_value'].get(op_n, None) == None:
                    result['option_value'][op_n] = []
                result['option_value'][op_n].append(op_v)

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_node_properties_from_mysite(self, exec_id, mpid):
        try:
            query = 'select job_id from execution where id = {}'.format(
                exec_id)
            self.gp_cur.execute(query)
            job_id = self.gp_cur.fetchone()[0]

            query = "select column_name from information_schema.columns where table_name = 'job_source_view'"
            self.gp_cur.execute(query)
            col_names_tmp = self.gp_cur.fetchall()
            col_names = []
            for i in col_names_tmp:
                col_names.append(i[0])

            values = ''
            for name in col_names:
                values += str(name) + ', '
            values = values[0:-2]
            query = 'select '+values + \
                ' from job_source_view where mpid = {} and job_id = {}'.format(
                    mpid, job_id)
            self.gp_cur.execute(query)
            col_values = self.gp_cur.fetchall()
            col_values = col_values[0]

            result = {}
            for i in range(0, len(col_names)):
                if col_names[i] == 'name':
                    result[col_names[i]] = bytes.fromhex(
                        col_values[i]).decode()
                else:
                    result[col_names[i]] = col_values[i]

            query = 'select image_url from job_thumbnail_source_view where mpid = {} and job_id = {}'.format(
                mpid, job_id)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            result['images'] = []
            for row in rows:
                result['images'].append(row[0])

            query = 'select key, value from job_description_source_view where mpid = {} and job_id = {}'.format(
                mpid, job_id)
            self.gp_cur.execute(query)
            rows = self.pg_cur.fetchall()

            for row in rows:
                if 'sha256' not in row[0]:
                    result[row[0]] = bytes.fromhex(row[1]).decode()

            #query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_option_source_view'";
            # self.gp_cur.execute(query)
            #col_names = self.gp_cur.fetchall()
            # self.gp_conn.commit()

            query = 'select option_name, option_value, stock from job_option_source_view where mpid = {} and job_id = {}'.format(
                mpid, job_id)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result['option_name'] = set()
            result['option_value'] = {}
            for row in rows:
                op_n = bytes.fromhex(row[0]).decode()
                op_v = bytes.fromhex(row[1]).decode()
                #op_v_stock = row[2]
                result['option_name'].add(op_n)
                if result['option_value'].get(op_n, None) == None:
                    result['option_value'][op_n] = []
                result['option_value'][op_n].append(op_v)

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_node_properties_from_mysite_oldversion(self, exec_id, mpid):
        try:
            query = 'select job_id from execution where id = {}'.format(
                exec_id)
            self.gp_cur.execute(query)
            job_id = self.gp_cur.fetchone()[0]
            self.gp_conn.commit()

            query = "select column_name from information_schema.columns where table_name = 'job" + \
                str(job_id)+"_source_view'"
            self.gp_cur.execute(query)
            col_names_tmp = self.gp_cur.fetchall()
            self.gp_conn.commit()

            query = 'select * from job' + \
                str(job_id)+'_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            col_values = self.gp_cur.fetchall()
            col_values = col_values[0]
            self.gp_conn.commit()
            col_names = []
            for i in col_names_tmp:
                col_names.append(i[0])
            result = {}
            for i in range(0, len(col_names)):
                if col_names[i] == 'name':
                    result[col_names[i]] = bytes.fromhex(
                        col_values[i]).decode()
                else:
                    result[col_names[i]] = col_values[i]

            query = 'select * from job' + \
                str(job_id)+'_thumbnail_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result['images'] = []
            for row in rows:
                result['images'].append(row[1])

            query = 'select * from job' + \
                str(job_id)+'_description_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.pg_cur.fetchall()
            self.pg_conn.commit()

            for row in rows:
                result[row[1]] = bytes.fromhex(row[2]).decode()

            #query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_option_source_view'";
            # self.gp_cur.execute(query)
            #col_names = self.gp_cur.fetchall()
            # self.gp_conn.commit()

            query = 'select * from job' + \
                str(job_id)+'_option_source_view where mpid = {}'.format(mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result['option_name'] = set()
            result['option_value'] = {}
            print_flushed(rows)
            for row in rows:
                print_flushed(row[1])
                op_n = bytes.fromhex(row[1]).decode()
                print_flushed(op_n)
                print_flushed(row[2])
                op_v = bytes.fromhex(row[2]).decode()
                print_flushed(op_v)
                result['option_name'].add(op_n)
                if result['option_value'].get(op_n, None) == None:
                    result['option_value'][op_n] = []
                result['option_value'][op_n].append(op_v)

            print_flushed(result)
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    # get description and product name using mpid
    def get_pname_and_description_using_mpid(self, mpid):
        try:
            query = 'select url from url_to_mpid where my_product_id = {}'.format(
                mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            url = rows[0]
            query = "select node_id from node_property where key = 'url' and value::text like '_" + \
                str(url)+"_'"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            node_id = rows[0]
            query = "select value from node_property where node_id = " + \
                str(node_id)+" and key = 'description'"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            description = rows[0]
            query = "select value from node_property where node_id = " + \
                str(node_id)+" and key = 'name'"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            name = rows[0]
            self.gp_conn.commit()
            return (node_id, name, description)
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    # get url using mpid

    def get_url_using_mpid(self, mpid):
        try:
            query = 'select url from url_to_mpid where my_product_id = {}'.format(
                mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            return rows
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_targetsite_product_id(self, job_id, tpid, mpid, targetsite_url):
        try:
            query = "update tpid_mapping set tpid = " + \
                str(tpid)+" where mpid = {} and job_id = {} and targetsite_url = '{}';".format(
                    mpid, job_id, targetsite_url)
            print_flushed(query)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_targetsite_product_id_oldversion(self, job_id, product_no, mpid):
        try:
            query = "update job_source_view set tpid = " + \
                str(product_no) + \
                " where mpid = {} and job_id = {};".format(mpid, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    # def check_is_first_upload(self, job_id, mpid):
    #  try:
    #    query = "select tpid from job"+str(job_id)+"source_view where mpid = " + str(mpid)+";"
    #    self.gp_cur.execute(query)
    #    row = self.gp_cur.fetchone()
    #    if row is None:
    #      result = True
    #    else row if not None:
    #      result = row[0]
    #    self.gp_conn.commit()
    #    return result
    #  except:
    #    self.gp_conn.rollback()
    #    print_flushed(str(traceback.format_exc()))
    #    raise

    def check_status_of_product(self, job_id, mpid):
        try:
            query = "select status from job_source_view where mpid = {} and job_id = {};".format(
                mpid, job_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            result = row[0]
            self.gp_conn.commit()
            return int(result)
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_node_properties(self, nodeId):
        try:
            query = 'select key, value from node_property where node_id = {}'.format(
                nodeId)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = {}
            for row in rows:
                if row[0] == 'option_list':
                    if row[0] in result:
                        for key, value in row[1].items():
                            result[row[0]][key] = value
                    else:
                        result[row[0]] = row[1]
                elif row[0] == 'option_matrix':
                    if row[0] in result:
                        for key, value in row[1].items():
                            result[row[0]][key] = value
                    else:
                        result[row[0]] = row[1]
                else:
                    result[row[0]] = row[1]
            purl = result['url']

            query_mpid = "select my_product_id from url_to_mpid where url like '{}'".format(
                purl)
            self.gp_cur.execute(query_mpid)
            self.gp_conn.commit()
            rows = self.gp_cur.fetchall()
            if len(rows) == 0:
                query_insert = "insert into url_to_mpid(url) values('{}') returning my_product_id".format(
                    purl)
                self.gp_cur.execute(query_insert)
                self.gp_conn.commit()
                rows = self.gp_cur.fetchall()
                mpid = rows[0][0]
                result['mpid'] = mpid
            else:
                result['mpid'] = rows[0][0]

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_node_properties_oldversion(self, nodeId):
        try:
            query = 'select key, value'
            query += ' from node_property'
            query += ' where node_id = {}'.format(nodeId)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            self.gp_conn.commit()
            result = {}
            print_flushed(rows)
            for row in rows:
                # print_flushed(row[1])
                result[row[0]] = row[1]
            print_flushed(result)
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def change_groupbykey(self, job_id):
        try:
            query = 'select max(id) from execution where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            row = list(row)[0]
            if row is None:
                return -1
            return row

        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_latest_eid_from_job_id(self, job_id):
        try:
            query = 'select max(id) from execution where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            row = list(row)[0]
            if row is None:
                return -1
            return row

        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_max_label_from_eid(self, eid):
        try:
            query = 'select max(n1.label) as max_label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = '+str(eid)+';'
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            self.gp_conn.commit()
            exec_id = row[0]
            return exec_id
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_job_id_from_eid(self, exec_id):
        try:
            query = 'select job_id from execution where id = {}'.format(
                exec_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            self.gp_conn.commit()
            exec_id = row[0]
            return exec_id
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def none_to_blank(self, str):
        if not str or str is None or str == None or str == "None":
            return ''
        else:
            return str

#    def set_status_for_duplicated_data_old(self, job_id):
#        try:
#            query = 'select mpid from job_source_view where id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(
#                job_id)
#            self.gp_cur.execute(query)
#            rows = self.gp_cur.fetchall()
#            mpids = [int(row[0]) for row in rows]
#            mpids_str = '('
#            for mpid in mpids:
#                mpids_str += str(mpid) + ', '
#            mpids_str = mpids_str[0:-2] + ')'
#
#            query = 'select count(*) from job_source_view where id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            self.gp_cur.execute(query)
#            result = self.gp_cur.fetchone()[0]
#
#            query = 'select count(*) from job_source_view where status = 0 and id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            self.gp_cur.execute(query)
#            res0 = self.gp_cur.fetchone()[0]
#
#            query = 'select count(*) from job_source_view where status = 1 and id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            self.gp_cur.execute(query)
#            res1 = self.gp_cur.fetchone()[0]
#
#            query = 'select count(*) from job_source_view where status = 2 and id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            self.gp_cur.execute(query)
#            res2 = self.gp_cur.fetchone()[0]
#
#            query = 'select count(*) from job_source_view where status = 3 and id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            self.gp_cur.execute(query)
#            res3 = self.gp_cur.fetchone()[0]
#
#            #query = 'update job_source_view set status = 4 where id in (select id from job_source_view where job_id = {} and id not in ( select min(id) from job_source_view group by groupby_key_sha256));'.format(job_id)
#            # self.gp_cur.execute(query)
#            # self.gp_conn.commit()
#            if len(rows) != 0:
#                query = "delete from job_thumbnail_source_view where job_id = {} and mpid in ".format(
#                    job_id) + mpids_str
#                #query =  "delete from job_thumbnail_source_view_backup where mpid in " + mpids_str
#                self.gp_cur.execute(query)
#                self.gp_conn.commit()
#
#                query = "delete from job_option_source_view where job_id = {} and mpid in ".format(
#                    job_id) + mpids_str
#                #query =  "delete from job_option_source_view_backup where mpid in " + mpids_str
#                self.gp_cur.execute(query)
#                self.gp_conn.commit()
#
#                query = "delete from job_description_source_view where job_id = {} and mpid in ".format(
#                    job_id) + mpids_str
#                #query =  "delete from job_description_source_view_backup where mpid in " + mpids_str
#                self.gp_cur.execute(query)
#                self.gp_conn.commit()
#
#                query = "delete from job_source_view where job_id = {} and mpid in ".format(
#                    job_id) + mpids_str
#                #query =  "delete from job_source_view_backup where mpid in " + mpids_str
#                self.gp_cur.execute(query)
#                self.gp_conn.commit()
#            return result, res0, res1, res2, res3
#        except:
#            self.gp_conn.rollback()
#            print_flushed(str(traceback.format_exc()))
#            raise

    # (id integer primary key generated always as identity, job_id integer, mpid integer unique, targetsite_url varchar(256), tpid integer, upload_time timestamp);

    def insert_tpid_into_history_table(self, job_id, targetsite_url, mpid, tpid):
        try:
            query = "insert into tpid_history(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{})".format(
                job_id, mpid, targetsite_url, tpid)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_tpid_into_mapping_table(self, job_id, tpid, mpid, targetsite_url):
        try:
            query = "select count(*) from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(
                job_id, targetsite_url, mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            if int(result) == 0:
                self.insert_tpid_into_mapping_table(
                    job_id, targetsite_url, mpid, tpid)
            else:
                query = "update tpid_mapping set tpid = {}, uplodate_time = now() where job_id = {} and targetsite_url = '{}' and mpid = {}".format(tpid,
                                                                                                                                                    job_id, targetsite_url, mpid)
                self.gp_cur.execute(query)
                self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def check_is_item_uploaded(self, job_id, targetsite_url, mpid):
        try:
            query = "select count(*) from tpid_mapping where job_id = {} and targetsite_url like '%{}%' and mpid = {}".format(
                job_id, targetsite_url, mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            if int(result) == 0:
                return False
            else:
                return True
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def insert_tpid_into_mapping_table(self, job_id, targetsite_url, mpid, tpid):
        try:
            query = "insert into tpid_mapping(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{})".format(
                job_id, mpid, targetsite_url, tpid)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_tpid(self, job_id, targetsite_url, mpid):
        try:
            query = "select tpid from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(
                job_id, targetsite_url, mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def insert_sm_history(self, execution_id, start_date, job_id):
        try:
            query = "insert into sm_history(job_id, execution_id, start_time) values({},{},'{}') returning id".format(
                job_id, execution_id, start_date)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_sm_history(self, end_date, input_id):
        try:
            query = "update sm_history set end_time = '{}' where id = {}".format(
                end_date, input_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    def update_current_num_sm_history(self, num, input_id):
        try:
            query = "update sm_history set num_expected_success = {} where id = {}".format(num, input_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    def insert_expected_num_sm_history(self, num, input_id):
        try:
            query = "update sm_history set num_expected_all = {} where id = {}".format(num, input_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    # (id integer primary key generated always as identity, sm_history_id bigint, start_time timestamp, end_time timestamp, targetsite text, job_id integer);
    def insert_mt_history(self, targetsite, start_time, job_id, sm_history_id):
        try:
            query = "insert into mt_history(job_id, targetsite, start_time, sm_history_id) values({},'{}','{}',{}) returning id".format(
                job_id, targetsite, start_time, sm_history_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            rows = self.gp_cur.fetchone()
            result = rows[0]
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_mt_history(self, end_date, input_id):
        try:
            query = "update mt_history set end_time = '{}' where id = {}".format(
                end_date, input_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_num_threads_in_job_configuration_onetime(self, job_id):
        try:
            query = 'select num_thread from targetsite_job_configuration where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            self.gp_conn.commit()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_num_worker_in_job_configuration_onetime(self, tsid):
        try:
            query = 'select num_worker from targetsite_job_configuration where id = {}'.format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            self.gp_conn.commit()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_num_threads_in_job_configuration(self, job_id):
        try:
            query = 'select num_thread from job_configuration where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            self.gp_conn.commit()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_num_worker_in_job_configuration(self, job_id):
        try:
            query = 'select num_worker from job_configuration where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            self.gp_conn.commit()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def update_last_sm_date_in_job_configuration(self, sm_time, job_id):
        try:
            query = "update job_configuration set last_sm_date = '{}' where job_id = {}".format(
                sm_time, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_targetsite(self, target_id):
        try:
            query = 'select targetsite_url, targetsite_id from targetsite_job_configuration where id = {}'.format(
                target_id)
            self.gp_cur.execute(query)
            res = self.gp_cur.fetchone()
            url = bytes.fromhex(res[0]).decode()
            tid = res[1]

            query = 'select gateway from targetsite where id = {}'.format(tid)
            self.gp_cur.execute(query)
            res = self.gp_cur.fetchone()

            gateway = bytes.fromhex(res[0]).decode()

            return url, gateway
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_targetsiteOld(self, job_id):
        try:
            query = 'select targetsite_id from job_configuration where job_id = {}'.format(
                job_id)
            self.gp_cur.execute(query)
            res = self.gp_cur.fetchone()[0]

            query = 'select url, gateway from targetsite where id = {}'.format(
                res)
            self.gp_cur.execute(query)
            res = self.gp_cur.fetchone()

            url = bytes.fromhex(res[0]).decode()
            gateway = bytes.fromhex(res[1]).decode()

            return url, gateway
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_site_code_from_job_id(self, job_id):
        try:

            query = "select site_code from job_id_to_site_code where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            site_code = result[0]

            return site_code
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_mpid_in_job_source_view_using_status(self, status):
        try:
            query = "select mpid from job_source_view_backup where status = {}".format(
                status)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchall()
            self.gp_conn.commit()
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_shipping_prd_mpid_using_stage_id(self, stage_id):
        try:
            query = "select distinct(value::text) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = {})) and key = 'stock' and value::text like '%Ship%');".format(stage_id)
            #query = "select distinct(value::text) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = 2948 or stage_id = 2951 or stage_id = 2954 or stage_id = 2957 or stage_id = 2960 or stage_id = 2963 or stage_id = 2966 or stage_id = 2969 or stage_id = 2972 or stage_id = 2975 or stage_id = 2978)) and key = 'stock' and value::text like '%Ship%');"
            #query = "select my_product_id from url_to_mpid where cast(url as varchar(2048)) in (select cast(value::text as varchar(2048)) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = {})) and key = 'stock' and value::text like '%Ship%'));".format(stage_id)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            url = []
            for i in rows:
                url.append(i[0].replace('"', "'"))

            query = "select my_product_id from url_to_mpid where url in ("
            for val in url:
                query += val+", "
            query = query[:-3] + "');"

            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            mpids = []
            for i in rows:
                mpids.append(i[0])
            return mpids
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_cnum_from_job_configuration(self, job_id):
        try:
            query = "select cnum from job_configuration where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_max_items_from_tsid(self, tsid):
        try:
            query = "select max_num_items from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_cnum_from_targetsite_job_configuration_using_tsid(self, tsid):
        try:
            query = "select cnum from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def add_worker(self, ip, port):
        try:
            query = "insert into registered_workers(ip, port) values('{}', {}) returning id".format(
                ip, port)
            self.gp_cur.execute(query)
            wid = self.gp_cur.fetchone()[0]
            self.gp_conn.commit()
            return wid
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def delete_worker(self, worker_name):
        try:
            query = "delete from registered_workers where id = '{}'".format(
                worker_name.split('-')[1])
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_workers(self):
        try:
            query = "select id, ip, port from registered_workers"
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchall()
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_workers_ip_and_port(self, worker_name):
        try:
            query = "select ip, port from registered_workers where id = '{}'".format(
                worker_name.split('-')[1])
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_targetsite_id_using_job_id(self, job_id):
        try:
            query = 'select id from targetsite_job_configuration where job_id = {} and checked = True order by id'.format(
                job_id)
            self.gp_cur.execute(query)
            results = self.gp_cur.fetchall()
            final_results = []
            for res in results:
                final_results.append(res[0])
            return final_results
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_job_configuration(self, job_id):
        try:
            query = "select cnum from job_configuration where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_targetsite_label_encode(self, tsid):
        try:
            query = "select targetsite_label from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            #result = bytes.fromhex(result[0]).decode()

            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_targetsite_label(self, tsid):
        try:
            query = "select targetsite_label from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            result = bytes.fromhex(result[0]).decode()

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_selected_gateway_configuration_program_onetime(self, tsid):
        try:
            query = "select cid from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            cid = result[0]

            query = "select configuration from gateway_configuration where id = {}".format(
                cid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            #result = bytes.fromhex(result[0]).decode()

            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_selected_gateway_configuration_program(self, job_id):
        try:
            query = "select cid  from job_configuration where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            cid = result[0]

            query = "select configuration from gateway_configuration where id = {}".format(
                cid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            #result = bytes.fromhex(result[0]).decode()

            return result[0]
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_selected_transformation_program_onetime(self, target_id):
        try:
            query = "select transformation_program_id  from targetsite_job_configuration where id = {}".format(
                target_id)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            tpid = result[0]

            query = "select transformation_program from transformation_program where id = {}".format(
                tpid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            result = bytes.fromhex(result[0]).decode()

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_ransformation_program(self, tpid):
        try:
            query = "select transformation_program from transformation_program where id = {}".format(
                tpid)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()
            result = bytes.fromhex(result[0]).decode()

            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_shipping_fee(self, delivery_company):
        try:
            query = "select id from delivery_companies where name like '%{}%'".format(
                delivery_company)
            self.gp_cur.execute(query)
            dcid = self.gp_cur.fetchone()[0]

            query = "select min_kg, max_kg, fee from shipping_fee where delivery_company_id = {}".format(
                dcid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()

            delivery_charge_list = [
                [float(row[0]), float(row[1]), float(row[2])] for row in rows]
            self.gp_conn.commit()

            return delivery_charge_list
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_pricing_information_onetime(self, tsid):
        try:
            # id | job_id | targetsite_id |     targetsite_label     |                         targetsite_url                         |        t_category        | transformation_program_id | cid | cnum |   exchange_rate    | tariff_rate | vat_rate | tariff_threshold | margin_rate | min_margin | delivery_company | shipping_cost
            query = "select * from targetsite_job_configuration where id = {}".format(
                tsid)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = {}
            result['exchange_rate'] = row[9]
            result['tariff_rate'] = row[10]
            result['vat_rate'] = row[11]
            result['tariff_threshold'] = row[12]
            result['margin_rate'] = row[13]
            result['min_margin'] = row[14]
            result['delivery_company'] = row[15]
            result['shipping_cost'] = row[16]
            query = "select exchange_rate from exchange_rate order by id desc limit 1"
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()[0]
            result['dollar2krw'] = row['USD']
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_pricing_information(self, job_id):
        try:
            #id | job_id | targetsite_id |      category       |   exchange_rate    | tariff_rate | vat_rate | tariff_threshold | margin_rate | min_margin | delivery_company | shipping_cost
            query = "select * from pricing_information where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = {}
            result['exchange_rate'] = row[4]
            result['tariff_rate'] = row[5]
            result['vat_rate'] = row[6]
            result['tariff_threshold'] = row[7]
            result['margin_rate'] = row[8]
            result['min_margin'] = row[9]
            result['shipping_cost'] = row[11]
            result['delivery_company'] = row[10]
            query = "select exchange_rate from exchange_rate order by id desc limit 1"
            self.gp_cur.execute(query)
            row = self.gp_cur.fetchone()[0]
            result['dollar2krw'] = row['USD']
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def logging_all_uploaded_product(self, job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum):
        try:
            #query =  "insert into all_uploaded_product(job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum) values({}, {}, {}, '{}', '{}','{}',{})".format(job_id, execution_id, mpid,  json.dumps(origin_product, default=self.json_default), json.dumps(converted_product,default=self.json_default ), targetsite_url, cnum)
            query = "insert into all_uploaded_product(job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum) values({}, {}, {}, '{}', '{}','{}',{})".format(
                job_id, execution_id, mpid,  json.dumps({}), json.dumps({}), targetsite_url, cnum)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def get_uploaded_product(self, job_id):
        try:
            query = "select mpid, origin_product, converted_product from all_uploaded_product where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            results = self.gp_cur.fetchall()  # [[mpid,dsfsdf]]
            # print_flushed(result[1][1:-1])
            # origin_product = json.dumps(origin_product).encode('UTF-8').hex()
            for idx, result in enumerate(results):
                result = list(result)
                result[1] = json.loads(bytes.fromhex(result[1][1:-1]).decode())
                for key in ['num_options', 'num_images', 'id', 'm_category', 'tpid', 'groupby_key_sha256', 'source_site_product_id', 'url_sha256', 'name_sha256', 'html', 'smpid', 'cnum', 'shipping_fee', 'shipping_cost', 'brand', 'shipping_weight', 'weight', 'shpping_price']:
                    try:
                        result[1].pop(key)
                    except:
                        pass
                results[idx] = tuple(result)
                result[2] = json.loads(bytes.fromhex(result[2][1:-1]).decode())
                for key in result[2].keys():
                    if key not in ['product_name', 'targetsite_url']:
                        try:
                            result[2].pop(key)
                        except:
                            pass
                results[idx] = tuple(result)
                print_flushed(result[2].keys())
            # print_flushed(res.keys())
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def create_row_job_current_working(self, job_id):
        try:
            query = "insert into job_current_working(job_id) values({})".format(
                job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def log_err_msg_of_task(self, task_id, err_msg):
        try:
            err_msg = err_msg.replace("'", '"')
            query = "insert into failed_task_detail(task_id, err_msg) values({},'{}')".format(
                task_id, err_msg)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    # failed_my_site_detail (id integer primary key generated always as identity, sm_history_id integer, err_msg text);
    def log_err_msg_of_my_site(self, sm_history_id, mpid, err_msg, node_id = 0):
        try:
            err_msg = err_msg.replace("'", '"')
            query = "insert into failed_my_site_detail(sm_history_id, mpid, err_msg, node_id) values({}, {}, '{}', {})".format(sm_history_id, mpid, err_msg, node_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def log_err_msg_of_target_site(self, task_id, err_msg):
        try:
            query = "insert into failed_task_detail(task_id, err_msg) values({},'{}')".format(
                task_id, err_msg)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def log_to_job_current_targetsite_working(self, log, job_id):
        try:
            query = "update job_current_working set targetsite_working = targetsite_working || '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise


    def re_log_to_job_current_targetsite_working(self, log, job_id):
        try:
            query = "update job_current_working set targetsite_working = '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def log_to_job_current_mysite_working(self, log, job_id):
        try:
            #query =  "update job_current_working set mysite_working = '{}' where job_id = {}".format(log, job_id)
            query = "update job_current_working set mysite_working = mysite_working || '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def re_log_to_job_current_mysite_working(self, log, job_id):
        try:
            query = "update job_current_working set mysite_working = '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def re_log_to_job_current_crawling_working(self, log, job_id):
        try:
            query = "update job_current_working set crawling_working = '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def log_to_job_current_crawling_working(self, log, job_id):
        try:
            query = "update job_current_working set crawling_working = crawling_working || '{}' where job_id = {}".format(
                log, job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def check_exist_in_job_source_view(self, mpid):
        try:
            query = "select count(*) from job_source_view where mpid = {} and status != 3 and status != 4".format(mpid)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            return result
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def delete_stock_zero_in_mysite(self, job_id):
        try:
            #query =  "select mpid from job_source_view where stock = '0' and job_id = {}".format(job_id)
            query = "select mpid from job_source_view where (stock = '0' or status = 3)and job_id = {} and mpid not in (select mpid from tpid_mapping) ".format(
                job_id)
            #query =  "select mpid from job_source_view_backup where stock = '0'"
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchall()
            deleted_mpids = [int(row[0]) for row in rows]
            print_flushed("# of deleted (except uploaded items): ",
                          len(deleted_mpids))

            if len(deleted_mpids) != 0:
                mpids_str = '('
                for mpid in deleted_mpids:
                    mpids_str += str(mpid) + ', '
                mpids_str = mpids_str[0:-2] + ')'

                query = "delete from job_thumbnail_source_view where mpid in " + mpids_str
                #query =  "delete from job_thumbnail_source_view_backup where mpid in " + mpids_str
                self.gp_cur.execute(query)
                self.gp_conn.commit()

                query = "delete from job_option_source_view where mpid in " + mpids_str
                #query =  "delete from job_option_source_view_backup where mpid in " + mpids_str
                self.gp_cur.execute(query)
                self.gp_conn.commit()

                query = "delete from job_description_source_view where mpid in " + mpids_str
                #query =  "delete from job_description_source_view_backup where mpid in " + mpids_str
                self.gp_cur.execute(query)
                self.gp_conn.commit()

                query = "delete from job_source_view where mpid in " + mpids_str
                #query =  "delete from job_source_view_backup where mpid in " + mpids_str
                self.gp_cur.execute(query)
                self.gp_conn.commit()
                return
        except:
            self.gp_conn.rollback()
            raise

    def add_is_error(self, job_id):
        try:
            query = "select count(*) from check_is_error where job_id = {}".format(job_id)
            self.gp_cur.execute(query)
            rows = self.gp_cur.fetchone()
            self.gp_conn.commit()
            result = rows[0]
            query = ""
            if int(result) == 0:
                query = "insert into check_is_error(job_id, is_error) values({}, -1)".format(
                    job_id)
            else:
                query = "update check_is_error set is_error = -1 where job_id = {}".format(
                    job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return

        except:
            self.gp_conn.rollback()
            raise

    def reset_is_error(self, job_id):
        try:
            query = "update check_is_error set is_error = -1 where job_id = {}".format(
                job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            raise

    def update_is_error(self, job_id):
        try:

            query = "update check_is_error set is_error = 1 where job_id = {} and is_error = -1".format(
                job_id)
            self.gp_cur.execute(query)
            self.gp_conn.commit()

            return
        except:
            self.gp_conn.rollback()
            raise

    def update_last_mt_date_in_job_configuration(self, mt_time, job_id, tsid):
        try:
            query = "update job_configuration set last_mt_date = '{}' where job_id = {} and tsid = {}".format(
                mt_time, job_id, tsid)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise

    def check_string_is_int(self, s):
        try:
            if s is None or s == 'null':
                return False
            int(s)
            return True
        except ValueError:
            return False

    def get_zipcode(self, url):
        try:
            src_url = urlparse(url).netloc
            query = "select zipcode from url_and_zipcode where url_to_zipcode where url = '{}';".format(
                src_url)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()[0]
            return result
        except Exception as e:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def update_zipcode(self, url, zipcode):
        try:
            src_url = urlparse(url).netloc
            query = "select count(zipcode) from url_and_zipcode where url_to_zipcode where url = '{}';".format(
                url)
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchone()[0]
            if int(result) == 0:
                query = "insert into url_and_zipcode(url,zipcode) values('{}','{}')".format(
                    src_url, zipcode)
            else:
                query = "update url_and_zipcode set zipcode = '{}' where url = '{}'".format(
                    zipcode, src_url)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except Exception as e:
            self.gp_conn.rollback()
            print_flushed(str(traceback.format_exc()))
            raise e

    def test_string(self, input_str):
        query = ""
        try:
            query = "insert into test_char(string) values('{}')".format(
                input_str)
            self.gp_cur.execute(query)
            self.gp_conn.commit()
            return
        except Exception as e:
            self.gp_conn.rollback()
            print_flushed(query)
            print_flushed(str(traceback.format_exc()))
            raise e


    def get_mysite_column_name(self):
        try:
            query = "select column_name from information_schema.columns where table_name = 'job_source_view' order by column_name"
            self.gp_cur.execute(query)
            result = self.gp_cur.fetchall()
            col_names = []
            for i in result:
                col_names.append(i[0])

            return col_names
        except:
            conn.rollback()
            return { "success": False }



    # for jomashop

    def check_stock_for_jomashop(self, input_dictionary):
        try:
            stock = '0'
            if 'stock' in input_dictionary:
                if input_dictionary['stock'] is None:
                    stock = '0'
                else:
                    if self.check_string_is_int(input_dictionary['stock']):
                        stock = str(input_dictionary['stock'])
                    else:
                        if 'in stock' in str(input_dictionary['stock']).lower():
                            stock = '999'
                        else:
                            stock = '0'

            return stock
        except:
            print_flushed(str(traceback.format_exc()))
            raise

    def json_default(self, value):
        if isinstance(value, date):
            return str(value.strftime('%Y-%m-%d %H:%M:%S'))
        elif isinstance(value, set):
            return ''
        print_flushed(value.replace("'", "\'"))
        return value.replace("'", "\'")
        raise TypeError('not JSON serializable')

    def check_stock_old(self, input_dictionary):
        try:
            stock = '0'
            if 'stock' in input_dictionary:
                if input_dictionary['stock'] is None:
                    if 'out_of_stock' in input_dictionary:
                        # stock is None & out of stock is None => 999
                        if input_dictionary['out_of_stock'] is None:
                            stock = '999'
                        # stock is None & out of stock is not None => 0
                        else:
                            stock = '0'
                    # stock is None & out of stock does not crawled => 999
                    else:
                        stock = '999'
                else:
                    # stock is not None & stock is integer => use that value
                    if self.check_string_is_int(input_dictionary['stock']):
                        stock = str(input_dictionary['stock'])
                    # stock is not None & stock is 'in stock' => 999
                    else:
                        if 'in stock' in str(input_dictionary['stock']).lower():
                            stock = '999'
                        # stock is not None & stock is other string => 0
                        else:
                            stock = '0'
            else:
                if 'out_of_stock' in input_dictionary:
                    # stock does not crawled & out of stock is None => 999
                    if input_dictionary['out_of_stock'] is None:
                        stock = '999'
                    # stock does not crawled & out of stock is not None => 0
                    else:
                        stock = '0'
                # stock does not crawled & out of stock does not crawled => 999
                else:
                    stock = '999'
            return stock
        except:
            print_flushed(str(traceback.format_exc()))
            raise

    def check_stock(self, input_dictionary):
        log_input_dictionary = ""
        try:
            stock = '999'
            if 'stock' in input_dictionary:
                if type(input_dictionary) == str:
                    #input_dictionary = input_dictionary.replace("'",'"')
                    cleaner = re.compile("':")
                    input_dictionary = re.sub(cleaner, '":', input_dictionary)
                    cleaner = re.compile("{'")
                    input_dictionary = re.sub(cleaner, '{"', input_dictionary)
                    cleaner = re.compile(" '")
                    input_dictionary = re.sub(cleaner, ' "', input_dictionary)
                    cleaner = re.compile("',")
                    input_dictionary = re.sub(cleaner, '",', input_dictionary)
                    cleaner = re.compile("'")
                    input_dictionary = re.sub(cleaner, '', input_dictionary)
                    input_dictionary = input_dictionary.replace('\\xa0','')
                    cleaner = re.compile("None")
                    input_dictionary = re.sub(
                        cleaner, 'null', input_dictionary)
                    log_input_dictionary = input_dictionary
                    input_dictionary = json.loads(input_dictionary)

                if self.check_string_is_int(input_dictionary['stock']):
                    stock = str(input_dictionary['stock'])
                else:
                    # stock value is "in stock" or None (there is no stock information in source site
                    if 'in stock' in str(input_dictionary['stock']).lower() or input_dictionary['stock'] is None:
                        stock = '999'
                    else:
                        print(input_dictionary)
                        stock = '0'
            return stock
        except:
            print_flushed(log_input_dictionary)
            print_flushed(str(traceback.format_exc()))
            raise
