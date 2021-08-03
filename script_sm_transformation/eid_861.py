import datetime
from datetime import timedelta

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         utcnow = datetime.datetime.utcnow()
         time_gap = datetime.timedelta(hours=9)
         kor_time = utcnow + time_gap
       
         result = {} 
         ### jobN_option_source_view
         #my_product_id integer default nextval(‘my_product_id’), status integer, c_date timestamp, sm_date timestamp,  url varchar(2048), p_name varchar(2048), sku varchar(2048), list_price float, price float, origin varchar(2048), company varchar(2048), image_url varchar(2048), num_options integer, num_images integer
         result_for_source_view = {}
         result_for_source_view['my_product_id'] = node_properties['mpid'] 
         result_for_source_view['status'] = 2
         result_for_source_view['c_date'] = datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S")
         result_for_source_view['sm_date'] = datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S")
         result_for_source_view['url'] = node_properties.get('url', 'no url') 
         result_for_source_view['p_name'] = node_properties['name']
         result_for_source_view['sku'] = None
         result_for_source_view['list_price'] = node_properties['price'] 
         result_for_source_view['price'] = node_properties['price']
         result_for_source_view['origin'] = None
         node_properties['brand'] = 'Taylor Made'
         result_for_source_view['company'] = node_properties['brand']
         result_for_source_view['image_url'] = node_properties['images'][0]
         num_option = 0
         if None != node_properties.get('option1_name', None):
            num_option++
         if None != node_properties.get('option2_name', None):
            num_option++
         result['num_options'] = num_option
         result['num_images'] = len(node_properties['images'])



         ### jobN_thumbnail_source_view
         result_for_thumbnail = {}
         result_for_thumbnail['my_product_id'] = node_properties['mpid']
         result_for_thumbnail['image_urls'] = node_properties['images']


         ### jobN_option_source_view
         # my_product_id integer, option_name varchar(2048), option_value varchar(2048), list_price float, integer float, stock integer, stock_status integer, msg varchar(2048) 
         result_for_option = {}
         result_for_option['my_product_id'] = node_properties['mpid'] 
         result_for_option['option_names'] = []  
         result_for_option['option_values'] = {} 
         if None != node_properties.get('option1_name', None):
            result_for_option['option_names'].append(node_properties.get('option1_name', None))
            result_for_option['option_values'][node_properties.get('option1_name', None)] = node_properties['option1']
         if None != node_properties.get('option2_name', None):
            result_for_option['option_names'].append(node_properties.get('option2_name', None))
            result_for_option['option_values'][node_properties.get('option2_name', None)] = node_properties['option2']
         result_for_option['list_price'] = node_properties['price'] 
         result_for_option['price'] =  node_properties['price'] 
         result_for_option['stock'] = 999 
         result_for_option['stock_status'] = None
         result_for_option['msg'] = None


         ### jobN_description_source_view
         # create table if not exists jobN_description_source_view (my_product_id integer, key varchar(2048), value varchar(2048))
         result_for_desc = {}
         result_for_desc['my_product_id'] = node_properties['mpid']
         result_for_desc['key'] = []
         result_for_desc['value'] = {}
         if None != node_properties['description']:
            result_for_desc['key'].append('desc')
            result_for_desc['value']['desc'] = node_properties['description']
         if None != node_properties['weight']:
            result_for_desc['key'].append('weight')
            result_for_desc['value']['weight'] = node_properties['weight']
         if None != node_properties['shipping_price']:
            result_for_desc['key'].append('shipping_price')
            result_for_desc['value']['shipping_price'] = node_properties['shipping_price']

         result['result_for_source_view'] = result_for_source_view 
         result['result_for_thumbnail'] = result_for_thumbnail 
         result['result_for_option'] = result_for_option
         result['result_for_desc'] = result_for_desc
     except:
         print(node_properties['url'])
         raise
     return result

