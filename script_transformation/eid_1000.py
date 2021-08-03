import csv
import datetime
from datetime import timedelta
import ast

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         node_properties['INFORMATION'] = ast.literal_eval(node_properties['INFORMATION'])
         node_properties['ADDITIONAL INFO'] =ast.literal_eval(node_properties['ADDITIONAL INFO'])
         node_properties['CASE'] =ast.literal_eval(node_properties['CASE'])
         node_properties['BAND'] =ast.literal_eval(node_properties['BAND'])
         node_properties['DIAL'] =ast.literal_eval(node_properties['DIAL'])
         node_properties['FEATURES'] =ast.literal_eval(node_properties['FEATURES'])
         result = {}
         result['display'] = 'T'
         site_code = '1017'
         if 'Brand :' not in node_properties['INFORMATION'] and 'Brand  :' in node_properties['INFORMATION']:
             node_properties['INFORMATION']['Brand :'] = node_properties['INFORMATION']['Brand  :']

         result['product_name'] ="["+  node_properties['INFORMATION']['Brand :']+"] "+ node_properties['p_name']
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.1;
         dollar2krw = 1290;
         yen2krw = 11.92
         margin_rate = 0.15
         #minimum_margin = 5000.0;
         lowest_price = -1;
         category_num = 109;

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_us.csv', 'r') as f:
             reader = csv.reader(f, delimiter=',')
             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]
         
         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))


         weight = 5.0## 동일하게 상품별로 달라질수 있음
         print("1.1 weight(kg)=["+str(weight)+"]")
         #delivery_charge = 35834.0 # 디폴트 배송비, 상품별로 달라질 수 있음. 이건 좀 코드가 이상한데 원래는 웨이트를 정하고 그 웨이트로부터 테이블에서 배송비를 얻어야 하는데
                                     # 하드코딩한 걸로 보임 (5kg의 배송비는 테이블을 보면  35834.0 임
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge = charge[-1]
                 break
         print("2. delivery_charge=["+str(delivery_charge)+"]")
         price = node_properties['price']
         print("3. price=["+str(price)+"]")

         shipping_price = node_properties['shipping_price']
         if 'Free' in str(shipping_price):
            shipping_price = 0
         else:
            shipping_price = Price.fromstring(node_properties['shipping_price']).amount_float
         print("3.1 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != 'None':
            amount = amount + shipping_price
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.8 if amount + delivery_charge/dollar2krw>= 200 else 0;
         taxRate = 0.1 if amount + delivery_charge/dollar2krw>= 200 else 0;
         supplyPrice = dollar2krw * amount + delivery_charge
         print("supply price= "+str(supplyPrice)+"")

         retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")

         if retailPriceWithoutMargin <= 700000:
             margin = dollar2krw * amount * 0.15
             if margin < 50000:
                margin = 50000
         else:
             margin = dollar2krw * amount * 0.1
             if margin < 50000:
                margin = 50000

         print("margin = "+str(margin)+"")
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")
         buf = 5000

         result['price'] = str(round(retailPrice,-2))
         print("4. supply_price = ["+ str(result['price'])+"]")
         result['supply_price'] = result['price']

         print("5. brand_code = [" + str( node_properties['INFORMATION']['Brand :']) + "]")
         result['brand_code'] = node_properties['INFORMATION']['Brand :']
         print("6. images")
         print(node_properties['images'])
         
         if len(node_properties['images']) >= 2:
            result['detail_image'] = node_properties['images'][0]
            result['additional_image'] = node_properties['images'][1:]
         elif len(node_properties['images']) == 1:
            result['detail_image'] = node_properties['images'][0]
         elif len(node_properties['images']) == 0:
            result['detail_image'] = node_properties['front_image']
            node_properties['images'] = []
            node_properties['images'].append(node_properties['front_image'])

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")

         option_names = node_properties.get('option_name', [])
         option_values = node_properties.get('option_value', {})

         variants = []
         if len(option_names) == 0:
            result['has_option'] = 'F'
         else:
            result['has_option'] = 'T'
            result['option_names'] = []
            for op_n in option_names:
               variant = {}
               result['option_names'].append(op_n)
               variant[op_n] = []
               for op_v in option_values[op_n]:
                  variant[op_n].append(op_v)
               variants.append(variant)
         result['variants'] = variants 


         #stock = node_properties.get('stock', None)
         result['stock'] = node_properties['stock']
         print("8. stock=["+ str(node_properties['stock']) +"]")

         
         result['mpid'] = node_properties['mpid']
         node_properties['mpid'] = site_code+str(node_properties['mpid']).zfill(7)
         print("9. custom_product_code=["+str(node_properties['mpid'])+"]")
         result['custom_product_code'] = node_properties['mpid']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] 
         description_title = '<div><div align="center" style="font-size:14pt;font-weight:bold">{}</div><br><br></div><br>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
            description_images += '<br><br><img style="width: 100%" src=\"{}\"></img>'.format(image)
         description_images = '<br>' + description_images + '</center>'


         description_detail = "<br>INFORMATION"
         description_detail += "<br>"
         for key in node_properties['INFORMATION']:
            description_detail += str(key) + "  " + node_properties['INFORMATION'][key] + "<br>"

         description_detail += "<br>"
         description_detail += "CASE"
         description_detail += "<br>"
         for key in node_properties['CASE']:
            description_detail += str(key) + " " + node_properties['CASE'][key] + "<br>"

         description_detail += "<br>"
         description_detail += "DIAL"
         description_detail += "<br>"
         for key in node_properties['DIAL']:
            description_detail += str(key) + " " + node_properties['DIAL'][key] + "<br>"

         description_detail += "<br>"
         description_detail += "FEATURES"
         description_detail += "<br>"
         for key in node_properties['FEATURES']:
            description_detail += str(key) + " " + node_properties['FEATURES'][key] + "<br>"

         description_detail += "<br>"
         description_detail += "BAND"
         description_detail += "<br>"
         for key in node_properties['BAND']:
            description_detail += str(key) + " " + node_properties['BAND'][key] + "<br>"


         description_detail += "<br>"
         description_detail += "ADDITIONAL INFO"
         description_detail += "<br>"
         for key in node_properties['ADDITIONAL INFO']:
            description_detail += str(key) + " " + node_properties['ADDITIONAL INFO'][key] + "<br>"


         description_content = "<div style='font-size:12pt; line-height:200%'>"
         if node_properties['description'] is not None:
            description_content += node_properties['description'] + "<br>"
            utcnow = datetime.datetime.utcnow()
            time_gap = datetime.timedelta(hours=9)
            kor_time = utcnow + time_gap
            description_content += "<br>{}<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(description_detail, node_properties['mpid'], datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
         else:
            description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
         result['description'] = description_title + description_content
     except:
         print(node_properties)
         raise
     return result

def escape( str ):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\"")
    return str


