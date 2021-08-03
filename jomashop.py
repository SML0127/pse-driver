import csv
import datetime
from datetime import timedelta
import ast

def user_defined_export(graph_mgr, node_id, node_properties):
     try:



         result = {}
         result['display'] = 'T'  #store
         site_code = node_properties["smpid"]

         

         result['product_name'] ="["+  result['brand'] +"] "+ node_properties['name']
         print("product=["+str(result['product_name'])+"]");
         result['manufacturer'] = result['brand']  #store
         result['manufacturer_code'] = result['manufacturer']  #store


         ############## New one
         pricing_information = node_properties["pricing_information"]

         dollar2krw = float(pricing_information["exchange_rate"])  #store
         minimum_margin = float(pricing_information["min_margin"])  #store
         margin_rate = float(pricing_information["margin_rate"])  #store

         tariff_rate = float(pricing_information["tariff_rate"])  #store
         vat_rate = float(pricing_information["vat_rate"])  #store
         #shipping_cost = pricing_information["shipping_cost"]
         tariff_threshold = float(pricing_information["tariff_threshold"])  #store
         delivery_charge_list = node_properties['shipping_fee']

         lowest_price = -1;
         category_num = node_properties["cnum"];

         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

         #weight = 1.5## 동일하게 상품별로 달라질수 있음
         weight = float(pricing_information["default_weight"])   #store
         print("1.1 weight(kg)=["+str(weight)+"]")
         result['pse_weight'] = str(weight)+""  #pse_store
         #delivery_charge = 35834.0 # 디폴트 배송비, 상품별로 달라질 수 있음. 이건 좀 코드가 이상한데 원래는 웨이트를 정하고 그 웨이트로부터 테이블에서 배송비를 얻어야 하는데
         delivery_charge_krw = 0                          # 하드코딩한 걸로 보임 (5kg의 배송비는 테이블을 보면  35834.0 임
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge_krw = charge[-1]
                 break
         print("2. delivery_charge=["+str(delivery_charge_krw)+"]")    #store
         result['pse_delivery_charge_krw'] = str(delivery_charge_krw)+""  #pse_store
         price = node_properties["price"]
         print("3. price=["+str(price)+"]")

         shipping_price = node_properties["shipping_price"]
         print("3.0 shipping price=["+str(shipping_price)+"]");
         if "Free" in str(shipping_price):
            shipping_price = 0
         else:
            shipping_price = Price.fromstring(node_properties["shipping_price"]).amount_float
         print("3.1 shipping price=["+str(shipping_price)+"]");
         result['pse_shipping_price'] = str(shipping_price)+""  #pse_store

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != "None":
            amount = amount + shipping_price
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = tariff_rate if (amount + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         taxRate = vat_rate if (amount + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         result['pse_tariffRate'] = str(tariffRate)+""  #pse_store
         result['pse_taxRate'] = str(taxRate)+""  #pse_store
         
         supplyPrice = dollar2krw * amount + delivery_charge_krw    #store
         print("supply price= "+str(supplyPrice)+"")
         result['pse_supplyPrice'] = str(supplyPrice)+""  #pse_store
          


         #tariffRate = tariff_rate if amount + delivery_charge/dollar2krw>= int(tariff_threshold) else 0;
         #taxRate = vat_rate if amount + delivery_charge/dollar2krw>= int(tariff_threshold) else 0;
         #supplyPrice = dollar2krw * amount + delivery_charge
         #print("supply price= "+str(supplyPrice)+"")
         #tariffRate: kwanse
         #taxRate: bukase
         #modified by lee
         supplyPriceWithtariff = (supplyPrice) * (1 + tariffRate)
         print("supplyPriceWithtariff = "+str(supplyPriceWithtariff)+"")    #store
         result['pse_supplyPriceWithtariff'] = str(supplyPriceWithtariff)+""  #pse_store
         
         individualtariff = (supplyPriceWithtariff-2000000)*0.2 if supplyPriceWithtariff > 2000000 else 0;
         print("individualtariff = "+str(individualtariff)+"")    #store
         result['pse_individualtariff'] = str(individualtariff)+""  #pse_store
         
         edutariff = individualtariff * 0.3
         print("edutariff = "+str(edutariff)+"")    #store
         result['pse_edutariff'] = str(edutariff)+""  #pse_store
         
         retailPriceWithoutMargin = (supplyPriceWithtariff + individualtariff + edutariff) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")    #store
         result['pse_retailPriceWithoutMargin'] = str(retailPriceWithoutMargin)+""  #pse_store
         

         if retailPriceWithoutMargin <= 700000:
             margin = dollar2krw * amount * 0.15
             if margin < minimum_margin:
                margin = minimum_margin
         else:
             margin = dollar2krw * amount * 0.15  
             if margin < minimum_margin:
                margin = minimum_margin
        # change 0.1 -> 0.15 by wshan request (2021.3.18)  

         print("margin = "+str(margin)+"")    #store
         result['pse_margin'] = str(margin)+""  #pse_store
         
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")    #store
         result['pse_retailPrice'] = str(retailPrice)+""  #pse_store
         
         buf = 5000

         result["price"] = str(round(retailPrice,-2))
         print("4. supply_price = ["+ str(result["price"])+"]")
         result["supply_price"] = result["price"]

         print("5. brand_code = [" + str( result['brand'] ) + "]")
         result['brand_code'] = result['brand']


         print("6. images")
         print(node_properties["images"])

         if len(node_properties["images"]) >= 2:
            result["detail_image"] = node_properties["images"][0]
            result["additional_image"] = node_properties["images"][1:]
         elif len(node_properties["images"]) == 1:
            result["detail_image"] = node_properties["images"][0]


         result["selling"] = "T"
         result["memo"] = node_properties.get("url", "no url")
         print("7. url=["+str(result["memo"])+"]")

         option_names = node_properties.get("option_name", [])
         option_values = node_properties.get("option_value", {})

         variants = []
         if len(option_names) == 0:
            result["has_option"] = "F"
         else:
            result["has_option"] = "T"
            result["option_names"] = []
            for op_n in option_names:
               variant = {}
               result["option_names"].append(op_n)
               variant[op_n] = []
               for op_v in option_values[op_n]:
                  variant[op_n].append(op_v)
               variants.append(variant)
         result["variants"] = variants


         result["stock"] = node_properties["stock"]
         print("8. stock=["+ str(node_properties["stock"]) +"]")


         result["mpid"] = node_properties["mpid"]
         node_properties["mpid"] = str(site_code) + str(node_properties["mpid"]).zfill(7)
         print("9. custom_product_code=["+str(node_properties["mpid"])+"]")
         result['custom_product_code'] = node_properties['mpid']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}]
         description_title = '<div><div align="center" style="font-size:14pt;font-weight:bold">{}</div><br><br></div><br>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
            description_images += '<br><br><img style="width: 100%" src=\"{}\"></img>'.format(image)
         description_images = '<br>' + description_images + '</center>'

         description_detail = ""
         if node_properties['Dictionary1'].get('dictionary_title0',None) is not None:
            description_detail += "<br>{}".format(node_properties['Dictionary1']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary1']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + node_properties['Dictionary1'][key] + "<br>"

         if node_properties['Dictionary2'].get('dictionary_title0',None) is not None:
            description_detail += "<br>{}".format(node_properties['Dictionary2']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary2']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + node_properties['Dictionary2'][key] + "<br>"

         if node_properties['Dictionary3'].get('dictionary_title0',None) is not None:
            description_detail += "<br>{}".format(node_properties['Dictionary3']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary3']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + node_properties['Dictionary3'][key] + "<br>"

         if node_properties['Dictionary4'].get('dictionary_title0',None) is not None:
            description_detail += "<br>{}".format(node_properties['Dictionary4']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary4']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + node_properties['Dictionary4'][key] + "<br>"

         if node_properties['Dictionary5'].get('dictionary_title0',None) is not None:
            description_detail += "<br>{}".format(node_properties['Dictionary5']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary5']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + node_properties['Dictionary5'][key] + "<br>"



         description_content = "<div style='font-size:12pt; line-height:200%'><center>"
         if node_properties['description'] is not None:
            description_content += node_properties['description'] + "<br>"
            utcnow = datetime.datetime.utcnow()
            time_gap = datetime.timedelta(hours=9)
            kor_time = utcnow + time_gap
            description_content += "<br>{}<br><br><br></center><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(description_detail, node_properties['mpid'], datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
         else:
            description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
         result['description'] = description_title + description_content


     except:
         print('Fail to transform')
         raise
     return result

def escape( str ):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\'")
    return str


