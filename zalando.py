import csv
import datetime
from datetime import timedelta
import ast

def user_defined_export(graph_mgr, node_id, node_properties):
     try:

         node_properties['DictMaterialAndCare'] = ast.literal_eval(node_properties['DictMaterialAndCare'])
         node_properties['DictDetails'] = ast.literal_eval(node_properties['DictDetails'])
         node_properties['DictSizeAndFit'] = ast.literal_eval(node_properties['DictSizeAndFit'])

         result = {}
         result['display'] = 'T'  #store
         site_code = node_properties["smpid"]
         
         result['brand'] = node_properties['brand']

         result['product_name'] ="["+  result['brand'] +"] "+ node_properties['name']
         print("product=["+str(result['product_name'])+"]");
         result['manufacturer'] = result['brand']  #store
         result['manufacturer_code'] = result['manufacturer']  #store
         


         ############## New one
         pricing_information = node_properties["pricing_information"]

         euro2krw = float(pricing_information["exchange_rate"])  #store
         dollar2krw = float(pricing_information["dollar2krw"])  #should be modified
         euro2dollar = euro2krw/dollar2krw   #euro2dollar
         
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

         #shipping_price = node_properties["shipping_price"]
         #if "Free" in str(shipping_price):
         #   shipping_price = 0
         #else:
         #   shipping_price = Price.fromstring(node_properties["shipping_price"]).amount_float
         shipping_price = 0
         print("3.1 shipping price=["+str(shipping_price)+"]");
         result['pse_shipping_price'] = str(shipping_price)+""  #pse_store

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != "None":
            amount = amount + shipping_price
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = tariff_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         #tariffRate = tariff_rate if (amount + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         taxRate = vat_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         #taxRate = vat_rate if (amount + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
         result['pse_tariffRate'] = str(tariffRate)+""  #pse_store
         result['pse_taxRate'] = str(taxRate)+""  #pse_store
         
         #supplyPrice = dollar2krw * amount + delivery_charge_krw    #store
         supplyPrice = euro2krw * amount + delivery_charge_krw    #store
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
             margin = euro2krw * amount * 0.15
             if margin < minimum_margin:
                margin = minimum_margin
         else:
             margin = euro2krw * amount * 0.15  
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
                   print("op_v=")
                   print(op_v)
                   option_additional_price = 0   #jhlee
                   option_stock = 999   #jhlee
                   start1 =0
                   end1 = op_v.find('\n')
                   if end1 == -1:
                      option_size = op_v
                   else :   
                       
                      option_size = op_v[start1:end1]
                      start2 = end1+1
                      end2 = op_v.find('\n', start2)
                      if end2 != -1:
                         option_price = op_v[start2:end2]
                      else:
                         option_price = op_v[start2:]
                      print('option_price=[' + option_price + ']')

                      if '€' in option_price :
                         amount = Price.fromstring(option_price).amount_float
                         if str(shipping_price) != "None":
                            amount = amount + shipping_price
                         tariffRate = tariff_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
                         taxRate = vat_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw)>= int(tariff_threshold) else 0;    #store
                         print("tariffRate=" + str(tariffRate))
                         print("taxRate=" + str(taxRate))
                         #result['pse_tariffRate'] = str(tariffRate)+""  #pse_store
                         #result['pse_taxRate'] = str(taxRate)+""  #pse_store
                         
                         supplyPrice = euro2krw * amount + delivery_charge_krw    #store
                         print("supply price= "+str(supplyPrice)+"")
                         #result['pse_supplyPrice'] = str(supplyPrice)+""  #pse_store
                         
                         supplyPriceWithtariff = (supplyPrice) * (1 + tariffRate)
                         print("supplyPriceWithtariff = "+str(supplyPriceWithtariff)+"")    #store
                         #result['pse_supplyPriceWithtariff'] = str(supplyPriceWithtariff)+""  #pse_store

                         individualtariff = (supplyPriceWithtariff-2000000)*0.2 if supplyPriceWithtariff > 2000000 else 0;
                         print("individualtariff = "+str(individualtariff)+"")    #store
                         #result['pse_individualtariff'] = str(individualtariff)+""  #pse_store

                         edutariff = individualtariff * 0.3
                         print("edutariff = "+str(edutariff)+"")    #store
                         #result['pse_edutariff'] = str(edutariff)+""  #pse_store
         
                         retailPriceWithoutMargin = (supplyPriceWithtariff + individualtariff + edutariff) * (1 + taxRate)
                         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")    #store
                         #result['pse_retailPriceWithoutMargin'] = str(retailPriceWithoutMargin)+""  #pse_store
                            
                         if retailPriceWithoutMargin <= 700000:
                            margin = euro2krw * amount * 0.15
                            if margin < minimum_margin:
                               margin = minimum_margin
                         else:
                            margin = euro2krw * amount * 0.15  
                            if margin < minimum_margin:
                               margin = minimum_margin

                         print("margin = "+str(margin)+"")    #store
                         #result['pse_margin'] = str(margin)+""  #pse_store
         
                         retailPrice = retailPriceWithoutMargin + margin
                         print("retailPrice = "+str(retailPrice)+"")    #store
                         #result['pse_retailPrice'] = str(retailPrice)+""  #pse_store
         
                         buf = 5000

                         option_retailPrice = round(retailPrice,-2)
                         print("4. option_supply_price = ["+ str(option_retailPrice)+"]")
                         #result["supply_price"] = result["price"]                          

                         print('option_additional_price='+str(option_retailPrice)+' - '+result["price"])
                         option_additional_price = option_retailPrice-float(result["price"])   #jhlee
                         print('option_additional_price=' + str(option_additional_price))
                         if end2 != -1 :
                            start3 = end2+1
                            option_stock = op_v[start3:]
                            print("option_org_stock=" + option_stock)
                            if 'Notify' in option_stock :
                               option_stock = 0
                            elif 'Only' in option_stock:   
                               option_stock = int(Price.fromstring(option_stock).amount_float)
                            else :
                               option_stock = int(Price.fromstring(option_stock).amount_float)
                      elif 'Notify' in option_price :
                         option_additional_price = 0   #jhlee
                         option_stock = 0
                      elif 'Only' in option_price :
                         option_additional_price = 0   #jhlee
                         option_stock = option_price
                         option_stock = int(Price.fromstring(option_stock).amount_float)
                      else :
                         option_additional_price = 0   #jhlee
                         option_stock = option_price
                         option_stock = int(Price.fromstring(option_stock).amount_float)
                         
                   print("final_option_size=" + option_size)
                   print("final_option_price=" + str(option_additional_price))
                   print("final_option_stock=" + str(option_stock))

                   if({'value': option_size, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n]) :
                        if option_stock != 0 :
                           variant[op_n].append({'value': option_size, 'additional_amount': option_additional_price, 'stock': option_stock})   #jhlee
                           print("inserted ............")
                        else :
                           print("*********** skip a record of zero stock")
                   else :
                        print ("*********** skip duplicated option")  
                   #variant[op_n].append({'value': option_size, 'additional_amount': option_additional_price, 'stock': option_stock})   #jhlee
               variants.append(variant)
         result["variants"] = variants
         
         
         #available_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [4])
         #print(available_variant_ids)
         #variants = []
         #for variant_id in available_variant_ids:
         #   variant = graph_mgr.get_node_properties(variant_id)
         #   stock = variant.get('stock')
         #   matches = re.findall('\d+', stock)
         #   variant['stock'] = 10 if len(matches) == 0 else int(matches[0])
         #   variants.append(variant)
         #unavailable_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [5])
         #print(unavailable_variant_ids)
         #for variant_id in unavailable_variant_ids:
         #   variant = graph_mgr.get_node_properties(variant_id)
         #   variant['stock'] = 0
         #   variants.append(variant)
            
         if len(variants) == 0:
            result['has_option'] = 'F'
         else:
            result['has_option'] = 'T'
            result['variants'] = variants
            print("variants=")
            print(variant)
            #result['option_names'] = ['size']
            
         result["mpid"] = node_properties["mpid"]    
         node_properties['mpid'] = str(site_code) + str(node_properties['mpid']).zfill(7)
         print("9. custom_product_code=["+str(node_properties['mpid'])+"]")
         result["custom_product_code"] = node_properties["mpid"]
         print("10")
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}]
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = ''
         for image in node_properties['images']:
            description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = '<h2>Images</h2>' + description_images
         
         #node_properties['DictMaterialAndCare'] = ast.literal_eval(node_properties['DictMaterialAndCare'])
         #node_properties['DictDetails'] = ast.literal_eval(node_properties['DictDetails'])
         #node_properties['DictSizeAndFit'] = ast.literal_eval(node_properties['DictSizeAndFit'])
         
         
         description_detail = "<br>"
         description_detail += "<b>Material &amp; care</b>"
         description_detail += "<br>"
         print(node_properties["DictMaterialAndCare"])
         for key in node_properties["DictMaterialAndCare"]:
           if 'dictionary_title0' not in str(key):
              description_detail += "<b><span>"+str(key) + "</span></b> : " + node_properties["DictMaterialAndCare"][key] + "<br>"

         description_detail += "<br>"
         description_detail += "<b>Details</b>"
         description_detail += "<br>"
         print(node_properties["DictDetails"])
         for key in node_properties["DictDetails"]:
           if 'dictionary_title0' not in  str(key):
              description_detail += "<b><span>"+str(key) + "</span></b> : " + node_properties["DictDetails"][key] + "<br>"

         description_detail += "<br>"
         description_detail += "<b>Size &amp; fit</b>"
         description_detail += "<br>"
         print(node_properties["DictSizeAndFit"])
         for key in node_properties["DictSizeAndFit"]:
           if 'dictionary_title0' not in  str(key):
              description_detail += "<b><span>"+str(key) + "</span></b> : " + node_properties["DictSizeAndFit"][key] + "<br>"

         
         utcnow = datetime.datetime.utcnow()
         time_gap = datetime.timedelta(hours=9)
         kor_time = utcnow + time_gap
         description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br><br><label style=\"font-weight:bold\">PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(description_detail,node_properties["mpid"], datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
         print("11")

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
