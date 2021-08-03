import csv
import datetime
from datetime import timedelta
import ast

#### amazon gpu ############
#### other_technical_details_dict 추가 ############
def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         node_properties['ADDITIONAL INFO'] =ast.literal_eval(node_properties['ADDITIONAL INFO'])
         #add by jhlee
         node_properties['brand'] = node_properties['other_technical_details.dict'].get('Brand', 'none').lower() #get brand name
         
         result = {}
         result['display'] = 'T'
         if node_properties.get('brand') == 'none':
            idx = node_properties['name'].find(' ')  #dewalt brand name has a different form
           if idx < 1:
              node_properties['brand'] = ''
           else:
              node_properties['brand'] = node_properties['name'][:idx-1]
         else:
           node_properties['brand]] =''
#        result['product_name'] = node_properties['brand'] + ' - ' + node_properties['p_name']
         result['product_name'] = node_properties['name']
         print("")
         print("product=["+str(result['product_name'])+"]");
         site_code = node_properties["smpid"]

         # get weight information
         #shipping_weight = node_properties['additional_info_dict'].get('Shipping Weight', 'none').lower() #shipping weight
         shipping_weight = node_properties['additional_info_dict'].get('Shipping Weight', 'none').lower() #shipping weight
         #item_weight = node_properties['technical_info_dict'].get('Item Weight', 'none').lower() #item weight
         item_weight = node_properties['other_technical_info_dict'].get('Item Weight', 'none').lower() #item weight
         if shipping_weight == 'none':
            if item_weight == 'none':
               weight = '1kg'  #should be modified**************************************
            else:
               weight = item_weight
         else:
            weight = shipping_weight

         r = re.compile(re.compile(r"\d+(\.\d*)?"))
         print("1. weight=["+str(weight)+"]")
         try:
             b = float(re.match(r, weight).group(0))
             if weight.find('kg'):
                 weight = float(b)
             elif weight.find('g'):
                 weight = float(b) * 0.001
             elif weight.find('pound'):
                 weight = float(b) * 0.453592
             elif weight.found('ounce'):
                 weight = float(b) * 0.0283495
         except:
             weight = 1.0  #should be modified.
        # get weight information

         ############## New one
         pricing_information = node_properties["pricing_information"]

         dollar2krw = pricing_information["exchange_rate"]
         minimum_margin = pricing_information["min_margin"]
         margin_rate = pricing_information["margin_rate"]

         tariff_rate = pricing_information["tariff_rate"]
         vat_rate = pricing_information["vat_rate"]
         shipping_cost = pricing_information["shipping_cost"]
         tariff_threshold = pricing_information["tariff_threshold"]  # add by jhlee
         delivery_charge_list = node_properties['shipping_fee'] #shipping_fee

         #modified by jhlee
         buf = 5000   
         isCompetitiveProduct = True
         lowest_price = -1
         #modified by jhlee

         category_num = node_properties["cnum"]

#         with open("/home/pse/PSE-engine/script_transformation/delivery_charge_us.csv", "r") as f:
#             reader = csv.reader(f, delimiter=",")
#             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]

         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

#        weight = 5.0 
         print("1.1 weight(kg)=["+str(weight)+"]")

         delivery_charge = -1
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge = charge[-1]
                 break
         # modified by jhlee
         if delivery_charge == -1:
            delivery_charge = pricing_information["shipping_cost"]
         # modified by jhlee
         print("2. delivery_charge=["+str(delivery_charge)+"]")

         # modified by jhlee
         price = node_properties.get("price", None)
         print("3. price=["+str(price)+"]")
         if price is None:
           print("3.1 new_seller_price =[" + str(node_properties['new_seller_price']) + "]")
           print("3.1 Price(new_seller_price) =[" + str(Price.fromstring(node_properties['new_seller_price']).amount_float) + "]")
           print("3.2 new_seller_shipping_price =[" + str(node_properties['new_seller_shipping_price']) + "]")
           print("3.2 Price(new_seller_shipping_price) =[" + str(Price.fromstring(node_properties['new_seller_shipping_price']).amount_float) + "]")

         shipping_price = node_properties['shipping_price']
         if "Free" in str(shipping_price):
            shipping_price = 0
         else
            shipping_price = Price.fromstring(node_properties["shipping_price"]).amount_float
         print("3.3 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         if price is None:
            amount = Price.fromstring(node_properties['new_seller_price']).amount_float;
            if str(node_properties['new_seller_shipping_price']) != 'None':
               amount = amount + Price.fromstring(node_properties['new_seller_shipping_price']).amount_float;
         else:
            amount = Price.fromstring(price).amount_float;
            if str(shipping_price) != 'None':
               amount = amount + Price.fromstring(shipping_price).amount_float;
               print("3.4 price,shipping,sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         # modified by jhlee

         tariffRate = tariff_rate if amount + delivery_charge/dollar2krw>= int(tariff_threshold) else 0;
         taxRate = vat_rate if amount + delivery_charge/dollar2krw>= int(tariff_threshold) else 0;
         supplyPrice = dollar2krw * amount + delivery_charge
         print("supply price= "+str(supplyPrice)+"")

         # 공구는 개별소비세와 교육세 정보가 없음 
         retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")

         margin = dollar2krw * amount * margin_rate
         #if retailPriceWithoutMargin <= 700000:
         #    margin = dollar2krw * amount * 0.15
         #    if margin < 50000:
         #       margin = 50000
         #else:
         #    margin = dollar2krw * amount * 0.1
         #    if margin < 50000:
         #       margin = 50000

         print("margin = "+str(margin)+"")
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")

         # modified by jhlee

         if lowest_price == -1:
            adjRetailPrice = retailPrice
         else:
            adjRetailPrice = lowest_price - buf
         adjRetailPrice = lowest_price - retailPriceWithoutMargin

         true_min_margine = max(minimum_margin, (retailPriceWithoutMargin + adjMargin) * 0.065)
         if adjMargin < true_min_margin:
            adjMargin = true_min_margin
            isCompetitiveProduct = False
         price = retailPriceWithoutMargin + adjMargin

         # modified by jhlee

         result["price"] = str(round(retailPrice,-2))
         print("4. supply_price = ["+ str(result["price"])+"]")
         result["supply_price"] = result["price"]

         print("5. brand_code = [" + str(node_properties['brand']) + "]")
         result["brand_code"] =  node_properties['brand']
         print("6. images")
         print(node_properties["images"])

         if len(node_properties["images"]) >= 2:
            result["detail_image"] = node_properties["images"][0]
            result["additional_image"] = node_properties["images"][1:]
         elif len(node_properties["images"]) == 1:
            result["detail_image"] = node_properties["images"][0]
         elif len(node_properties["images"]) == 0:
            result["detail_image"] = node_properties["front_image"]
            node_properties["images"] = []
            node_properties["images"].append(node_properties["front_image"])

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


         stock = node_properties.get("stock", None)
         if stock is None:
            result["stock"] = 999
         result["stock"] = node_properties["stock"]
         print("8. stock=["+ str(node_properties["stock"]) +"]")

         result["mpid"] = node_properties["mpid"]
         node_properties["mpid"] = str(site_code)+str(node_properties["mpid"]).zfill(7)
         print("9. custom_product_code=["+str(node_properties["mpid"])+"]")
         result["custom_product_code"] = node_properties["mpid"]
         result["add_category_no"] = [{"category_no": category_num, "recommend": "F", "new": "T"}]
         description_title = "<div><div align=\"center\" style=\"font-size:14pt;font-weight:bold\">{}</div><br><br></div><br>".format(result["product_name"])
         description_images = "<center>"
         for image in node_properties["images"]:
            description_images += "<br><br><img style=\"width: 100%\" src=\"{}\"></img>".format(image)
         description_images = "<br>" + description_images + "</center>"

         description_content = "<div style=\"font-size:12pt; line-height:200%\">"
         if node_properties["description"] is not None:
            description_content += node_properties["description"] + "<br>"
            utcnow = datetime.datetime.utcnow()
            time_gap = datetime.timedelta(hours=9)
            kor_time = utcnow + time_gap
            description_content += "<br><br><br><label style=\"font-weight:bold\">PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties["mpid"], datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
         else:
            description_content = "<div style=\"padding-left: 1em; margin-top:10pt\"><br><br>{}</div>".format(description_images)
         result["description"] = description_title + description_content
     except:
         print('error')
         raise
     return result

def escape( str ):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\'")
    return str

