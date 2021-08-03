import csv

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         result = {}

         result['display'] = 'T'
         brand = "Rex Specs"
         node_properties['brand'] = brand
         #if node_properties['item_no'] == 0:
         #   node_properties['item_no'] = node_properties['additional_info_dict2'].get('UPC:', 0)

         result['product_name'] = node_properties['brand'] + ' - ' + node_properties['name']
         if 'lenses' in str(node_properties['name']).lower() or 'wraps' in str(node_properties['name']).lower():
            if node_properties['option1_value'] is not None and node_properties['option2_value'] is None:
               result['product_name'] = result['product_name'] + ' ('+ node_properties['option1_value'] + ')'
            elif node_properties['option1_value'] is None and node_properties['option2_value'] is not None:
               result['product_name'] = result['product_name'] + ' ('+ node_properties['option2_value'] + ')'
            elif node_properties['option1_value'] is not None and node_properties['option2_value'] is not None:
               result['product_name'] = result['product_name'] + ' ('+ node_properties['option1_value'] +'/'+ node_properties['option2_value'] + ')'
         print("")
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.1;
         dollar2krw = 1300;
         margin_rate = 0.20;
         minimum_margin = 15000.0;
         lowest_price = -1;
         category_num = 135;

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_us.csv', 'r') as f:
             reader = csv.reader(f, delimiter=',')
             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]
         
         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))


         weight = 1.0## 동일하게 상품별로 달라질수 있음
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

         shipping_price = "$6.5"
         print("3.1 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != 'None':
            amount = amount + Price.fromstring(shipping_price).amount_float;
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.08 if amount >= 200 else 0;
         taxRate = 0.1 if amount >= 200 else 0;
         supplyPrice = dollar2krw * amount + delivery_charge
         if supplyPrice == delivery_charge:
             print("Error calculate price")
             raise
         print("supply price= "+str(supplyPrice)+"")
         margin = supplyPrice * margin_rate
         print("margin = "+str(margin)+"")
         retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")
         buf = 5000# 버퍼로 최소이윤이 1만 5천원이었는데 5천원 뺀 1만원까지도 봐주겠다는 뜻
         isCompetitiveProduct = True

         if lowest_price == -1:
             adjRetailPrice = retailPrice
         else:
             adjRetailPrice = lowest_price - buf

         adjMargin = adjRetailPrice - retailPriceWithoutMargin
         true_min_margin = max(minimum_margin, (retailPriceWithoutMargin + adjMargin) * 0.065)# 0.065, 물건 팔면 발생하는 세금등의 마이너스 되는 금액 비율(카드사에 줘야할 돈 등), 일단 고정
         if adjMargin < true_min_margin:
             adjMargin = true_min_margin
             isCompetitiveProduct = False
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
         print("adjMargin = "+str(adjMargin)+"")
         price = retailPriceWithoutMargin + adjMargin

         result['price'] = str(price)
         print("4. supply_price = ["+ str(result['price'])+"]")
         result['supply_price'] = result['price']

         print("5. brand_code = [" + str(node_properties['brand']) + "]")
         result['brand_code'] = node_properties['brand']
         print("IMAGE")
         img = node_properties.get('images_one',None)
         if img is not None:
            node_properties['images'] = img
         print(node_properties['images'])
         for idx,val in enumerate(node_properties['images']):
            node_properties['images'][idx] = "http://" + node_properties['images'][idx][2:] 
         print(node_properties['images'])

         if len(node_properties['images']) >= 2:
            result['detail_image'] = node_properties['images'][0]
            result['additional_image'] = node_properties['images'][1:]
         if len(node_properties['images']) == 1:
            result['detail_image'] = node_properties['images'][0]
          
         print(result)
            

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")


         option_name1 = node_properties.get('option1', None)
         option_name2 = node_properties.get('option2', None)

         result['has_option'] = 'F'
         #variants = []
         #if option_name1 is None and option_name2 is None:
         #   result['has_option'] = 'F'
         #else:
         #   result['has_option'] = 'T'
         #   result['option_names'] = []
         #   if option_name1 is not None:
         #      variant = {}
         #      print(option_name1)
         #      print(node_properties['option_value1'])
         #      result['option_names'].append(str(option_name1).split('*')[1])
         #      variant['stock'] = 999;
         #      variant[str(option_name1).split('*')[1]] = node_properties['option_value1']
         #      variants.append(variant)
         #   if option_name2 is not None:
         #      variant = {}
         #      print(option_name2)
         #      print(node_properties['option_value2'])
         #      result['option_names'].append(str(option_name2).split('*')[1])
         #      variant['stock'] = 999;
         #      variant[str(option_name2).split('*')[1]] = node_properties['option_value2']
         #      variants.append(variant)
         #   print(result['option_names'])
         #result['variants'] = variants
#         available_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [4])
#         print(available_variant_ids)
#         variants = []
#         for variant_id in available_variant_ids:
#             variant = graph_mgr.get_node_properties(variant_id)
#             stock = variant.get('stock')
#             matches = re.findall('\d+', stock)
#             variant['stock'] = 10 if len(matches) == 0 else int(matches[0])
#             variants.append(variant)
#         unavailable_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [5])
#         print(unavailable_variant_ids)
#         for variant_id in unavailable_variant_ids:
#             variant = graph_mgr.get_node_properties(variant_id)
#             variant['stock'] = 0
#             variants.append(variant)
#         if len(variants) == 0:
#             result['has_option'] = 'F'
#         else:
#             result['has_option'] = 'T'
#             result['variants'] = variants
#             result['option_names'] = ['size']


         #stock = node_properties.get('stock', None)
         node_properties['stock'] = 999
         result['stock'] = node_properties['stock']
         print("8. stock=["+ str(node_properties['stock']) +"]")

         node_properties['item_no'] = node_properties['url'].split('variant=')[1]
         print("9. custom_produce_code=["+str(node_properties['item_no'])+"]")
         result['custom_product_code'] = node_properties['item_no']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
            description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = description_images + '</center>'
         de1 = node_properties['description']
         de2 = node_properties['description2']
         de3 = node_properties['description3']
         de4 = node_properties['description4']
         if de1 is None:
           de1 = ""
         if de2 is None:
           de2 = ""
         if de3 is None:
           de3 = ""
         if de4 is None:
           de4 = ""
         
         description_content = ""
         if de4 is not "":
            description_content = "<div style='padding-left: 1em;'>{}<br><br>{}<br><br><br /><br />{}</div>".format(de4,de1,description_images)
         else:
            description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br>{}<br>{}<br>{}<br><br>{}</div>".format(de1,de2,de3,description_images)
         #description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br>{}<br><br>{}<br><br>{}</div>".format(
         #    node_properties['MaterialAndCare'], node_properties['Details'], node_properties['SizeAndFit'],
         #    description_images)
         result['description'] = description_title + description_content
     except:
         print(node_properties)
         print(node_properties['url'])
         raise
     #print(result.keys())
     return result



