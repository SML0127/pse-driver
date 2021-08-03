import csv

def user_defined_export(graph_mgr, node_id, node_properties):
     print(1)
     try:
         result = {}

         result['display'] = 'T'
         brand = "Hurtta"
         node_properties['brand'] = brand
         option_name = node_properties['url'].split('/')[-2]
         result['product_name'] = node_properties['brand'] + ' - ' + node_properties['name'] +' (' + option_name +')'
         print("")
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.1;
         eur2krw = 1459;
         dollar2krw = 1300;
         yen2krw = 12.07
         margin_rate = 0.20
         minimum_margin = 5000.0;
         lowest_price = -1;
         category_num = 135;

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_uk.csv', 'r') as f:
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
         price = node_properties['additional_info_dict'].get(option_name, None)
         #price = node_properties['price']
         print("3. price=["+str(price)+"]")
         

         amount = 0.0
         amount = Price.fromstring(price).amount_float;

         shipping_price = '£4.99' 
         if amount > 4.99:
            shipping_price = '0.0' 
         print("3.1 shipping price=["+str(shipping_price)+"]");

         if str(shipping_price) != 'None':
            amount = amount + Price.fromstring(shipping_price).amount_float;
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.13 if amount >= 200 else 0;
         taxRate = 0.1 if amount >= 200 else 0;
         supplyPrice = eur2krw * amount + delivery_charge
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
         print(node_properties['images'])

         for idx, val in enumerate(node_properties['images']):
            node_properties['images'][idx] = 'https://www.hyperdrug.co.uk/' + val
 
         if len(node_properties['images']) >= 2:
            result['detail_image'] = node_properties['images'][0]
            result['additional_image'] = node_properties['images'][1:]
         if len(node_properties['images']) == 1:
            result['detail_image'] = node_properties['images'][0]
          
         print(result)
            

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")


         result['has_option'] = 'F'
         #option_name1 = node_properties.get('option1_name', None)
         #option_name2 = node_properties.get('option2_name', None)
         #print(option_name1)
         #print(option_name2)
         #variants = []
         #if option_name1 is None and option_name2 is None:
         #   result['has_option'] = 'F'
         #elif option_name1 is '' and option_name2 is '':
         #   print('err option')
         #   raise
         #else:
         #   result['has_option'] = 'T'
         #   result['option_names'] = []
         #   if option_name1 is not None and option_name1 is not '-' and option_name1 is not '':
         #      variant = {}
         #      print(option_name1)
         #      print(node_properties['option1'])
         #      result['option_names'].append(str(translator.translate(node_properties['option1_name'], dest="ko").text))
         #      variant['stock'] = 999;
         #      for idx, val in enumerate(node_properties['option1']):
         #         node_properties['option1'][idx] = str(translator.translate(node_properties['option1'][idx], dest="ko").text).replace('"','').replace("'","")
         #      variant[translator.translate(node_properties['option1_name'], dest="ko").text] = node_properties['option1']
         #      print(node_properties['option1'])
         #      variants.append(variant)
         #   if option_name2 is not None and option_name2 is not '-' and option_name2 is not '':
         #      variant = {}
         #      print(option_name2)
         #      print(node_properties['option2'])
         #      result['option_names'].append(translator.translate(node_properties['option2_name'], dest="ko").text)
         #      variant['stock'] = 999;
         #      for idx, val in enumerate(node_properties['option2']):
         #         node_properties['option2'][idx] = str(translator.translate(node_properties['option2'][idx], dest="ko").text).replace('"','').replace("'","")
         #      variant[translator.translate(node_properties['option2_name'], dest="ko").text] = node_properties['option2']
         #      print(node_properties['option2'])
         #      variants.append(variant)
         #   print(result['option_names'])
         #result['variants'] = variants

         #stock = node_properties.get('stock', None)
         node_properties['stock'] = 999
         result['stock'] = node_properties['stock']
         print("8. stock=["+ str(node_properties['stock']) +"]")

         node_properties['item_no'] = node_properties['item_no'].split(':')[1].split('\n')[0]
         print("9. custom_produce_code=["+str(node_properties['item_no'])+"]")
         result['custom_product_code'] = node_properties['item_no']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
            description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = '<h2>Images</h2>' + description_images + '</center>'
         description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br>{}</div>".format(node_properties['description'],description_images)
         #description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br>{}<br><br>{}<br><br>{}</div>".format(
         #    node_properties['MaterialAndCare'], node_properties['Details'], node_properties['SizeAndFit'],
         #    description_images)
         result['description'] = description_title + description_content
     except:
         print(node_properties['url'])
         raise
     #print(result.keys())
     return result



