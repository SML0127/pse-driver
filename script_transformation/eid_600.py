import csv

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         result = {}

         result['display'] = 'T'
         brand = node_properties.get('brand',None)
         if brand is None:
            brand = node_properties.get('additional_info_dict_for_brand',{}).get('Brand', None)
            if brand is None:
               brand = node_properties.get('additional_info_dict_for_brand',{}).get('BRAND', None)
               if brand is None:
                  brand = node_properties.get('additional_info_dict_for_brand',{}).get('Marca', None)
                  if brand is None:
                     brand = node_properties.get('additional_info_dict_for_brand',{}).get('brand', None)
                     if brand is None:
                        brand = node_properties.get('additional_info_dict',{}).get('brand', None)
                        if brand is None:
                           brand = node_properties.get('additional_info_dict',{}).get('Brand', None)
                           if brand is None:
                              brand = node_properties.get('additional_info_dict',{}).get('BRAND', None)
            node_properties['brand'] = brand
         node_properties['ePID'] = node_properties.get('additional_info_dict',{}).get('eBay Product ID (ePID)', 0)
         #if node_properties['ePID'] == 0:
         #   node_properties['ePID'] = node_properties['additional_info_dict2'].get('UPC:', 0)

         if node_properties['brand'] is None:
            result['product_name'] = node_properties['name'].split(' ')[0] + ' - ' + node_properties['name']
            node_properties['brand'] = node_properties['name'].split(' ')[0]
         else:
            result['product_name'] = node_properties['brand'] + ' - ' + node_properties['name']
         print("")
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.1;
         dollar2krw = 1300;
         margin_rate = 0.20;
         minimum_margin = 15000.0;
         lowest_price = -1;
         category_num = 147;

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

         price = node_properties.get('approx_price',None)
         if price is None:
            price = node_properties['price']
         print("3. price=["+str(price)+"]")

         shipping_price = node_properties.get('approx_shipping_price',None)
         print("3.1 approx shipping price=["+str(shipping_price)+"]");
         if shipping_price is not None:
            shipping_price = node_properties['shipping_price'].lower()
         print("3.1 shipping price=["+str(shipping_price)+"]");
         if 'free' in str(shipping_price):
            shipping_price = 0
         print("3.1 shipping price(check free)=["+str(shipping_price)+"]");

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != 'None':
            if shipping_price == 0:
               amount = amount + shipping_price;
            else:
               amount = amount + Price.fromstring(shipping_price).amount_float;
            print("3.4 price,shipping,sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.00 if amount >= 200 else 0;
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
         print(node_properties['images'])
         front_img = node_properties.get('front_image',None)
         if front_img is not None:
            node_properties['images'] = front_img
            result['detail_image'] = node_properties['images']
            #result['additional_image'] = [node_properties['images']]
         else: 
            if len(node_properties['images']) >= 2:
               result['detail_image'] = node_properties['images'][0]
               result['additional_image'] = node_properties['images'][1:]
            if len(node_properties['images']) == 1:
               result['detail_image'] = node_properties['images'][0]
               #result['additional_image'] = [node_properties['images'][0]]
         print(result)
            
         print("6. NEW_Detail_Images=["+str(result['detail_image'])+"]")
         #print("6.2 images=["+str(node_properties['images'])+"]")

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")

         stock = node_properties.get('stock', None)
         if stock is None:
            node_properties['stock'] = 999
         elif 'one' in str(stock):
            node_properties['stock'] = 1
         else: 
            node_properties['stock'] = re.search(r'\d+', str(stock)).group()
         result['stock'] = node_properties['stock']
         print("8. stock=["+ str(node_properties['stock']) +"]")

         result['has_option'] = 'F'

         dic = node_properties.get('additional_info_dict2',{})
         if len(dic) != 0:
            node_properties['additional_info_dict'] = dic


         description_feature = ''
         for key in node_properties['additional_info_dict']:
            description_feature += key.strip().split(':')[0] + ": " + node_properties['additional_info_dict'][key]+'<br>'
         print("9. custom_produce_code=["+str(node_properties['ePID'])+"]")
         result['custom_product_code'] = node_properties['ePID']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = '<center>'
         if front_img is not None:
            description_images += '<br><img src=\"{}\"></img>'.format(front_img)
         else:
            for image in node_properties['images']:
               description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = '<h2>Images</h2>' + description_images + '</center>'
         description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br>{}<br><br>{}</div>".format(description_feature,description_images)
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




