import csv

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         result = {}
         result['display'] = 'T'
         if node_properties['brand'] is None:
            idx = node_properties['name'].find(' ')
            if idx < 1:
               node_properties['brand'] = ' '
            else:
               node_properties['brand'] = node_properties['name'][:idx-1]
         result['product_name'] = node_properties['brand'] + ' - ' + node_properties['name']
         print("")
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.12;
         dollar2krw = 1300;
         margin_rate = 0.20;
         minimum_margin = 15000.0;# 최소 이윤 1만 5천원, 일단 고정
         lowest_price = -1;# 가장 가격이 작은 사이트의 가격인데 상품마다 다름, -1은은 가격이 최소인 사이트를 고려하지 않겠다는 뜻. 일단 -1로 고정
         category_num = 110;

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_us.csv', 'r') as f:
             reader = csv.reader(f, delimiter=',')
             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]
         
         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

         weight = 5.0
         print("1. weight(kg)=["+str(weight)+"]")
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge = charge[-1]
                 break
         
         shipping_price = node_properties['shipping_price'].split(' ')[0]
         if str(shipping_price).lower() == 'free':
             shipping_price = 0
         else:
             shipping_price = Price.fromstring(shipping_price).amount_float
         
         r = re.compile(re.compile(r"\d+(\.\d*)?"))

         print("2. delivery_charge=["+str(delivery_charge)+"]")

         price = node_properties['price']
         print("3. price=["+str(price)+"]")
         print("3.1 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         if str(price) == 'None':
             amount = Price.fromstring(node_properties['new_seller_price']).amount_float;
             if str(node_properties['new_seller_shipping_price']) != 'None':
                amount = amount + Price.fromstring(node_properties['new_seller_shipping_price']).amount_float;
         else:
             amount = Price.fromstring(price).amount_float;
             if str(shipping_price) != 'None':
                amount = amount + shipping_price;
                print("3.4 price,shipping,sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.08 if amount >= 200 else 0;# 1)0.8은 관세율, 상품별로 달라짐. 2)미국의 경우 amount >=200 달러 이고 나머지 나라는 amount >=150 달러
         taxRate = 0.1 if amount >= 200 else 0;# 2)0.1은 부가세율, 상품과 상관없이 고정,2)미국의 경우 amount >=200 달러 이고 나머지 나라는 amount >=150 달러

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

         result['detail_image'] = node_properties['images'][0]
         print(node_properties['images'])
         if result['detail_image'] == '':
             if len(node_properties['images']) >= 2:
                result['detail_image'] = node_properties['images'][1]
         print(result['detail_image'])
         print("6. NEW_Detail_Images=["+str(result['detail_image'][:10])+"]")
         print("6 number of images=["+str(len(node_properties['images']))+"]")

         result['additional_image'] = node_properties['images'][1:]
         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")

         variants = []
         variant = {}
         print("8. stock=["+ "999" +"]")
         variant['stock'] = 999
         variants.append(variant)
         size = node_properties.get('size', 'one size')
         if size is None:
             size = 'one size'
         variant['size'] = size
         variants.append(variant)

         if len(variants) == 0:
             result['has_option'] = 'F'
         else:
             result['has_option'] = 'T'
             result['variants'] = variants
             result['option_names'] = ['size']
         print("9. custom_produce_code=["+str(node_properties['item_no'])+"]")
         result['custom_product_code'] = node_properties['item_no']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = '<center>'

         for image in node_properties['images']:
             description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = '<h2>Images</h2>' + description_images + '</center>'
         #print(node_properties['information'])
         #print(node_properties['guarantee'])
         #description_info = '{}</center>{}</center>'.format(node_properties['information'],node_properties['guarantee'])
         #print(description_info)
         description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><b>Item No:</b>{}<br><br>{}<br><br>{}</div>".format(result['custom_product_code'],node_properties['description'],description_images)
         
         #description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br>{}<br><br>{}<br><br>{}</div>".format(
         #    node_properties['MaterialAndCare'], node_properties['Details'], node_properties['SizeAndFit'],
         #    description_images)
         result['description'] = description_title + description_content
     except:
         #print(result.keys())
         print(node_properties)
         raise
     #print(result.keys())
     return result




