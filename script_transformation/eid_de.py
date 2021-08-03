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
         print("0.zipcode=["+str(node_properties['zipcode'])+"]");

         # 국가별로 달라질 수 있음, 환율도 달라질 수 있으니
         # 현재처럼 하는 것보다 달러비용만 저장하고 거기에 환율 곱해서 계산하도록 바꾸는 게 나을듯
         # 예) 0~0.5kg : 10.2 * 환율(현재는 1223원) = 12473원(현재)

         eur2usd = 1.13;
         dollar2krw = 1300;
         margin_rate = 0.20;
         minimum_margin = 15000.0;# 최소 이윤 1만 5천원, 일단 고정
         lowest_price = -1;# 가장 가격이 작은 사이트의 가격인데 상품마다 다름, -1은은 가격이 최소인 사이트를 고려하지 않겠다는 뜻. 일단 -1로 고정
         category_num = 135

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_de.csv', 'r') as f:
             reader = csv.reader(f, delimiter=',')
             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]
         
         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))


         #delivery_charge_list = list(csv.reader(open(datafile)))
         #print(data[1][4])
         #for row in delivery_charge
         #    data[row][3] = usd
         #    data[row][4] = data[row][2] * data[row][3]


         ## 상품별로 달라질수 있음. 아마존을 무게가 있지만, 다른 사이트는 없어서 기준 무게 필요 (5kg, 10kg ..등)
         weight = node_properties['additional_info_dict'].get('Shipping Weight', '5kg').lower()
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
             weight = 5.0## 동일하게 상품별로 달라질수 있음

         #delivery_charge = 35834.0 # 디폴트 배송비, 상품별로 달라질 수 있음. 이건 좀 코드가 이상한데 원래는 웨이트를 정하고 그 웨이트로부터 테이블에서 배송비를 얻어야 하는데
                                     # 하드코딩한 걸로 보임 (5kg의 배송비는 테이블을 보면  35834.0 임
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge = charge[-1]
                 break
         print("2. delivery_charge=["+str(delivery_charge)+"]")

         price = node_properties['price']
         print("3. price=["+str(price)+"]")
         if str(price) == 'None':
            print("3.1 new_seller_price =[" + str(node_properties['new_seller_price']) + "]")
            print("3.1 Price(new_seller_price) =[" + str(Price.fromstring(node_properties['new_seller_price']).amount_float) + "]")
            print("3.2 new_seller_shipping_price =[" + str(node_properties['new_seller_shipping_price']) + "]")
            print("3.2 Price(new_seller_shipping_price) =[" + str(Price.fromstring(node_properties['new_seller_shipping_price']).amount_float) + "]")
         else:
            shipping_price = node_properties['shipping_price']
            print("3.3 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         if str(price) == 'None':
             amount = Price.fromstring(node_properties['new_seller_price']).amount_float;
             if str(node_properties['new_seller_shipping_price']) != 'None':
                amount = amount + Price.fromstring(node_properties['new_seller_shipping_price']).amount_float;
         else:
             amount = Price.fromstring(price).amount_float;
             if str(shipping_price) != 'None':
                amount = amount + Price.fromstring(shipping_price).amount_float;
                print("3.4 price,shipping,sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.13 if amount >= 200 else 0;# 1)0.8은 관세율, 상품별로 달라짐. 2)미국의 경우 amount >=200 달러 이고 나머지 나라는 amount >=150 달러
         taxRate = 0.1 if amount >= 200 else 0;# 2)0.1은 부가세율, 상품과 상관없이 고정,2)미국의 경우 amount >=200 달러 이고 나머지 나라는 amount >=150 달러

         #supplyPrice = dollar2krw * amount + delivery_charge
         supplyPrice = eur2usd * dollar2krw * amount + delivery_charge
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
         #print("6. OLD_Detail_Images=["+str(result['detail_image'][:10])+"]")
         if result['detail_image'] == '':
             if len(node_properties['images']) >= 2:
                result['detail_image'] = node_properties['images'][1]
         print("6. NEW_Detail_Images=["+str(result['detail_image'][:10])+"]")
         print("6.1 number of images=["+str(len(node_properties['images']))+"]")
         #print("6.2 images=["+str(node_properties['images'])+"]")

         result['additional_image'] = node_properties['images'][1:]
         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")

         #available_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [4])
         #print(available_variant_ids)
         variants = []
         variant = {}
         print("8. stock=["+ str(node_properties['stock']) +"]")
         if str(node_properties['stock']) == 'None':
              variant['stock'] = 1
         else:
              variant['stock'] = int(node_properties['stock'])
         variants.append(variant)
         variant['size'] = "one size"
         variants.append(variant)
         #for variant_id in available_variant_ids:
         #    variant = graph_mgr.get_node_properties(variant_id)
         #    stock = variant.get('stock')
         #    matches = re.findall('\d+', stock)
         #    variant['stock'] = 10 if len(matches) == 0 else int(matches[0])
         #    variants.append(variant)
         #unavailable_variant_ids = graph_mgr.find_n_hop_neighbors(node_id, [5])
         #print(unavailable_variant_ids)
         #for variant_id in unavailable_variant_ids:
         #    variant = graph_mgr.get_node_properties(variant_id)
         #    variant['stock'] = 0
         #    variants.append(variant)
         if len(variants) == 0:
             result['has_option'] = 'F'
         else:
             result['has_option'] = 'T'
             result['variants'] = variants
             result['option_names'] = ['size']
         print("9. custom_produce_code=["+str(node_properties['asin'])+"]")
         result['custom_product_code'] = node_properties['asin']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
             description_images += '<br><img src=\"{}\"></img>'.format(image)
         description_images = '<h2>Images</h2>' + description_images + '</center>'
         description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>i<b>ASIN:</b>{}<br><br>{}<br><br>{}</div>".format(result['custom_product_code'],node_properties['description'],description_images)
         #description_content = "<div style='padding-left: 1em;'><h2>Description</h2><br><br>{}<br><br>{}<br><br>{}<br><br>{}</div>".format(
         #    node_properties['MaterialAndCare'], node_properties['Details'], node_properties['SizeAndFit'],
         #    description_images)
         result['description'] = description_title + description_content
     except:
         #print(result.keys())
         print(node_properties['url'])
         raise
     #print(result.keys())
     return result




