import csv
import datetime
from datetime import timedelta
from google.cloud import translate_v2 as translate

def user_defined_export(graph_mgr, node_id, node_properties):
     try:
         result = {}
         client = translate.Client()
         result['display'] = 'T'
         brand = "Cleveland"
         site_code = '0004'
         node_properties['brand'] = brand
         result['product_name'] = '(Final) ' + escape(client.translate(node_properties['name'],source_language='ja',target_language='ko')['translatedText'])
         print("product=["+str(result['product_name'])+"]");

         eur2usd = 1.1;
         dollar2krw = 1300;
         yen2krw = 12.30
         margin_rate = 0.15
         #minimum_margin = 5000.0;
         lowest_price = -1;
         category_num = 165;

         with open('/home/pse/PSE-engine/script_transformation/delivery_charge_jp.csv', 'r') as f:
             reader = csv.reader(f, delimiter=',')
             delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in reader]
         
         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

         weight = 10.0## 동일하게 상품별로 달라질수 있음
         print("1.1 weight(kg)=["+str(weight)+"]")
         #delivery_charge = 35834.0 # 디폴트 배송비, 상품별로 달라질 수 있음. 이건 좀 코드가 이상한데 원래는 웨이트를 정하고 그 웨이트로부터 테이블에서 배송비를 얻어야 하는데
                                     # 하드코딩한 걸로 보임 (5kg의 배송비는 테이블을 보면  35834.0 임
         for charge in delivery_charge_list:
             if weight >= charge[0] and weight < charge[1]:
                 delivery_charge = charge[-1]
                 break
         print("2. delivery_charge=["+str(delivery_charge)+"]")
         price = node_properties['price']
         print("3. price=["+str(price)+"]")

         shipping_price = node_properties['shipping_price']
         if '送料無料' in str(shipping_price) or '送料別' in str(shipping_price) :
            shipping_price = "0"
         print("3.1 shipping price=["+str(shipping_price)+"]");

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != 'None':
            amount = amount + Price.fromstring(shipping_price).amount_float;
            print("3.4 price + shipping = sum=["+str(price)+"]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = 0.08 if amount * yen2krw >= 150 * dollar2krw else 0;
         taxRate = 0.1 if amount * yen2krw >= 150 * dollar2krw else 0;
         supplyPrice = yen2krw * amount + delivery_charge
         print("supply price= "+str(supplyPrice)+"")
         if supplyPrice == delivery_charge:
             print("Error calculate price")
             raise

         retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")

         margin = 0
         if retailPriceWithoutMargin <= 300000:
             margin_rate = 0.30
             margin = retailPriceWithoutMargin * margin_rate
             if margin <= 30000:
                margin = 30000
             elif margin >= 60000:
                margin = 60000
         else:
             if retailPriceWithoutMargin <= 500000:
                 margin_rate = 0.20
             elif retailPriceWithoutMargin <= 800000:
                 margin_rate = 0.17
             elif retailPriceWithoutMargin <= 1000000:
                 margin_rate = 0.15
             elif retailPriceWithoutMargin <= 1500000:
                 margin_rate = 0.125
             else: 
                 margin_rate = 0.1
             margin = retailPriceWithoutMargin * margin_rate


         print("margin = "+str(margin)+"")
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")
         buf = 5000# 버퍼로 최소이윤이 1만 5천원이었는데 5천원 뺀 1만원까지도 봐주겠다는 뜻

         result['price'] = str(round(retailPrice,-2))
         print("4. supply_price = ["+ str(result['price'])+"]")
         result['supply_price'] = result['price']

         print("5. brand_code = [" + str(node_properties['brand']) + "]")
         result['brand_code'] = node_properties['brand']
         print("6. images")
         print(node_properties['images'])

         img_list = []
         for idx,val in enumerate(node_properties['images']):
            if node_properties['images'][idx][-3:] == 'jpg':
               node_properties['images'][idx] = node_properties['images'][idx] + '?&downsize=1000:*'
            else:
               node_properties['images'][idx] = node_properties['images'][idx] + '&downsize=1000:*'
            img_list.append(node_properties['images'][idx].split('/')[-1].split('?')[0].split('.')[0])
 
         if len(node_properties['images']) >= 2:
            result['detail_image'] = node_properties['images'][0]
            result['additional_image'] = node_properties['images'][1:]
         if len(node_properties['images']) == 1:
            result['detail_image'] = node_properties['images'][0]
            

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")


         option_name1 = node_properties.get('option1_name', None)
         option_name2 = node_properties.get('option2_name', None)
         print(option_name1)
         print(option_name2)
         variants = []
         if option_name1 is None and option_name2 is None:
            result['has_option'] = 'F'
         elif option_name1 is '' and option_name2 is '':
            print('err option')
            raise
         else:
            result['has_option'] = 'T'
            result['option_names'] = []
            if option_name1 is not None and option_name1 is not '-' and option_name1 is not '':
               variant = {}
               result['option_names'].append(escape(str(client.translate(node_properties['option1_name'], source_language='ja', target_language='en')['translatedText'])))
               variant['stock'] = 999;
               for idx, val in enumerate(node_properties['option1']):
                  node_properties['option1'][idx] = escape(str(client.translate(node_properties['option1'][idx], source_language='ja',   target_language='en')['translatedText']).replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', ''))
               variant[escape(client.translate(node_properties['option1_name'], source_language='ja',   target_language='en')['translatedText'])] = node_properties['option1']
               print(node_properties['option1'])
               variants.append(variant)
            if option_name2 is not None and option_name2 is not '-' and option_name2 is not '':
               variant = {}
               result['option_names'].append(escape(str(client.translate(node_properties['option2_name'], source_language='ja', target_language='en')['translatedText'])))
               variant['stock'] = 999;
               for idx, val in enumerate(node_properties['option2']):
                  node_properties['option2'][idx] = escape(str(client.translate(node_properties['option2'][idx], source_language='ja',   target_language='en')['translatedText']).replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', ''))
               variant[escape(client.translate(node_properties['option2_name'], source_language='ja',   target_language='en')['translatedText'])] = node_properties['option2']
               print(node_properties['option2'])
               variants.append(variant)
         result['variants'] = variants         #stock = node_properties.get('stock', None)


         node_properties['stock'] = 999
         result['stock'] = node_properties['stock']
         print("8. stock=["+ str(node_properties['stock']) +"]")

         node_properties['mpid'] = site_code + str(node_properties['mpid']).zfill(6)
         print("9. custom_product_code=["+str(node_properties['mpid'])+"]")
         result['custom_product_code'] = node_properties['mpid']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}] # 106이 상품 카데고리 번호, cafe24 카테고리 번호로 excel 참조
         description_title = '<div><div align="center" style="font-size:14pt;font-weight:bold">{}</div><br><br></div><br>'.format(result['product_name'])
         description_images = '<center>'
         for image in node_properties['images']:
            description_images += '<br><br><img style="width: 100%" src=\"{}\"></img>'.format(image)
         description_images = '<br>' + description_images + '</center>'
         

         refined_description = ""
         for description_str in node_properties['description'].split('<br>'):
            if "wr_rec" in description_str:
               continue
            refined_description += description_str + "<br>"

         #cleanr0 = re.compile('<table.*?>.*?</table>')
         cleanr1 = re.compile('<iframe.*?>.*?</iframe>')
         cleanr2 = re.compile('<iframe.*?>.*?')
         cleanr3 = re.compile('</iframe>')
         #cleanr4 = re.compile('<a.*?>.*?</a>')
         cleanr5 = re.compile('<a.*?>')
         cleanr6 = re.compile('</a>')
         cleanr7 = re.compile('<font.*?>')
         cleanr8 = re.compile('<h.*?>')
         cleanr_garbagge = re.compile('<table[^>]*>\n<tbody><tr><td colspan="4"><img src="https://image.rakuten.co.jp/hayamimi/cabinet/img/img58097420.gif">[\s\S]*</table>')



         cleantext1 = re.sub(cleanr1, '', refined_description)
         cleantext2 = re.sub(cleanr2, '', cleantext1)
         cleantext3 = re.sub(cleanr3, '', cleantext2)
         #cleantext4 = re.sub(cleanr4, '', cleantext3)
         cleantext5 = re.sub(cleanr5, '', cleantext3)
         cleantext6 = re.sub(cleanr6, '', cleantext5)
         cleantext7 = re.sub(cleanr7, '', cleantext6)
         cleantext8 = re.sub(cleanr8, '', cleantext7)
         refined_description_last = re.sub(cleanr_garbagge, '', cleantext8)

         for img in img_list:
            cleanr9 = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".jpg[\"']?[^>]*>")
            refined_description_last = re.sub(cleanr9, '', refined_description_last)
            cleanr10 = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".gif[\"']?[^>]*>")
            refined_description_last = re.sub(cleanr10, '', refined_description_last)
         for sub_str in ['配送','出荷','納期']:
            cleanr11 = re.compile("<[^>]*>(.+?)"+sub_str+"(.+?)</[^>]*>")
            refined_description_last = re.sub(cleanr11, '', refined_description_last)

         refined_description_last = refined_description_last.strip()
         refined_description_last = refined_description_last.replace('</ ', '</').replace('< ', '<').replace('&amp',' ').replace('↑','').replace('→','').replace('指輪のサイズ表','').replace('<br>',' <br> ').replace('<b>','').replace('</b>','').replace('<h1>','').replace('</h1>','<br>').replace('<h2>','').replace('</h2>','<br>').replace('<h3>','').replace('</h3>','<br>').replace('<h4>','').replace('</h4>','<br>').replace('<h5>','').replace('</h5>','<br>').replace('</font>','').replace('https://www.rakuten.ne.jp/gold/f-netgolf/images/spacer.gif','')
         refined_description_last = refined_description_last.strip().replace('<br><br><br>','').replace('※商品ページのサイズ表は海外サイズを日本サイズに換算した一般的なサイズとなりメーカー・商品によってはサイズが異なる場合もございます。サイズ表は参考としてご活用ください。','')
         print('-----------------------------------------------------------------------------------------------------------------------')
         print(node_properties['description_sale'])
         print('-----------------------------------------------------------------------------------------------------------------------')
         refined_description_sale = ""

         for description_str in node_properties['description_sale'].split('<br>'):
            if "wr_rec" in description_str:
               continue
            refined_description_sale += description_str + "<br>"
         #split('/')[-1].split('?')[0].split('.')[0]

         cleantext1 = re.sub(cleanr1, '', refined_description_sale)
         cleantext2 = re.sub(cleanr2, '', cleantext1)
         cleantext3 = re.sub(cleanr3, '', cleantext2)
         #cleantext4 = re.sub(cleanr4, '', cleantext3)
         cleantext5 = re.sub(cleanr5, '', cleantext3)
         cleantext6 = re.sub(cleanr6, '', cleantext5)
         cleantext7 = re.sub(cleanr7, '', cleantext6)
         refined_description_sale_last = re.sub(cleanr8, '', cleantext7)
         for img in img_list:
            cleanr9 = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".jpg[\"']?[^>]*>")
            refined_description_sale_last = re.sub(cleanr9, '', refined_description_sale_last)
            cleanr10 = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".gif[\"']?[^>]*>")
            refined_description_sale_last = re.sub(cleanr10, '', refined_description_sale_last)


         refined_description_sale_last = refined_description_sale_last.strip()
         refined_description_sale_last = refined_description_sale_last.replace('</ ', '</').replace('< ', '<').replace('&amp',' ').replace('↑','').replace('→','').replace('指輪のサイズ表','').replace('<br>',' <br> ').replace('<b>','').replace('</b>','').replace('<h1>','').replace('</h1>','<br>').replace('<h2>','').replace('</h2>','<br>').replace('<h3>','').replace('</h3>','<br>').replace('<h4>','').replace('</h4>','<br>').replace('<h5>','').replace('</h5>','<br>').replace('</font>','').replace('https://www.rakuten.ne.jp/gold/f-netgolf/images/spacer.gif','')
         refined_description_sale_last = refined_description_sale_last.strip().replace('<br><br><br>','').replace('※商品ページのサイズ表は海外サイズを日本サイズに換算した一般的なサイズとなりメーカー・商品によってはサイズが異なる場合もございます。サイズ表は参考としてご活用ください。','')
         
         #refined_description_sale_tmp = ''
         #cont = False
         #for desc in refined_description_sale_last.split('<br>'): 
         #   if desc == '\n' and cont == False:
         #      cont = True
         #      refined_description_sale_tmp += desc +'<br>'
         #   elif desc != '\n':
         #      cont == False
         #      refined_description_sale_tmp += desc +'<br>'
         #   elif desc == '\n' and cont == True:
         #      continue
 
         #refined_description_sale_last = refined_description_sale_tmp
         cleanr_garbagge = re.compile('<table[^>]*cellspacing="0" cellpadding="0" border="0"[^>]*>[\s\S]*<div[^>]*id="rnkInShopTemplate"[^>]*>[\s\S]*</div>')
         for sub_str in ['item_desc','配送','出荷','納期','img58097420']:
            if sub_str in refined_description_sale_last:
               refined_description_sale_last = ""
               break;

         print('-----------------------------------------------------------------------------------------------------------------------')
         print(refined_description_sale)
         print('-----------------------------------------------------------------------------------------------------------------------')


         refined_description_sale_last = ""
         if refined_description_last != "":
            description_content = "<div style='padding-left: 1em;font-size:12pt'>원본상품명: {}</div><br><br>".format(node_properties['name'])
            description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
            if node_properties['description'] is not None:
               desc_con = ""
               description_content += str(client.translate(refined_description_last, source_language='ja',   target_language='ko')['translatedText']).replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','').replace('↑','').replace('→','') + "<br>"
               if refined_description_sale_last != "":
                  description_content += remove_br(str(client.translate(refined_description_sale_last, source_language='ja',   target_language='ko')['translatedText']).replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','').replace('↑','').replace('→','') + "<br>")
               utcnow = datetime.datetime.utcnow()
               time_gap = datetime.timedelta(hours=9)
               kor_time = utcnow + time_gap
               description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'],datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
            else:
               description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
            result['description'] = description_title + description_content

         else:
            description_content = "<div style='padding-left: 1em;font-size:12pt'>원본상품명: {}</div><br><br>".format(node_properties['name'])
            description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
            if node_properties['description'] is not None:
               desc_con = ""
               description_content += refined_description_last.replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','') + "<br>"
               if refined_description_sale_last != "":
                  description_content += remove_br(str(client.translate(refined_description_sale_last, source_language='ja',   target_language='ko')['translatedText']).replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','').replace('↑','').replace('→','') + "<br>")
               utcnow = datetime.datetime.utcnow()
               time_gap = datetime.timedelta(hours=9)
               kor_time = utcnow + time_gap
               description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'],datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
            else:
               description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
            result['description'] = description_title + description_content

     except:
         print(node_properties['url'])
         raise
     return result

def escape( str ):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\"")
    return str

def remove_br( str ):
    refined_description_sale_tmp = ''
    cont = False
    for desc in str.split('<br>'): 
       if (desc == '\n' or desc == '') and cont == False:
          cont = True
          refined_description_sale_tmp += desc +'<br>'
       elif (desc != '\n' and desc != ''):
          cont == False
          refined_description_sale_tmp += desc +'<br>'
       elif (desc == '\n' or desc == '') and cont == True:
          continue

    return refined_description_sale_tmp

