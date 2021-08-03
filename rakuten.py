import csv
import datetime
from datetime import timedelta
from googletrans import Translator
from util.pse_errors import *
import ast

def user_defined_export(graph_mgr, node_id, node_properties):
     try:

         result = {}
         result['memo'] = node_properties.get('url', 'no url')
         print("0. url=["+str(result['memo'])+"]")
         
         result['display'] = 'T'
         site_code = node_properties["smpid"]
         brand = node_properties.get('brand', 'Rakuten') # use Rakuten as its default value 
         node_properties['brand'] = brand
         translator = Translator()
         print(node_properties['name'])
         p_name = translator.translate(
             node_properties['name'], src='ja', dest="ko").text
         print("translate p_name OK")
         if '중고' in p_name:
             print('Used product (no error)')
             raise UserDefinedError('Used product (no error)')
         print("중고 OK")
         result['product_name'] = node_properties['brand'] + \
             ' - ' + escape(p_name)
         print("product=["+str(result['product_name'])+"]");

         pricing_information = node_properties["pricing_information"]

         dollar2krw = float(pricing_information["dollar2krw"])
         yen2krw = float(pricing_information["exchange_rate"])/100
         minimum_margin = float(pricing_information["min_margin"])
         margin_rate = float(pricing_information["margin_rate"])
         tariff_rate = float(pricing_information["tariff_rate"])
         vat_rate = float(pricing_information["vat_rate"])
         tariff_threshold = float(pricing_information["tariff_threshold"])
         delivery_charge_list = node_properties['shipping_fee']  # shipping_fee

         lowest_price = -1;
         category_num = node_properties["cnum"];

         for row in range(len(delivery_charge_list)):
             delivery_charge_list[row].append(float(dollar2krw))
             delivery_charge_list[row].append(
                 int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

         weight = float(pricing_information["default_weight"])
         print("1.1 weight(kg)=["+str(weight)+"]")
         result['pse_weight'] = str(weight)+""  #pse_store

         delivery_charge_krw = 0
         for charge in delivery_charge_list:
             if weight > charge[0] and weight <= charge[1]:
                 delivery_charge_krw = charge[-1]
                 break
         print("2. delivery_charge=["+str(delivery_charge_krw)+"]")
         result['pse_delivery_charge_krw'] = str(delivery_charge_krw)+""  #pse_store
         price = node_properties["price"]
         print("3. price=["+str(price)+"]") 

         shipping_price = node_properties['shipping_price']
         if '送料無料' in str(shipping_price) or '送料別' in str(shipping_price):
            shipping_price = 0
         else:
            shipping_price = Price.fromstring(
                node_properties["shipping_price"]).amount_float
         print("3.1 shipping price=["+str(shipping_price)+"]");
         result['pse_shipping_price'] = str(shipping_price)+""  #pse_store

         amount = 0.0
         amount = Price.fromstring(price).amount_float;
         if str(shipping_price) != "None":
            amount = amount + shipping_price
            print("3.4 price + shipping = sum=["+str(price) +
                  "]["+str(shipping_price)+"]["+str(amount)+"]");

         tariffRate = tariff_rate if (
             amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0;
         taxRate = vat_rate if (
             amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0;
         result['pse_tariffRate'] = str(tariffRate)+""  #pse_store
         result['pse_taxRate'] = str(taxRate)+""  #pse_store         
         
         supplyPrice = yen2krw * amount + delivery_charge_krw
         print("supply price= "+str(supplyPrice)+"")
         result['pse_supplyPrice'] = str(supplyPrice)+""  #pse_store         

         print("3.2 price + shipping = sum=["+str(price) +
               "]["+str(shipping_price)+"]["+str(amount)+"]");
         if supplyPrice == delivery_charge_krw:
             print("Error calculate price")
             raise UserDefinedError('Error calculate price(supplyPrice == delivery_charge_krw)')
             raise

         retailPriceWithoutMargin = (
             (supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
         print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
         result['pse_retailPriceWithoutMargin'] = str(retailPriceWithoutMargin)+""  #pse_store

         margin = 0
         margin = supplyPrice * margin_rate

#         if retailPriceWithoutMargin <= 700000:
#             margin = yen2krw * amount * 0.12
#             if margin < 35000:
#                margin = 35000
#         else:
#             margin = yen2krw * amount * 0.1
#             if margin < 35000:
#                margin = 35000

         if retailPriceWithoutMargin <= 700000:
             margin = yen2krw * amount * 0.15
             if margin < minimum_margin:
                margin = minimum_margin
         else:
             margin = yen2krw * amount * 0.15
             if margin < minimum_margin:
                margin = minimum_margin

         print("margin = "+str(margin)+"")
         result['pse_margin'] = str(margin)+""  #pse_store
         
         retailPrice = retailPriceWithoutMargin + margin
         print("retailPrice = "+str(retailPrice)+"")
         result['pse_retailPrice'] = str(retailPrice)+""  #pse_store
         
         buf = 5000  # 버퍼로 최소이윤이 1만 5천원이었는데 5천원 뺀 1만원까지도 봐주겠다는 뜻

         result['price'] = str(round(retailPrice, -2))
         print("4. supply_price = [" + str(result['price'])+"]")
         result['supply_price'] = result['price']

         print("5. brand_code = [" + str(node_properties['brand']) + "]")
         result['brand_code'] = node_properties['brand']
         print("6. images")
         print(node_properties['images'])

         img_list = []
         for idx, val in enumerate(node_properties['images']):
            if node_properties['images'][idx][-3:] == 'jpg':
               node_properties['images'][idx] = node_properties['images'][idx] + \
                   '?&downsize=1000:*'
            else:
               node_properties['images'][idx] = node_properties['images'][idx] + \
                   '&downsize=1000:*'
            img_list.append(node_properties['images'][idx].split(
                '/')[-1].split('?')[0].split('.')[0])

         if len(node_properties['images']) >= 2:
            result['detail_image'] = node_properties['images'][0]
            result['additional_image'] = node_properties['images'][1:]
         if len(node_properties['images']) == 1:
            result['detail_image'] = node_properties['images'][0]

         result['selling'] = 'T'
         result['memo'] = node_properties.get('url', 'no url')
         print("7. url=["+str(result['memo'])+"]")

         option_names = node_properties.get('option_name', [])
         option_names = list(option_names)
         option_values = node_properties.get('option_value', {})
         print(option_names)
         print(option_values) 
          
         matrix_row_name = ""
         matrix_col_name = ""
         variants = []
         option_matrix = {}
         processed = 0
         if len(option_names) == 0:
            result["has_option"] = "F"
         else:
            result["has_option"] = "T"
            result["option_names"] = []      
            
         tmp_option_names = []
         for idx, op_n in enumerate(option_names):
            if op_n == 'option_matrix_col_name':
               matrix_col_name = option_names[idx-1]
               print("matrix_col_name=")
               print(matrix_col_name)
               print("len1="+str(len(matrix_col_name)))
            elif op_n == 'option_matrix_row_name' :
               matrix_row_name = option_names[idx-1]
               print("matrix_row_name=")
               print(matrix_row_name)
               print("len2="+str(len(matrix_row_name)))
               
            else:
               tmp_option_names.append(op_n)
         option_names = tmp_option_names
         
         if (len(matrix_col_name) != 0 and ((len(matrix_row_name) == 1 and '-' in matrix_row_name) or len(matrix_row_name)==0)) :
            print("matrix_row_name is null or -") 
            processed = 1 
            print("matrix_row_name=["+str(matrix_row_name)+"]")
            print("len="+str(len(matrix_row_name)))
            print("matrix_col_name=["+str(matrix_col_name)+"]")
            print("len="+str(len(matrix_col_name)))
            variant = {}
            if len(matrix_col_name) != len(matrix_col_name.encode()) :
               op_n_translated = escape(str(translator.translate(matrix_col_name, src='ja',  dest="ko").text))
            else :
               op_n_translated = matrix_col_name
            result["option_names"].append(op_n_translated)
            variant[op_n_translated] = []
            option_stock = 999
            option_additional_price = 0
            
            print("option_values=")
            print(option_values)

            matrix_row_option_name = option_values.get(matrix_row_name,[])
            print("matrix_row_option_name=")
            print(matrix_row_option_name)
            matrix_col_option_name = option_values.get(matrix_col_name,[])
            print("matrix_col_option_name=")
            print(matrix_col_option_name)
            
            idx = 0
            if len(matrix_row_option_name) != 0 :
               for ov in matrix_row_option_name:
                  if ov != '' :  
                     if len(ov) != len(ov.encode()) :
                        ov = escape(str(translator.translate(ov, src='ja',  dest="ko").text))
                  for ov2 in matrix_col_option_name:
                     if len(ov2) != len(ov2.encode()) : 
                        ov2 = escape(str(translator.translate(ov2, src='ja',  dest="ko").text))
                     #print("["+ov+","+ov2+"]("+str(idx)+")")
                     option_stock = 999
                     option_additional_price = 0                  
                  #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 999} # for test 3000, 999
                     if '×' in option_values['option_maxtrix_value'][idx] or '売り切れ' in option_values['option_maxtrix_value'][idx] or '入荷未定' in option_values['option_maxtrix_value'][idx]:
                     #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 0}
                        option_stock = 0
                        option_additional_price = 0
                     ov2 = ov2.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')    
                     if({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]) :
                        # ov2가 들어간건지 체크
                        variant[op_n_translated].append({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock})
                     idx = idx + 1
               variants.append(variant)
            else :
               for ov2 in matrix_col_option_name:
                  if len(ov2) != len(ov2.encode()) : 
                     ov2 = escape(str(translator.translate(ov2, src='ja',  dest="ko").text))
                  #print("[","+ov2+"]("+str(idx)+")")
                  option_stock = 999
                  option_additional_price = 0                  
                  #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 999} # for test 3000, 999
                  if '×' in option_values['option_maxtrix_value'][idx] or '売り切れ' in option_values['option_maxtrix_value'][idx] or '入荷未定' in option_values['option_maxtrix_value'][idx]:
                     #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 0}
                     option_stock = 0
                     option_additional_price = 0
                  ov2 = ov2.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')     
                  if({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]) :
                     # ov2가 들어간건지 체크
                     variant[op_n_translated].append({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock})                     
                  idx = idx + 1
               variants.append(variant)
                  #print("option_matrix=")
                  #print(option_matrix[ov,ov2])
            #result['option_matrix'] = option_matrix
            #result['matrix_row_name'] = escape(
            #          str(translator.translate(matrix_row_name, src='ja',  dest="ko").text))
            #result['matrix_col_name'] = escape(
            #          str(translator.translate(matrix_col_name, src='ja',  dest="ko").text))            
            #      variants.append(variant)
         elif (len(matrix_row_name) != 0 and ((len(matrix_col_name) == 1 and '-' in matrix_col_name) or len(matrix_col_name)==0)) :
            print("matrix_col_name is null or -") 
            processed = 1 
            print("matrix_col_name="+str(matrix_col_name))
            variant = {}
            if len(matrix_row_name) != len(matrix_row_name.encode()) :
               op_n_translated = escape(str(translator.translate(matrix_row_name, src='ja',  dest="ko").text))
            else :
               op_n_translated = matrix_row_name
            result["option_names"].append(op_n_translated)
            variant[op_n_translated] = []
            option_stock = 999
            option_additional_price = 0

            print("option_values=")
            print(option_values)

            matrix_row_option_name = option_values.get(matrix_row_name,[])
            print("matrix_row_option_name=")
            print(matrix_row_option_name)
            matrix_col_option_name = option_values.get(matrix_col_name,[])
            print("matrix_col_option_name=")
            print(matrix_col_option_name)
            
            idx = 0
            if len(matrix_col_option_name) != 0 :
               for ov in matrix_row_option_name:
                  if len(ov) != len(ov.encode()) :
                     ov = escape(str(translator.translate(ov, src='ja',  dest="ko").text))
                  for ov2 in matrix_col_option_name:
                     if ov2 != '' :
                         if len(ov2) != len(ov2.encode()) :
                            ov2 = escape(str(translator.translate(ov2, src='ja',  dest="ko").text))
                     option_stock = 999
                     option_additional_price = 0                  
                     if '×' in option_values['option_maxtrix_value'][idx] or '売り切れ' in option_values['option_maxtrix_value'][idx] or '入荷未定' in option_values['option_maxtrix_value'][idx]:
                       option_stock = 0
                       option_additional_price = 0
                     ov = ov.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')      
                     if({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]) :
                        # ov가 들어간건지 체크
                       variant[op_n_translated].append({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock})                     
                     idx = idx + 1
               variants.append(variant)
            else :      
               for ov in matrix_row_option_name:
                  if len(ov) != len(ov.encode()) :
                     ov = escape(str(translator.translate(ov, src='ja',  dest="ko").text))
                  option_stock = 999
                  option_additional_price = 0                  
              #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 999} # for test 3000, 999
                  if '×' in option_values['option_maxtrix_value'][idx] or '売り切れ' in option_values['option_maxtrix_value'][idx] or '入荷未定' in option_values['option_maxtrix_value'][idx]:
                     #option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 0}
                     option_stock = 0
                     option_additional_price = 0
                  ov = ov.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')                         
                  if({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]) :
                     # ov가 들어간건지 체크     
                     variant[op_n_translated].append({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock})                     
                  idx = idx + 1
               variants.append(variant)
                  #print("option_matrix=")
                  #print(option_matrix[ov,ov2])
            #result['option_matrix'] = option_matrix
            #result['matrix_row_name'] = escape(
            #          str(translator.translate(matrix_row_name, src='ja',  dest="ko").text))
            #result['matrix_col_name'] = escape(
            #          str(translator.translate(matrix_col_name, src='ja',  dest="ko").text))            
            #      variants.append(variant)
         elif matrix_row_name != "":
            matrix_row_option_name = option_values.get(matrix_row_name,[])
            print("matrix_row_option_name=")
            print(matrix_row_option_name)
            matrix_col_option_name = option_values.get(matrix_col_name,[])
            print("matrix_col_option_name=")
            print(matrix_col_option_name)
            idx = 0
            for ov in matrix_row_option_name:
               if len(ov) != len(ov.encode()) :
                  ov = escape(str(translator.translate(ov, src='ja',  dest="ko").text))
               ov = ov.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')          
               for ov2 in matrix_col_option_name:
                  if len(ov2) != len(ov2.encode()) :
                     ov2 = escape(str(translator.translate(ov2, src='ja',  dest="ko").text))
                  ov2 = ov2.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                  option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 999} # for test 3000, 999
                  if '×' in option_values['option_maxtrix_value'][idx] or '売り切れ' in option_values['option_maxtrix_value'][idx] or '入荷未定' in option_values['option_maxtrix_value'][idx]:
                     option_matrix[ov,ov2] = {'additional_amount': 0, 'stock': 0}
                  idx = idx + 1
                  print("option_matrix=")
                  print(option_matrix[ov,ov2])
            result['option_matrix'] = option_matrix
            if len(matrix_row_name) != len(matrix_row_name.encode()) :
               result['matrix_row_name'] = escape(
                         str(translator.translate(matrix_row_name, src='ja',  dest="ko").text))
            else :
               result['matrix_row_name'] = matrix_row_name
            if len(matrix_col_name) != len(matrix_col_name.encode()) :
               result['matrix_col_name'] = escape(
                         str(translator.translate(matrix_col_name, src='ja',  dest="ko").text))
            else :              
               result['matrix_col_name'] = matrix_col_name
         print("option_matrix=")
         print(option_matrix)
         print(option_names)
         if len(option_names) == 0:
            result["has_option"] = "F"
         else:
            result["has_option"] = "T"
            #result["option_names"] = [] 
            for op_n in option_names:
               if (op_n == matrix_row_name or op_n == matrix_col_name) and op_n != "" and op_n != '-' and processed != 1:
                  print("aaa") 
                  variant = {}
                  if len(op_n) != len(op_n.encode()) :
                     op_n_translated = escape(
                         str(translator.translate(op_n, src='ja',  dest="ko").text))
                  else :
                     op_n_translated = op_n 
                  #op_n_translated = op_n_translated.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                  result["option_names"].append(op_n_translated)
                  variant[op_n_translated] = []
                  
                  option_stock = 999
                  option_additional_price = 0
                  for op_v in option_values[op_n]:
                     if len(op_v) != len(op_v.encode()) :
                        op_v = escape(str(translator.translate(op_v, src='ja', dest="ko").text))
                     op_v = op_v.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                     if({'value': op_v, 'additional_amount': -1, 'stock': -1} not in variant[op_n_translated]) :
                        # op_v가 들어간건지 체크
                        variant[op_n_translated].append({'value': op_v, 'additional_amount': -1, 'stock': -1}) 
                     # for test -1, -1, 최초 옵션 등록시 필요해서 matrix도 넣음
                  variants.append(variant)

               elif op_n != '' and op_n != 'option_maxtrix_value' and op_n != matrix_row_name and op_n != matrix_col_name and ('商品レビュー募集中' not in op_n) and ('日時指定' not in op_n) and ('即納品納期' not in op_n) and ('取寄品納期' not in op_n)  and ('日本正規品モデル' not in op_n) and ('沖縄県・離島' not in op_n) and ('ヤマト営業所止' not in op_n) and ( '納期・発送予定日はあくまでも目安です' not in op_n) and ('次回送料無料レビューキャンペーン' not in op_n) and ('北海道/沖縄県は配送不可' not in op_n) and ('沖縄・一部離島への配送について' not in op_n) and ('沖縄県' not in op_n) and ('離島' not in op_n) and ('営業日' not in op_n) and ('配送日時の指定' not in op_n):
                  print("bbb") 
                  variant = {}
                  if len(op_n) != len(op_n.encode()) :
                     op_n_translated = escape(
                         str(translator.translate(op_n, src='ja',  dest="ko").text))
                  else :
                     op_n_translated = op_n
                  op_n_translated = op_n_translated.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                  result["option_names"].append(op_n_translated)
                  variant[op_n_translated] = []
                  option_stock = 999
                  option_additional_price = 0
                  for op_v in option_values[op_n]:
                     a = op_v
                     start1 = 0
                     if '円' in a:
                        print('option[' + a + ']')
                        start = a.rfind('+') + 1
                        end = a.rfind('円')
                        start1 = start
                        if start == 0 :
                           start = a.rfind('-') + 1
                           end = a.rfind('円')
                           start1 = start-1
                        option_additional_price_org = Price.fromstring(a[start:end]).amount_float   #jhlee additional_amount * 1.15
                        if 'None' in str(option_additional_price_org) :
                           start = a.rfind('（') + 1
                           end = a.rfind('）')
                           start1 = start
                           if start == 0 :
                              start = a.rfind('(') + 1
                              end = a.rfind(')')
                              start1 = start
                           option_additional_price_org = Price.fromstring(a[start:end]).amount_float   #jhlee additional_amount * 1.15

                        if '-' in a[start1:end]:
                           option_additional_price_org = option_additional_price_org * -1
    
                        print("option_additioal_price_org=" + str(option_additional_price_org))
                        
                        new_amount = amount + option_additional_price_org
                        
                        tariffRate = tariff_rate if (new_amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0;
                        taxRate = vat_rate if (new_amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0;
                        print("tariffRate=" + str(tariffRate))
                        print("taxRate=" + str(taxRate))
                        
                        supplyPrice = yen2krw * new_amount + delivery_charge_krw                        
                        print("supply price= "+str(supplyPrice)+"")

                        retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
                        print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")

                        margin = 0
                        margin = supplyPrice * margin_rate

                        if retailPriceWithoutMargin <= 700000:
                           margin = yen2krw * new_amount * 0.15
                           if margin < minimum_margin:
                              margin = minimum_margin
                        else:
                          margin = yen2krw * new_amount * 0.15
                          if margin < minimum_margin:
                             margin = minimum_margin

                        print("margin = "+str(margin)+"")
                        retailPrice = retailPriceWithoutMargin + margin
                        print("retailPrice = "+str(retailPrice)+"")
                  
                        option_retailPrice = round(retailPrice,-2)
                        print("4. option_supply_price = ["+ str(option_retailPrice)+"]")
                        
                        print('option_additional_price='+str(option_retailPrice)+' - '+result["price"])
                        option_additional_price = option_retailPrice-float(result["price"])   #jhlee
                        print('option_additional_price=' + str(option_additional_price))
                        
                        #result['price'] = str(round(retailPrice, -2))
                        #print("4. supply_price = [" + str(result['price'])+"]")
                        #result['supply_price'] = result['price']

                        #option_additional_price = Price.fromstring(a[start:end]).amount_float * yen2krw * 1.15   #jhlee additional_amount * 1.15
                        #print("option_additioal_price_org=" + str(option_additional_price_org))
                        #print("option_additioal_price=" + str(option_additional_price))
                        #if '-' in a[start:end]:
                        #    option_additional_price = option_additional_price * -1
                     if '選択してください' != op_v : #선택하세요 라는 텍스트
                        if len(op_v) != len(op_v.encode()) :
                           op_v = escape(str(translator.translate(op_v, src='ja', dest="ko").text))
                        op_v = op_v.replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')   
                        if({'value': op_v, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]) :
                           # op_v가 들어간건지 체크
                           variant[op_n_translated].append({'value': op_v, 'additional_amount': option_additional_price, 'stock': option_stock})
                  variants.append(variant)
         print("variants=")          
         print(variants)
         print("variants len=" + str(len(variants)))
         if len(variants) == 0 :
            result["has_option"] = "F"
         else :     
            result["variants"] = variants

         node_properties['stock'] = 999 #
         result['stock'] = node_properties['stock']  
         print("8. stock=["+ str(node_properties['stock']) +"]")  

         node_properties['mpid'] = str(site_code) + str(node_properties['mpid']).zfill(6)
         print("9. custom_product_code=["+str(node_properties['mpid'])+"]")
         result['custom_product_code'] = node_properties['mpid']
         result['add_category_no'] = [{"category_no": category_num, "recommend": "F", "new": "T"}]
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

         cleanr1 = re.compile('<iframe.*?>.*?</iframe>')
         cleanr2 = re.compile('<iframe.*?>.*?')
         cleanr3 = re.compile('</iframe>')
         cleanr5 = re.compile('<a.*?>')
         cleanr6 = re.compile('</a>')
         cleanr7 = re.compile('<font.*?>')
         cleanr8 = re.compile('<h.*?>')
         cleanr_garbagge = re.compile('<table[^>]*>\n<tbody><tr><td colspan="4"><img src="https://image.rakuten.co.jp/hayamimi/cabinet/img/img58097420.gif">[\s\S]*</table>')


         cleantext1 = re.sub(cleanr1, '', refined_description)
         cleantext2 = re.sub(cleanr2, '', cleantext1)
         cleantext3 = re.sub(cleanr3, '', cleantext2)
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

         #print('refined0')
         refined_description_last = refined_description_last.strip()
         #print('refined1')
         refined_description_last = refined_description_last.replace('</ ', '</').replace('< ', '<').replace('&amp',' ').replace('↑','').replace('→','').replace('指輪のサイズ表','').replace('<br>',' <br> ').replace('<b>','').replace('</b>','').replace('<h1>','').replace('</h1>','<br>').replace('<h2>','').replace('</h2>','<br>').replace('<h3>','').replace('</h3>','<br>').replace('<h4>','').replace('</h4>','<br>').replace('<h5>','').replace('</h5>','<br>').replace('</font>','').replace('https://www.rakuten.ne.jp/gold/f-netgolf/images/spacer.gif','')
         #print('refined2')
         refined_description_last = refined_description_last.strip().replace('<br><br><br>','').replace('※商品ページのサイズ表は海外サイズを日本サイズに換算した一般的なサイズとなりメーカー・商品によってはサイズが異なる場合もございます。サイズ表は参考としてご活用ください。','')
         #print('refined3')

         if refined_description_last != "":
            #print('1...') 
            description_content = "<div style='padding-left: 1em;font-size:12pt'>원본상품명: {}</div><br><br>".format(node_properties['p_name'])
            description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
            if node_properties['description'] is not None:
               #print('1.1..') 
               desc_con = ""
               print("refined_description_last")
               #print(refined_description_last)
               chunks, chunk_size = len(refined_description_last), 5000
               translated_batch = [str(translator.translate(refined_description_last[i:i+chunk_size], src='ja',  dest='ko').text) for i in range(0, chunks, chunk_size)]
               translated_str = ''.join(translated_batch)
               description_content += remove_delivery_string(remove_br(translated_str)).replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','').replace('↑','').replace('→','').replace('https : //','https://').replace('http : //','http://').replace(' /','/').replace('/ ','/').replace(' / ','/').replace(' .','.').replace('. ','.').replace(' . ','.')  + "<br>"
               #description_content += remove_delivery_string(remove_br(str(translator.translate(refined_description_last, src='ja', dest="ko").text))).replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','').replace('↑','').replace('→','') + "<br>"
               #print('1.1.1.') 
               print("refined_description_last end")
               utcnow = datetime.datetime.utcnow()
               time_gap = datetime.timedelta(hours=9)
               kor_time = utcnow + time_gap
               description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'],datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
               #print('1.2..') 
            else:
               #print('1.3..') 
               description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
            result['description'] = description_title + description_content
            #print('2...') 
            
         else:
            #print('3...')  
            description_content = "<div style='padding-left: 1em;font-size:12pt'>원본상품명: {}</div><br><br>".format(node_properties['p_name'])
            description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
            if node_properties['description'] is not None:
               #print('3.1..') 
               desc_con = ""
               description_content += refined_description_last.replace('</ ', '</').replace('< ', '<').replace('<h3>','').replace('</h3>','') + "<br>"
               utcnow = datetime.datetime.utcnow()
               time_gap = datetime.timedelta(hours=9)
               kor_time = utcnow + time_gap
               description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'],datetime.datetime.strptime(str(kor_time),"%Y-%m-%d %H:%M:%S.%f"), description_images)
               print('3.2..') 
            else:
               #print('3.3..') 
               description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
            result['description'] = description_title + description_content
            #print('4...')    
   
         print('end...')
     except Exception as e:
         print("tranform Error")
         print(e)
         print(str(traceback.format_exc()))
         raise
     return result

def escape( str ):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\"")
    return str

def remove_delivery_string( str ) :
    new_str = ''
    for desc in str.split('<br>'):
       if '배송' in desc:
          new_str += ''
       else:
          new_str += desc +'<br>'
    return new_str

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
