import csv
import datetime
from datetime import timedelta
from googletrans import Translator
from util.pse_errors import *
import ast


stock_zero_option_matrix_values = ['x', '売り切れ', '入荷未定']
invalid_op_names = ['商品レビュー募集中', '日時指定','即納品納期','取寄品納期','日本正規品モデル','沖縄県・離島','ヤマト営業所止','納期・発送予定日はあくまでも目安です','次回送料無料レビューキャンペーン','北海道/沖縄県は配送不可','沖縄・一部離島への配送について','沖縄県', '離島','営業日','配送日時の指定']
def user_defined_export(graph_mgr, node_id, node_properties):
    try:
        #################################
        # Parsing dictionaries
        # amazon
        node_properties['additional_info_dict'] = ast.literal_eval(
            node_properties.get('additional_info_dict', '{}'))
        print("aaa11")
        node_properties['technical_details_dict'] = ast.literal_eval(
            node_properties.get('technical_details_dict', '{}'))
        print("aaa12")
        node_properties['product_information_dict'] = ast.literal_eval(
            node_properties.get('product_information_dict', '{}'))
        print("aaa13")
        node_properties['product_details_dict'] = ast.literal_eval(
            node_properties.get('product_details_dict', '{}'))
        print("aaa14")
        node_properties['summary_technical_details_dict'] = ast.literal_eval(
            node_properties.get('summary_technical_details_dict', '{}'))
        print("aaa15")
        node_properties['other_technical_details_dict'] = ast.literal_eval(
            node_properties.get('other_technical_details_dict', '{}'))
        print("aaa16")

        remove_chars_from_keys_in_product_details_dict = '\n:'
        node_properties['product_details_dict'] = {
            key.rstrip(remove_chars_from_keys_in_product_details_dict): value for (key, value) in node_properties['product_details_dict'].items()
        }
        brand = node_properties['additional_info_dict'].get(
            'Manufacturer', 'none').lower().strip('\u200e')  # tools, apple watch
        if brand == 'none':
            brand = node_properties['additional_info_dict'].get(
                'Brand', 'none').lower().strip('\u200e')  # sony game
        if brand == 'none':
            brand = node_properties['additional_info_dict'].get(
                'Brand Name', 'none').lower().strip('\u200e')  # sony game
        if brand == 'none':
            brand = node_properties['technical_details_dict'].get(
                'Manufacturer', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['technical_details_dict'].get(
                'Brand', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['technical_details_dict'].get(
                'Brand Name', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['product_information_dict'].get(
                'Manufacturer', 'none').lower().strip('\u200e')  # pet-shaver, game
        if brand == 'none':
            brand = node_properties['product_information_dict'].get(
                'Brand', 'none').lower().strip('\u200e')
        if brand == 'none':
            brand = node_properties['product_information_dict'].get(
                'Brand Name', 'none').lower().strip('\u200e')
        if brand == 'none':
            for (key, value) in node_properties['product_details_dict'].items():
                if brand == 'none' and key == 'Manufacturer':
                    brand = value.lower()  # smart-watch
                if brand == 'none' and key == 'Brand':
                    brand = value.lower()  # smart-watch
                if brand == 'none' and key == 'Brand Name':
                    brand = value.lower()  # smart-watch
        if brand == 'none':
            brand = node_properties['summary_technical_details_dict'].get(
                'Manufacturer', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['summary_technical_details_dict'].get(
                'Brand', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['summary_technical_details_dict'].get(
                'Brand Name', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['other_technical_details_dict'].get(
                'Manufacturer', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['other_technical_details_dict'].get(
                'Brand', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = node_properties['other_technical_details_dict'].get(
                'Brand Name', 'none').lower().strip('\u200e')  # sony game, xbox
        if brand == 'none':
            brand = 'unknown'
        print("aaa4")

        # Jomashop
        node_properties['Dictionary1'] = ast.literal_eval(
            node_properties['Dictionary1'])
        node_properties['Dictionary2'] = ast.literal_eval(
            node_properties['Dictionary2'])
        node_properties['Dictionary3'] = ast.literal_eval(
            node_properties['Dictionary3'])
        node_properties['Dictionary4'] = ast.literal_eval(
            node_properties['Dictionary4'])
        node_properties['Dictionary5'] = ast.literal_eval(
            node_properties['Dictionary5'])
        if node_properties['Dictionary1'].get('dictionary_title0', None) == 'Information':
            result['brand'] = node_properties['Dictionary1']['Brand']  # store
        elif node_properties['Dictionary2'].get('dictionary_title0', None) == 'Information':
            result['brand'] = node_properties['Dictionary2']['Brand']  # store
        elif node_properties['Dictionary2'].get('dictionary_title0', None) == 'Information':
            result['brand'] = node_properties['Dictionary3']['Brand']  # store
        elif node_properties['Dictionary2'].get('dictionary_title0', None) == 'Information':
            result['brand'] = node_properties['Dictionary4']['Brand']  # store
        elif node_properties['Dictionary2'].get('dictionary_title0', None) == 'Information':
            result['brand'] = node_properties['Dictionary5']['Brand']  # store

        # Zalando
        node_properties['DictMaterialAndCare'] = ast.literal_eval(
            node_properties['DictMaterialAndCare'])
        node_properties['DictDetails'] = ast.literal_eval(
            node_properties['DictDetails'])
        node_properties['DictSizeAndFit'] = ast.literal_eval(
            node_properties['DictSizeAndFit'])

        #################################

        #################################
        # Set product_name, display, memo, brand, manufacturer(= manufacturer_code), site code
        bundle_price = node_properties.get(
            'bundle_price', 'none').lower().strip('\u200e')  # process bundle
        print("bundle_price="+str(bundle_price))
        if bundle_price != 'none' and str(bundle_price) != "":
            raise UserDefinedError(
                'BUNDLE PRODUCT ERROR: We can not process this product because of missing weights')

        result = {}
        result["memo"] = node_properties.get("url", "no url")
        print("0. url=["+str(result["memo"])+"]")

        result['display'] = 'T'  # store
        result['brand'] = node_properties['brand']

        translator = Translator()
        p_name = translator.translate(node_properties['name'], src='ja', dest="ko").text
        if '중고' in p_name:
            print('Used product (no error)')
            raise UserDefinedError('Used product (no error)')

        result['product_name'] = "[" + result['brand'] + "] " + \
            node_properties['name']
        # rakuten style
        #result['product_name'] = node_properties['brand'] + ' - ' + escape(p_name)
        print("product=["+str(result['product_name'])+"]")

        result['manufacturer'] = result['brand']
        result['manufacturer_code'] = result['manufacturer']


        #########################
        # Set pricing information
        pricing_information = node_properties["pricing_information"]

        dollar2krw = float(pricing_information["exchange_rate"])
        euro2krw = float(pricing_information["exchange_rate"])  # store
        yen2krw = float(pricing_information["exchange_rate"])/100
        # should be modified
        dollar2krw = float(pricing_information["dollar2krw"])

        minimum_margin = float(pricing_information["min_margin"])
        margin_rate = float(pricing_information["margin_rate"])
        tariff_rate = float(pricing_information["tariff_rate"])
        vat_rate = float(pricing_information["vat_rate"])
        #shipping_cost = pricing_information["shipping_cost"]
        tariff_threshold = float(pricing_information["tariff_threshold"])
        delivery_charge_list = node_properties['shipping_fee']
        for row in range(len(delivery_charge_list)):
            delivery_charge_list[row].append(float(dollar2krw))
            delivery_charge_list[row].append(
                int(delivery_charge_list[row][2] * delivery_charge_list[row][3]))

        # get weight information
        shipping_weight = node_properties['additional_info_dict'].get(
            'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight
        if shipping_weight == 'none':
            shipping_weight = node_properties['technical_details_dict'].get(
                'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight
        if shipping_weight == 'none':
            shipping_weight = node_properties['product_information_dict'].get(
                'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight
        if shipping_weight == 'none':
            shipping_weight = node_properties['product_details_dict'].get(
                'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight
        if shipping_weight == 'none':
            shipping_weight = node_properties['summary_technical_details_dict'].get(
                'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight
        if shipping_weight == 'none':
            shipping_weight = node_properties['other_technical_details_dict'].get(
                'Shipping Weight', 'none').lower().strip('\u200e')  # shipping weight

        item_weight = node_properties['additional_info_dict'].get(
            'Item Weight', 'none').lower().strip('\u200e')  # item weight
        if item_weight == 'none':
            item_weight = node_properties['technical_details_dict'].get(
                'Item Weight', 'none').lower().strip('\u200e')  # item weight
        if item_weight == 'none':
            item_weight = node_properties['product_information_dict'].get(
                'Item Weight', 'none').lower().strip('\u200e')  # item weight
        if item_weight == 'none':
            item_weight = node_properties['product_details_dict'].get(
                'Item Weight', 'none').lower().strip('\u200e')  # item weight
        if item_weight == 'none':
            item_weight = node_properties['summary_technical_details_dict'].get(
                'Item Weight', 'none').lower().strip('\u200e')  # item weight
        if item_weight == 'none':
            item_weight = node_properties['other_technical_details_dict'].get(
                'Item Weight', 'none').lower().strip('\u200e')  # item weight

        package_dimension_weight = node_properties['additional_info_dict'].get(
            'Package Dimensions', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['technical_details_dict'].get(
                'Package Dimensions', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['product_information_dict'].get(
                'Package Dimensions', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['product_details_dict'].get(
                'Package Dimensions', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['summary_technical_details_dict'].get(
                'Package Dimensions', 'none').lower().strip('\u200e')  # pet-shaver
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['other_technical_details_dict'].get(
                'Package Dimensions', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':  # pet-shaver
            package_dimension_weight = node_properties['additional_info_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['technical_details_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['product_information_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['product_details_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['summary_technical_details_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight
        if package_dimension_weight == 'none':
            package_dimension_weight = node_properties['other_technical_details_dict'].get(
                'Item Package Dimensions L x W x H', 'none').lower().strip('\u200e')  # item weight

        product_dimension_weight = node_properties['additional_info_dict'].get(
            'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['technical_details_dict'].get(
                'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_information_dict'].get(
                'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_details_dict'].get(
                'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['summary_technical_details_dict'].get(
                'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['other_technical_details_dict'].get(
                'Product Dimensions', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['additional_info_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['technical_details_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_information_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_details_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['summary_technical_details_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['other_technical_details_dict'].get(
                'Item Dimensions LxWxH', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['additional_info_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['technical_details_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_information_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['product_details_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['summary_technical_details_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight
        if product_dimension_weight == 'none':
            product_dimension_weight = node_properties['other_technical_details_dict'].get(
                'Size', 'none').lower().strip('\u200e')  # item weight

        print("0.1 product_dimension weight(kg)="+product_dimension_weight)
        print("0.1 package_dimension weight(kg)="+package_dimension_weight)

        # Calculate weight
        dweight = -1
        pro_dim_weight = -1
        pac_dim_weight = -1
        if product_dimension_weight != 'none' and 'x' in product_dimension_weight:
            dw = product_dimension_weight.split('x')
            dw2 = dw[2].split()
            if 'inch' in dw2[1]:
                pro_dim_weight = math.ceil(
                    float(dw[0])*float(dw[1])*float(dw2[0])/166) * 0.453592
                if len(dw2) > 3 and 'none' in item_weight:
                    if 'kg' in dw2[3]:
                        item_weight = dw2[2] + ' kg'
                    elif 'kilogram' in dw2[3]:
                        item_weight = dw2[2] + ' kg'
                    elif 'gram' in dw2[3]:
                        item_weight = dw2[2] + ' g'
                    elif 'g' in dw2[3]:
                        item_weight = dw2[2] + ' g'
                    elif 'pound' in dw2[3]:
                        item_weight = dw2[2] + ' pound'
                    elif 'ounce' in dw2[3]:
                        item_weight = dw2[2] + ' ounce'
                    print('item_weight=' + item_weight)

        if package_dimension_weight != 'none' and 'x' in package_dimension_weight:
            dw = package_dimension_weight.split('x')
            dw2 = dw[2].split()
            if 'inch' in dw2[1]:
                pac_dim_weight = math.ceil(
                    float(dw[0])*float(dw[1])*float(dw2[0])/166) * 0.453592
                if len(dw2) > 3 and 'none' in item_weight:
                    if 'kg' in dw2[3]:
                        item_weight = dw2[2] + ' kg'
                    elif 'kilogram' in dw2[3]:
                        item_weight = dw2[2] + ' kg'
                    elif 'gram' in dw2[3]:
                        item_weight = dw2[2] + ' g'
                    elif 'g' in dw2[3]:
                        item_weight = dw2[2] + ' g'
                    elif 'pound' in dw2[3]:
                        item_weight = dw2[2] + ' pound'
                    elif 'ounce' in dw2[3]:
                        item_weight = dw2[2] + ' ounce'
                    print('item_weight=' + item_weight)

        print("0.1 product dimension weight(kg)=["+str(
            pro_dim_weight)+"] and product dimension="+product_dimension_weight)
        print("0.1 pakcage dimension weight(kg)=["+str(
            pac_dim_weight)+"] and package dimension="+package_dimension_weight)

        dweight = pro_dim_weight
        if pro_dim_weight < pac_dim_weight:
            dweight = pac_dim_weight

        print("0.1 item_weight(kg)="+item_weight)
        print("0.1 shipping weight(kg)="+shipping_weight)
        print("0.1 dimensin weight(kg)="+str(dweight))
        print("0.1 default weight(kg)="+str(default_weight))
        result['pse_shipping_weight'] = str(shipping_weight)+""  # pse_store
        result['pse_item_weight'] = str(item_weight)+""  # pse_store
        result['pse_default_weight'] = str(default_weight)+""  # pse_store
        result['pse_dimension_weight'] = str(dweight)+""  # pse_store

        if shipping_weight == 'none':
            if item_weight == 'none':
                if dweight == -1:
                    # should be modified**************************************
                    weight = str(default_weight)+'kg'
                else:
                    weight = str(dweight)+'kg'
            else:
                weight = item_weight
        else:
            weight = shipping_weight

        r = re.compile(re.compile(r"\d+(\.\d*)?"))
        print("1. weight=["+str(weight)+"]")
        try:
            b = float(re.match(r, weight).group(0))
            if 'kg' in weight:
                weight = float(b)
            elif 'kilogram' in weight:
                weight = float(b)
            elif 'gram' in weight:
                weight = float(b) * 0.001
            elif 'g' in weight:
                weight = float(b) * 0.001
            elif 'pound' in weight:
                weight = float(b) * 0.453592
            elif 'ounce' in weight:
                weight = float(b) * 0.0283495
        except:
            weight = default_weight  # should be modified.

        print("weight=", weight, " dweight=", dweight)
        if weight < dweight:
            weight = dweight
        print("weight=", weight, " dweight=", dweight)
        # get weight information
        result['pse_weight'] = str(weight)+""  # pse_store

        default_weight = float(pricing_information["default_weight"])
        print("1. default_weight(kg)=["+str(default_weight)+"]")
        weight = node_properties.get("weight", '')
        weight = float(weight) if weight != '' else None
        print("1.1 weight(kg)=["+str(weight)+"]")
        result['pse_weight'] = str(weight)+""  # pse_store

        delivery_charge_krw = 0
        for charge in delivery_charge_list:
            if weight > charge[0] and weight <= charge[1]:
                delivery_charge_krw = charge[-1]
                break
        print("2. delivery_charge=["+str(delivery_charge_krw)+"]")
        result['pse_delivery_charge_krw'] = str(
            delivery_charge_krw)+""  # pse_store

        price = node_properties["price"]
        print("3. price=["+str(price)+"]")

        shipping_price = node_properties.get("shipping_price", "")
        print("3.0 shipping price=["+str(shipping_price)+"]")
        if "Free" in str(shipping_price):
            shipping_price = 0
        else:
            shipping_price = Price.fromstring(
                node_properties["shipping_price"]).amount_float
        print("3.1 shipping price=["+str(shipping_price)+"]")
        result['pse_shipping_price'] = str(shipping_price)+""  # pse_store

        amount = 0.0
        amount = Price.fromstring(price).amount_float
        if str(shipping_price) != "None":
            amount = amount + shipping_price
            print("3.4 price + shipping = sum=["+str(price) +
                  "]["+str(shipping_price)+"]["+str(amount)+"]")

        tariffRate = tariff_rate if (
            amount + delivery_charge_krw/dollar2krw) >= int(tariff_threshold) else 0
        taxRate = vat_rate if (
            amount + delivery_charge_krw/dollar2krw) >= int(tariff_threshold) else 0

        # for yen2krw
        tariffRate = tariff_rate if (
            amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0
        taxRate = vat_rate if (
            amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0

        result['pse_tariffRate'] = str(tariffRate)+""  # pse_store
        result['pse_taxRate'] = str(taxRate)+""  # pse_store

        supplyPrice = dollar2krw * amount + delivery_charge_krw
        supplyPrice = yen2krw * amount + delivery_charge_krw
        print("supply price= "+str(supplyPrice)+"")
        result['pse_supplyPrice'] = str(supplyPrice)+""  # pse_store

        #taxRate: bukase
        # modified by lee
        supplyPriceWithtariff = (supplyPrice) * (1 + tariffRate)
        print("supplyPriceWithtariff = "+str(supplyPriceWithtariff)+"")
        result['pse_supplyPriceWithtariff'] = str(
            supplyPriceWithtariff)+""  # pse_store

        individualtariff = 0
        print("individualtariff = "+str(individualtariff)+"")
        result['pse_individualtariff'] = str(individualtariff)+""  # pse_store

        edutariff = individualtariff * 0.3
        print("edutariff = "+str(edutariff)+"")
        result['pse_edutariff'] = str(edutariff)+""  # pse_store

        retailPriceWithoutMargin = (
            supplyPriceWithtariff + individualtariff + edutariff) * (1 + taxRate)
        print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
        result['pse_retailPriceWithoutMargin'] = str(
            retailPriceWithoutMargin)+""  # pse_store

        retailPriceWithoutMargin = (
            (supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
        print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
        result['pse_retailPriceWithoutMargin'] = str(
            retailPriceWithoutMargin)+""  # pse_store

        # if retailPriceWithoutMargin <= 700000:
        margin = dollar2krw * amount * 0.15
        if margin < minimum_margin:
            margin = minimum_margin
        # else:
        #    margin = dollar2krw * amount * 0.15
        #    if margin < minimum_margin:
        #       margin = minimum_margin
        # change 0.1 -> 0.15 by wshan request (2021.3.18)

        print("margin = "+str(margin)+"")
        result['pse_margin'] = str(margin)+""  # pse_store

        retailPrice = retailPriceWithoutMargin + margin
        print("retailPrice = "+str(retailPrice)+"")
        result['pse_retailPrice'] = str(retailPrice)+""  # pse_store

        result["price"] = str(round(retailPrice, -2))
        print("4. supply_price = [" + str(result["price"])+"]")
        result["supply_price"] = result["price"]
        #########################



        #########################
        print("5. brand_code = [" + str(node_properties['brand']) + "]")
        result["brand_code"] = node_properties['brand']
        #########################



        #########################
        print("6. images")
        print(node_properties["images"])

        # for rakuten
        img_list = []
        for idx, val in enumerate(node_properties['images']):
            if 'tshop.r10s' in node_properties['images'][idx] or 'downsize=' in node_properties['images'][idx]:
                if node_properties['images'][idx][-3:] == 'jpg':
                    node_properties['images'][idx] = node_properties['images'][idx] + \
                        '?&downsize=1000:*'
                else:
                    node_properties['images'][idx] = node_properties['images'][idx] + \
                        '&downsize=1000:*'
                img_list.append(node_properties['images'][idx].split('/')[-1].split('?')[0].split('.')[0])

        if len(node_properties["images"]) >= 2:
            result["detail_image"] = node_properties["images"][0]
            result["additional_image"] = node_properties["images"][1:]
        elif len(node_properties["images"]) == 1:
            result["detail_image"] = node_properties["images"][0]
        #########################

        #########################
        result["selling"] = "T"
        result["memo"] = node_properties.get("url", "no url")
        print("7. url=["+str(result["memo"])+"]")

        #########################
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

            # Check whether matrix option exist
            tmp_option_names = []
            for idx, op_n in enumerate(option_names):
                if op_n == 'option_matrix_col_name':
                    matrix_col_name = option_names[idx-1]
                    print("matrix_col_name : ", matrix_col_name)
                    print("len1 : ", str(len(matrix_col_name)))
                elif op_n == 'option_matrix_row_name':
                    matrix_row_name = option_names[idx-1]
                    print("matrix_row_name : ", matrix_row_name)
                    print("len2 : ", str(len(matrix_row_name)))
                else:
                    tmp_option_names.append(op_n)
            option_names = tmp_option_names

            # No matrix row in matrix option
            if (len(matrix_col_name) != 0 and ((len(matrix_row_name) == 1 and '-' in matrix_row_name) or len(matrix_row_name) == 0)):
                print("matrix_row_name is null or -")
                processed = 1
                print("matrix_row_name=["+str(matrix_row_name)+"]")
                print("len="+str(len(matrix_row_name)))
                print("matrix_col_name=["+str(matrix_col_name)+"]")
                print("len="+str(len(matrix_col_name)))
                variant = {}
                if len(matrix_col_name) != len(matrix_col_name.encode()):
                    op_n_translated = escape(str(translator.translate(matrix_col_name, src='ja', dest="ko").text))
                else:
                    op_n_translated = matrix_col_name
                result["option_names"].append(op_n_translated)
                variant[op_n_translated] = []
                option_stock = 999
                option_additional_price = 0

                print("option_values : ", option_values)

                matrix_row_option_name = option_values.get(matrix_row_name, [])
                print("matrix_row_option_name : ", matrix_row_option_name)
                matrix_col_option_name = option_values.get(matrix_col_name, [])
                print("matrix_col_option_name : ", matrix_col_option_name)

                idx = 0
                if len(matrix_row_option_name) != 0:
                    for ov in matrix_row_option_name:
                        if ov != '':
                            if len(ov) != len(ov.encode()):
                                ov = escape(str(translator.translate(ov, src='ja', dest="ko").text))
                        for ov2 in matrix_col_option_name:
                            if len(ov2) != len(ov2.encode()):
                                ov2 = escape(str(translator.translate(ov2, src='ja', dest="ko").text))

                            option_stock = 999
                            option_additional_price = 0
                            for om_value in stock_zero_option_matrix_values:
                                if om_value in option_values['option_maxtrix_value'][idx]:
                                    option_stock = 0
                                    option_additional_price = 0
                                    break;
                            #ov2 = ov2.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                            if({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]):
                                # ov2가 들어간건지 체크
                                variant[op_n_translated].append({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock})
                            idx = idx + 1
                    variants.append(variant)
                else:
                    for ov2 in matrix_col_option_name:
                        if len(ov2) != len(ov2.encode()):
                            ov2 = escape(str(translator.translate(ov2, src='ja', dest="ko").text))
                        option_stock = 999
                        option_additional_price = 0
                        for om_value in stock_zero_option_matrix_values:
                            if om_value in option_values['option_maxtrix_value'][idx]:
                                option_stock = 0
                                option_additional_price = 0
                                break;
                        #ov2 = ov2.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                        if({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]):
                            variant[op_n_translated].append({'value': ov2, 'additional_amount': option_additional_price, 'stock': option_stock})
                        idx = idx + 1
                    variants.append(variant)

            # No matrix col in matrix option
            elif (len(matrix_row_name) != 0 and ((len(matrix_col_name) == 1 and '-' in matrix_col_name) or len(matrix_col_name) == 0)):
                print("matrix_col_name is null or -")
                processed = 1
                print("matrix_col_name="+str(matrix_col_name))
                variant = {}
                if len(matrix_row_name) != len(matrix_row_name.encode()):
                    op_n_translated = escape(str(translator.translate(matrix_row_name, src='ja', dest="ko").text))
                else:
                    op_n_translated = matrix_row_name
                result["option_names"].append(op_n_translated)
                variant[op_n_translated] = []
                option_stock = 999
                option_additional_price = 0

                print("option_values : ", option_values)
                matrix_row_option_name = option_values.get(matrix_row_name, [])
                print("matrix_row_option_name : ", matrix_row_option_name)
                matrix_col_option_name = option_values.get(matrix_col_name, [])
                print("matrix_col_option_name : ", matrix_col_option_name)

                idx = 0
                if len(matrix_col_option_name) != 0:
                    for ov in matrix_row_option_name:
                        if len(ov) != len(ov.encode()):
                            ov = escape(str(translator.translate(ov, src='ja', dest="ko").text))
                        for ov2 in matrix_col_option_name:
                            if ov2 != '':
                                if len(ov2) != len(ov2.encode()):
                                    ov2 = escape(str(translator.translate(ov2, src='ja', dest="ko").text))
                            option_stock = 999
                            option_additional_price = 0
                            for om_value in stock_zero_option_matrix_values:
                                if om_value in option_values['option_maxtrix_value'][idx]:
                                    option_stock = 0
                                    option_additional_price = 0
                                    break
                            #ov = ov.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                            if({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]):
                                variant[op_n_translated].append({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock})
                            idx = idx + 1
                    variants.append(variant)
                else:
                    for ov in matrix_row_option_name:
                        if len(ov) != len(ov.encode()):
                            ov = escape(str(translator.translate(ov, src='ja', dest="ko").text))
                        option_stock = 999
                        option_additional_price = 0
                        for om_value in stock_zero_option_matrix_values:
                            if om_value in option_values['option_maxtrix_value'][idx]:
                                option_stock = 0
                                option_additional_price = 0
                                break;
                        #ov = ov.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                        if({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]):
                            variant[op_n_translated].append({'value': ov, 'additional_amount': option_additional_price, 'stock': option_stock})
                        idx = idx + 1
                    variants.append(variant)

            # Exist row and col in matrix option
            elif matrix_row_name != "":
                matrix_row_option_name = option_values.get(matrix_row_name, [])
                print("matrix_row_option_name : ", matrix_row_option_name)
                matrix_col_option_name = option_values.get(matrix_col_name, [])
                print("matrix_col_option_name : ", matrix_col_option_name)
                idx = 0
                for ov in matrix_row_option_name:
                    if len(ov) != len(ov.encode()):
                        ov = escape(str(translator.translate(ov, src='ja', dest="ko").text))
                    #ov = ov.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                    for ov2 in matrix_col_option_name:
                        if len(ov2) != len(ov2.encode()):
                            ov2 = escape(str(translator.translate(ov2, src='ja', dest="ko").text))
                        #ov2 = ov2.replace('"', '').replace("'", "").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                        option_matrix[ov, ov2] = {'additional_amount': 0, 'stock': 999}
                        for om_value in stock_zero_option_matrix_values:
                            if om_value in option_values['option_maxtrix_value'][idx]:
                                option_matrix[ov, ov2] = {'additional_amount': 0, 'stock': 0}
                                break;
                        idx = idx + 1
                        print("option_matrix=")
                        print(option_matrix[ov, ov2])
                result['option_matrix'] = option_matrix
                if len(matrix_row_name) != len(matrix_row_name.encode()):
                    result['matrix_row_name'] = escape(str(translator.translate(matrix_row_name, src='ja',  dest="ko").text))
                else:
                    result['matrix_row_name'] = matrix_row_name

                if len(matrix_col_name) != len(matrix_col_name.encode()):
                    result['matrix_col_name'] = escape(str(translator.translate(matrix_col_name, src='ja',  dest="ko").text))
                else:
                    result['matrix_col_name'] = matrix_col_name
            print("option_matrix : ")
            print(option_matrix)
            print(option_names)

            # Check whether list option exist
            for op_n in option_names:
                if op_n != '' and op_n != 'option_maxtrix_value' and op_n != matrix_row_name and op_n != matrix_col_name and (op_n not in invalid_op_names):
                    variant = {}
                    op_n_translated = ""
                    if len(op_n) != len(op_n.encode()):
                        op_n_translated = escape(str(translator.translate(op_n, src= 'ja', dest="ko").text))
                    else:
                        op_n_translated = op_n
                    result["option_names"].append(op_n_translated)
                    variant[op_n_translated] = []
                    option_stock = 999
                    option_additional_price = 0
                    for op_v in option_values[op_n]:
                        if '選択してください' != op_v:  # 선택하세요 라는 텍스트
                            a = op_v
                            if '円' in a:
                                start1 = 0
                                print('option[' + a + ']')
                                start = a.rfind('+') + 1
                                end = a.rfind('円')
                                start1 = start
                                if start == 0:
                                    start = a.rfind('-') + 1
                                    end = a.rfind('円')
                                    start1 = start-1
                                option_additional_price_org = Price.fromstring(a[start:end]).amount_float  
                                if 'None' in str(option_additional_price_org):
                                    start = a.rfind('（') + 1
                                    end = a.rfind('）')
                                    start1 = start
                                    if start == 0:
                                        start = a.rfind('(') + 1
                                        end = a.rfind(')')
                                        start1 = start
                                    option_additional_price_org = Price.fromstring(a[start:end]).amount_float  

                                if '-' in a[start1:end]:
                                    option_additional_price_org = option_additional_price_org * -1

                                print("[option] additioal_price_org= ", str(option_additional_price_org))

                                new_amount = amount + option_additional_price_org

                                tariffRate = tariff_rate if (new_amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0
                                taxRate = vat_rate if (new_amount * yen2krw + delivery_charge_krw) >= int(tariff_threshold) * dollar2krw else 0
                                supplyPrice = yen2krw * new_amount + delivery_charge_krw
                                retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
                                print("[option] tariffRate= " + str(tariffRate))
                                print("[option] taxRate= " + str(taxRate))
                                print("[option] supply price= "+str(supplyPrice)+"")
                                print("[option] retailPriceWithoutMargin = ", str(retailPriceWithoutMargin)+"")

                                margin = yen2krw * new_amount * margin_rate
                                if margin < minimum_margin:
                                    margin = minimum_margin

                                print("[option] margin = "+str(margin)+"")
                                retailPrice = retailPriceWithoutMargin + margin
                                print("[option] retailPrice = "+str(retailPrice)+"")

                                option_retailPrice = round(retailPrice, -2)
                                print("[option] option_supply_price = [" + str(option_retailPrice)+"]")
                                print('[option] option_additional_price= ' + str(option_retailPrice)+' - '+str(result["price"]))
                                option_additional_price = option_retailPrice - float(result["price"])  
                                print('[option] option_additional_price= ' + str(option_additional_price))

                            else:
                                start1 = 0
                                end1 = a.find('\n')
                                if end1 == -1:
                                    option_size = op_v
                                else:
                                    option_size = op_v[start1:end1]
                                    start2 = end1+1
                                    end2 = op_v.find('\n', start2)
                                    if end2 != -1:
                                        option_price = op_v[start2:end2]
                                    else:
                                        option_price = op_v[start2:]
                                    print('option_price=[' + option_price + ']')

                                    if '€' in option_price:
                                        amount = Price.fromstring(option_price).amount_float
                                        if str(shipping_price) != "None":
                                            amount = amount + shipping_price
                                        tariffRate = tariff_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw) >= int(tariff_threshold) else 0 
                                        taxRate = vat_rate if (amount*euro2dollar + delivery_charge_krw/dollar2krw) >= int(tariff_threshold) else 0 
                                        supplyPrice = euro2krw * amount + delivery_charge_krw 
                                        supplyPriceWithtariff = (supplyPrice) * (1 + tariffRate)
                                        individualtariff = (supplyPriceWithtariff-2000000)*0.2 if supplyPriceWithtariff > 2000000 else 0
                                        edutariff = individualtariff * 0.3
                                        retailPriceWithoutMargin = (supplyPriceWithtariff + individualtariff + edutariff) * (1 + taxRate)
                                        print("[option] tariffRate=" + str(tariffRate))
                                        print("[option] taxRate=" + str(taxRate))
                                        print("[option] supply price= "+str(supplyPrice)+"")
                                        print("[option] supplyPriceWithtariff = " +str(supplyPriceWithtariff)+"") 
                                        print("[option] individualtariff = " +str(individualtariff)+"") 
                                        print("[option] edutariff = "+str(edutariff)+"") 
                                        print("[option] retailPriceWithoutMargin = " + str(retailPriceWithoutMargin)+"")

                                        margin = euro2krw * amount * 0.15
                                        if margin < minimum_margin:
                                            margin = minimum_margin
                                        print("[option] margin = "+str(margin)+"") 

                                        retailPrice = retailPriceWithoutMargin + margin
                                        option_retailPrice = round(retailPrice, -2)
                                        print("[option] retailPrice = "+str(retailPrice)+"") 
                                        print("[option] option_supply_price = [" + str(option_retailPrice)+"]")
                                        print('[option] option_additional_price=' + str(option_retailPrice)+' - '+result["price"])
                                        option_additional_price = option_retailPrice - float(result["price"])  # jhlee
                                        print('[option] option_additional_price=' + str(option_additional_price))
                                        if end2 != -1:
                                            start3 = end2+1
                                            option_stock = op_v[start3:]
                                            print("option_org_stock=" + option_stock)
                                            if 'Notify' in option_stock:
                                                option_stock = 0
                                            elif 'Only' in option_stock:
                                                option_stock = int(Price.fromstring(option_stock).amount_float)
                                            else:
                                                option_stock = int(Price.fromstring(option_stock).amount_float)
                                    elif 'Notify' in option_price:
                                        option_additional_price = 0 
                                        option_stock = 0
                                    elif 'Only' in option_price:
                                        option_additional_price = 0
                                        option_stock = option_price
                                        option_stock = int(Price.fromstring(option_stock).amount_float)
                                    else:
                                        option_additional_price = 0
                                        option_stock = option_price
                                        option_stock = int(Price.fromstring(option_stock).amount_float)

                                print("[option] final_option_size=" + option_size)
                                print("[option] final_option_price=" + str(option_additional_price))
                                print("[option] final_option_stock=" + str(option_stock))
                            op_v = option_size
                            #if({'value': option_size, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n]):
                            #    if option_stock != 0:
                            #        variant[op_n].append({'value': option_size, 'additional_amount': option_additional_price, 'stock': option_stock})  

                            if len(op_v) != len(op_v.encode()):
                                op_v = escape(str(translator.translate(op_v, src='ja', dest="ko").text))

                            if({'value': op_v, 'additional_amount': option_additional_price, 'stock': option_stock} not in variant[op_n_translated]):
                                variant[op_n_translated].append({'value': op_v, 'additional_amount': option_additional_price, 'stock': option_stock})

                    variants.append(variant)
            print("variants : ", variants)
            print("variants len = " + str(len(variants)))
            if len(variants) != 0:
                result["variants"] = variants
        #########################

        #########################
        stock = node_properties.get("stock", 999)
        if stock is None:
            result["stock"] = 999
        result["stock"] = node_properties["stock"]
        print("8. stock=[" + str(node_properties["stock"]) + "]")

        site_code = node_properties["smpid"]
        result["mpid"] = node_properties["mpid"]
        node_properties["mpid"] = str(site_code)+str(node_properties["mpid"]).zfill(7)
        print("9. custom_product_code=["+str(node_properties["mpid"])+"]")
        result["custom_product_code"] = node_properties["mpid"]
        category_num = node_properties["cnum"]
        result["add_category_no"] = [{"category_no": category_num, "recommend": "F", "new": "T"}]
        #########################



        #########################
        description_title = "<div><div align=\"center\" style=\"font-size:14pt;font-weight:bold\">{}</div><br><br></div><br>".format(result["product_name"])
        description_images = "<center>"
        for image in node_properties["images"]:
            description_images += "<br><br><img style=\"width: 100%\" src=\"{}\"></img>".format(image)
        description_images = "<br>" + description_images + "</center>"

        description_content = "<div style=\"font-size:12pt; line-height:200%\">"

        # description detail for jomashop
        description_detail = ""
        if node_properties['Dictionary1'].get('dictionary_title0', None) is not None:
            description_detail += "<br>{}".format(
                node_properties['Dictionary1']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary1']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + \
                        node_properties['Dictionary1'][key] + "<br>"

        if node_properties['Dictionary2'].get('dictionary_title0', None) is not None:
            description_detail += "<br>{}".format(
                node_properties['Dictionary2']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary2']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + \
                        node_properties['Dictionary2'][key] + "<br>"

        if node_properties['Dictionary3'].get('dictionary_title0', None) is not None:
            description_detail += "<br>{}".format(
                node_properties['Dictionary3']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary3']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + \
                        node_properties['Dictionary3'][key] + "<br>"

        if node_properties['Dictionary4'].get('dictionary_title0', None) is not None:
            description_detail += "<br>{}".format(
                node_properties['Dictionary4']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary4']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + \
                        node_properties['Dictionary4'][key] + "<br>"

        if node_properties['Dictionary5'].get('dictionary_title0', None) is not None:
            description_detail += "<br>{}".format(
                node_properties['Dictionary5']['dictionary_title0'])
            description_detail += "<br>"
            for key in node_properties['Dictionary5']:
                if key != 'dictionary_title0':
                    description_detail += str(key) + ":  " + \
                        node_properties['Dictionary5'][key] + "<br>"



        # description detail for zalando
        description_detail = "<br>"
        description_detail += "<b>Material &amp; care</b>"
        description_detail += "<br>"
        print(node_properties["DictMaterialAndCare"])
        for key in node_properties["DictMaterialAndCare"]:
            if 'dictionary_title0' not in str(key):
                description_detail += "<b><span>" + \
                    str(key) + "</span></b> : " + \
                    node_properties["DictMaterialAndCare"][key] + "<br>"

        description_detail += "<br>"
        description_detail += "<b>Details</b>"
        description_detail += "<br>"
        print(node_properties["DictDetails"])
        for key in node_properties["DictDetails"]:
            if 'dictionary_title0' not in str(key):
                description_detail += "<b><span>" + \
                    str(key) + "</span></b> : " + \
                    node_properties["DictDetails"][key] + "<br>"

        description_detail += "<br>"
        description_detail += "<b>Size &amp; fit</b>"
        description_detail += "<br>"
        print(node_properties["DictSizeAndFit"])
        for key in node_properties["DictSizeAndFit"]:
            if 'dictionary_title0' not in str(key):
                description_detail += "<b><span>" + \
                    str(key) + "</span></b> : " + \
                    node_properties["DictSizeAndFit"][key] + "<br>"





        if node_properties["description"] != "None":
            refined_description = ""
            for description_str in node_properties['description'].split('<br>'):
                if "wr_rec" in description_str:
                    continue
                refined_description += description_str + "<br>"

            # amazon
            cleaner1_amazon = re.compile("<h1.*?>.*?</h1>")
            cleaner2_amazon = re.compile("<div class=\"?a-alert-content\"?>[\w\W]*?</div>")
            cleaner3_amazon = re.compile("<a class=\"?a-link-normal hsx-rpp-fitment-focus\"?[\w\W]*?>[\w\W]*?</span>")

            # rakuten
            cleaner1_rakuten = re.compile('<iframe.*?>.*?</iframe>')
            cleaner2_rakuten = re.compile('<iframe.*?>.*?')
            cleaner3_rakuten = re.compile('</iframe>')
            cleaner5_rakuten = re.compile('<a.*?>')
            cleaner6_rakuten = re.compile('</a>')
            cleaner7_rakuten = re.compile('<font.*?>')
            cleaner8_rakuten = re.compile('<h.*?>')
            cleaner9_rakuten = re.compile('<table[^>]*>\n<tbody><tr><td colspan="4"><img src="https://image.rakuten.co.jp/hayamimi/cabinet/img/img58097420.gif">[\s\S]*</table>')
            cleaner10_rakuten = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".jpg[\"']?[^>]*>")
            cleaner11_rakuten = re.compile("<img[^>]*src=[\"']?([^>\"']+)"+img+".gif[\"']?[^>]*>")
            cleaner12_rakuten = re.compile("<[^>]*>(.+?)"+sub_str+"(.+?)</[^>]*>")

            tmp_decription = re.sub(cleaner1_amazon, '', refined_description)
            tmp_decription = re.sub(cleaner2_amazon, '', tmp_description)
            tmp_decription = re.sub(cleaner3_amazon, '', tmp_description)
            tmp_decription = re.sub(cleaner1_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner2_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner3_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner4_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner5_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner6_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner7_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner8_rakuten, '', tmp_description)
            tmp_decription = re.sub(cleaner9_rakuten, '', tmp_description)

            # Delete duplicated images
            for img in img_list:
                tmp_decription = re.sub(cleaner10, '', tmp_decription)
                tmp_decription = re.sub(cleaner11, '', tmp_decription)

            # Delete delivey string
            for sub_str in ['配送', '出荷', '納期']:
                tmp_decription = re.sub(cleaner12, '', tmp_decription)

            tmp_decription = tmp_decription.strip()
            tmp_decription = tmp_decription.replace('</ ', '</').replace('< ', '<').replace('&amp', ' ').replace('↑', '').replace('→', '').replace('指輪のサイズ表', '').replace('<br>', ' <br> ').replace('<b>', '').replace('</b>', '').replace('<h1>', '').replace('</h1>', '<br>').replace('<h2>', '').replace('</h2>', '<br>').replace('<h3>', '').replace('</h3>', '<br>').replace('<h4>', '').replace('</h4>', '<br>').replace('<h5>', '').replace('</h5>', '<br>').replace('</font>', '').replace('https://www.rakuten.ne.jp/gold/f-netgolf/images/spacer.gif', '')

            tmp_decription = tmp_decription.strip().replace('<br><br><br>', '').replace('※商品ページのサイズ表は海外サイズを日本サイズに換算した一般的なサイズとなりメーカー・商品によってはサイズが異なる場合もございます。サイズ表は参考としてご活用ください。', '')
            description = tmp_decription



            if 'rakuten' in url:
                if description != "":
                    description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
                    if node_properties['description'] is not None:
                        desc_con = ""
                        chunks, chunk_size = len(description), 5000
                        translated_batch = [str(translator.translate(description[i:i+chunk_size], src='ja', dest='ko').text) for i in range(0, chunks, chunk_size)]
                        translated_str = ''.join(translated_batch)
                        description_content += remove_delivery_string(remove_br(translated_str)).replace('</ ', '</').replace('< ', '<').replace('<h3>', '').replace('</h3>', '').replace('↑', '').replace('→', '').replace('https : //', 'https://').replace('http : //', 'http://').replace(' /', '/').replace('/ ', '/').replace(' / ', '/').replace(' .', '.').replace('. ', '.').replace(' . ', '.') + "<br>"
                        utcnow = datetime.datetime.utcnow()
                        time_gap = datetime.timedelta(hours=9)
                        kor_time = utcnow + time_gap
                        description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'], datetime.datetime.strptime(str(kor_time), "%Y-%m-%d %H:%M:%S.%f"), description_images)
                    else:
                        description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
                    result['description'] = description_title + description_content

                else:
                    description_content += "<style> #innertable td{border:1px solid black} #innertable tr{border:1px solid black} #innertable th{border:1px solid black}</style><div id='innertable' style='padding-left: 1em;font-size:12pt; margin-top:10pt; line-height:200%'>"
                    if node_properties['description'] is not None:
                        desc_con = ""
                        description_content += description.replace('</ ', '</').replace('< ', '<').replace('<h3>', '').replace('</h3>', '') + "<br>"
                        utcnow = datetime.datetime.utcnow()
                        time_gap = datetime.timedelta(hours=9)
                        kor_time = utcnow + time_gap
                        description_content += "<br><br><br><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(node_properties['mpid'], datetime.datetime.strptime(str(kor_time), "%Y-%m-%d %H:%M:%S.%f"), description_images)
                    else:
                        description_content = "<div style='padding-left: 1em; margin-top:10pt'><br><br>{}</div>".format(description_images)
                    result['description'] = description_title + description_content

            else:
                description_content += description
                if node_properties["description2"] != "None":
                    input_description = node_properties['description2']
                    description = re.sub(cleaner1, "", input_description)
                    description_content += description

                if node_properties["description3"] != "None":
                    input_description = node_properties['description3']
                    description = re.sub(cleaner1, "", input_description)
                    description_content += description

                description_content += "<br>"
                utcnow = datetime.datetime.utcnow()
                time_gap = datetime.timedelta(hours=9)
                kor_time = utcnow + time_gap
                description_content += "<br>{}<br><br><br></center><label style='font-weight:bold'>PROD번호</label>: {}<br>정보업데이트: {}<br><br><br>{}</div>".format(description_detail, node_properties['mpid'], datetime.datetime.strptime(str(kor_time), "%Y-%m-%d %H:%M:%S.%f"), description_images)
        else:
            description_content = "<div style=\"padding-left: 1em; margin-top:10pt\"><br><br>{}</div>".format(description_images)
        result["description"] = description_title + description_content

        #########################
        print('end...')
    except Exception as e:
        print("tranform Error")
        print(e)
        print(str(traceback.format_exc()))
        raise
    return result


def escape(str):
    str = str.replace("&amp;", "&")
    str = str.replace("&lt;", "<")
    str = str.replace("&gt;", ">")
    str = str.replace("&quot;", "\"")
    return str


def remove_delivery_string(str):
    new_str = ''
    for desc in str.split('<br>'):
        if '배송' in desc:
            new_str += ''
        else:
            new_str += desc + '<br>'
    return new_str


def remove_br(str):
    refined_description_sale_tmp = ''
    cont = False
    for desc in str.split('<br>'):
        if (desc == '\n' or desc == '') and cont == False:
            cont = True
            refined_description_sale_tmp += desc + '<br>'
        elif (desc != '\n' and desc != ''):
            cont == False
            refined_description_sale_tmp += desc + '<br>'
        elif (desc == '\n' or desc == '') and cont == True:
            continue

    return refined_description_sale_tmp
