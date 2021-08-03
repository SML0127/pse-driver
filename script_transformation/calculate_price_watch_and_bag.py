# amount = source site에서의 판매가 x 환율 
tariffRate = 0.08 if amount + delivery_charge/dollar2krw>= 200 else 0;
taxRate = 0.1 if amount + delivery_charge/dollar2krw>= 200 else 0;
supplyPrice = dollar2krw * amount + delivery_charge
print("supply price= "+str(supplyPrice)+"")
if supplyPrice >= 2000000:
    tariff_tax = supplyPrice * 0.08
    print("tariff tax= "+str(tariff_tax)+"")
    individual_tax = (supplyPrice - 2000000) * 0.2
    print("individual tax= "+str(individual_tax)+"")
    edu_tax = individual_tax * 0.3
    print("edu tax= "+str(edu_tax)+"")
    vat_tax = (supplyPrice + tariff_tax + individual_tax + edu_tax) * 0.1
    print("vat tax= "+str(vat_tax)+"")
    retailPriceWithoutMargin = supplyPrice + tariff_tax + individual_tax +edu_tax + vat_tax
    print("retailPriceWithoutMargin= "+str(retailPriceWithoutMargin)+"")
    margin = dollar2krw * amount * 0.1 #margin rate 10%
    final_price = retailPriceWithoutMargin + margin 
    print("retailPriceWithoutMargin + margin  = "+str(final_price)+"")
    result['price'] = str(round(final_price,-2))
else:
    retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
    print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")
    if retailPriceWithoutMargin <= 700000:
        margin = dollar2krw * amount  * 0.15
        if margin < 50000:
           margin = 50000
    else:
        margin = dollar2krw * amount * 0.1
        if margin < 50000:
           margin = 50000
    print("margin = "+str(margin)+"")
    retailPrice = retailPriceWithoutMargin + margin
    print("retailPrice = "+str(retailPrice)+"")
    result['price'] = str(round(retailPrice,-2))

print("4. supply_price = ["+ str(result['price'])+"]")
result['supply_price'] = result['price']


tariffRate = 0.13 if amount + delivery_charge/dollar2krw>= 200 else 0;
taxRate = 0.1 if amount + delivery_charge/dollar2krw>= 200 else 0;
supplyPrice = dollar2krw * amount + delivery_charge
print("supply price= "+str(supplyPrice)+"")

retailPriceWithoutMargin = ((supplyPrice) * (1 + tariffRate)) * (1 + taxRate)
print("retailPriceWithoutMargin = "+str(retailPriceWithoutMargin)+"")

if retailPriceWithoutMargin <= 700000:
    margin = dollar2krw * amount * 0.15 # margin rate 15%
    if margin < 50000:
       margin = 50000
else:
    margin = dollar2krw * amount * 0.1 # margin rate 10%
    if margin < 50000:
       margin = 50000

print("margin = "+str(margin)+"")
retailPrice = retailPriceWithoutMargin + margin
print("retailPrice = "+str(retailPrice)+"")

result['price'] = str(round(retailPrice,-2))
print("4. supply_price = ["+ str(result['price'])+"]")
result['supply_price'] = result['price']


