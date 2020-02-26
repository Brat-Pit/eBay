# -*- coding: utf-8 -*-
"""
"""
import numpy as np
import time
import math
from ebaysdk.finding import Connection as Connection_finding
from ebaysdk.shopping import Connection as Connection_shopping

#parameters to change
date_report='20200226' #date.today()
eur_rate=4.3008 #EUR to PLN rate
gbp_rate=5.0411 #GBP to PLN rate
usd_rate=3.9047 #USD to PLN rate
aud_rate=2.6494 #AUD to PLN rate
cad_rate=2.9386 #CAD to PLN rate
hkd_rate=0.5000 #HKD to PLN rate
chf_rate=4.0000 #CHF to PLN rate
category_ebay_id=177831 #ebay's bikes category id
price_breaks_no=1 #number of price intervals (downloading in portions needed: due to maximum daily queries limit)
price_min_min=400.00 #minimum price
price_min_max=500.00 #maximum price
export_data_to_txt=1 #1, if export downloaded data into txt; 0, if not
yaml_path='C:/Users/Public/ebay/eBay_API/ebay_1.yaml' #path: yaml file location
folder_txt='C:/Users/Public/ebay/scripts/python_txt/' #path: downloaded data location
item_title_index=13 #column number of array: title of ebay item (transformation needed because of 'txt file separator' conflicts)
count_product_rows=200000 #array no of rows
count_product_cols=33 #array no of columns
count_product_getmultipleitems_cols=6 #array no of columns
count_product_getmultipleitems_ItemSpecifics_cols=3 #array no of columns
getMultipleItems_per_call_max=20 #ebay internal restriction: maximum value is 20
max_pages_response_ebay=2 #ebay internal restriction: maximum value is 100
delay=0.5 #sleep interval

#currency table
a = np.array([["EUR",eur_rate]], dtype=object)
b = [["USD", usd_rate]]
c = [["GBP", gbp_rate]]
d = [["AUD", aud_rate]]
e = [["CAD", cad_rate]]
f = [["HKD", hkd_rate]]
g = [["CHF", chf_rate]]
curr_rates = np.vstack((a,np.asarray(b,object)))
curr_rates = np.vstack((curr_rates,np.asarray(c,object)))
curr_rates = np.vstack((curr_rates,np.asarray(d,object)))
curr_rates = np.vstack((curr_rates,np.asarray(e,object)))
curr_rates = np.vstack((curr_rates,np.asarray(f,object)))
curr_rates = np.vstack((curr_rates,np.asarray(g,object)))

#price breaks
price_interval=round((price_min_max-price_min_min)/price_breaks_no,2)
price_breaks = []
for i in range(0,price_breaks_no):
    price_breaks.append([])
    for j in range(0,2):
            price_breaks[i].append(0)
for b in range(0, price_breaks_no):
    if (b==0):
        price_breaks[b][0]=price_min_min
        price_breaks[b][1]=price_min_min+price_interval
    else:        
        price_breaks[b][0]=price_breaks[b-1][1]+0.01
        price_breaks[b][1]=price_breaks[b-1][1]+price_interval

#functions
def mid(s, offset, amount):
    return s[offset:offset+amount]
def modify_string(txt, string, string_out):
    output=''
    for z in range(len(txt)):
        sign=mid(txt,z,1)
        if sign==string:
            sign=string_out
        output=str(output)+str(sign)
    return output
def exists_in_txt(txt, txt_subset):
    return (txt_subset.lower() in txt.lower())
def encode_seller(seller_name):
    if (seller_name==None):
        return('')
    else:    
        return(mid(seller_name,0,2)+'**'+mid(seller_name,4,3)+'***')
def find_currency_rate(curr_symbol):
    answ='currency_rate_error'
    for c in curr_rates:
        if (curr_symbol==c[0]):
            answ=c[1]
    return(answ)        

#define arrays
product = []
for i in range(0,count_product_rows):
    product.append([])
    for j in range(0,count_product_cols):
            product[i].append(0)
product_getmultipleitems = []
for i in range(0,count_product_rows):
    product_getmultipleitems.append([])
    for j in range(0,count_product_getmultipleitems_cols):
            product_getmultipleitems[i].append(0)             
            product_getmultipleitems_ItemSpecifics = []
for i in range(0,count_product_rows):
    product_getmultipleitems_ItemSpecifics.append([])
    for j in range(0,count_product_getmultipleitems_ItemSpecifics_cols):
            product_getmultipleitems_ItemSpecifics[i].append(0)    
                       
#location countries list (download items located only in these countries)
locat_countries=['GB','DE','CN']            
#site countries list (download data only from below services)     
sites_id = [1 for a in range(0,3)]
sites_id[0]='EBAY-GB'
sites_id[1]='EBAY-FR'
sites_id[2]='EBAY-DE'

##################################   findItemsAdvanced   ###################################################  

counter_=-1
for site_id in sites_id: 
    api = Connection_finding(config_file=yaml_path, siteid=site_id, https=True)
    api_sh = Connection_shopping(config_file=yaml_path)#, https=True)
    if (site_id=='EBAY-DE'): 
        keyword_1= 'fahrrad'
        keywords_2= ['','']
    if (site_id=='EBAY-FR'): 
        keyword_1= 'vélo' 
        keywords_2= ['','']
    if (site_id=='EBAY-GB'): 
        keyword_1= 'bike'
        keywords_2= ['','']
    if (site_id=='EBAY-ES'): 
        keyword_1= 'bicicleta'
        keywords_2= ['','']
    if (site_id=='EBAY-IT'): 
        keyword_1= 'bicicletta'
        keywords_2= ['','']
    for country_loc in locat_countries:
        for p in price_breaks:
            price_min=p[0]
            price_max=p[1]
            request = {
                    'keywords': keyword_1,
                    'categoryId': category_ebay_id,
                    'itemFilter': [
                        {'name': 'Condition', 'value': 'New'},
                        {'name': 'HideDuplicateItems', 'value': 1},
                        {'name': 'MinPrice', 'value': price_min},
                        {'name': 'MaxPrice', 'value': price_max},
                        {'name': 'LocatedIn', 'value': country_loc}
                    ],
                    'descriptionSearch' : 0,
                    'paginationInput': [
                        {'entriesPerPage': 100}
                    ]
            }
            
            try:
                response = api.execute('findItemsAdvanced', request)               
                time.sleep(4*delay)
            except:
                time.sleep(delay)
                response = api.execute('findItemsAdvanced', request)               
                time.sleep(8*delay)
                    
            print(response.reply.paginationOutput)       
            count_pages = min(max_pages_response_ebay,int(response.reply.paginationOutput.totalPages)) #eBay condition max.100        
            for page in range(1,count_pages+1):                      
                print('Stage 1: findItemsAdvanced, site: ' + str(site_id) + ', country_loc: ', country_loc + ', price_min: ' + str(price_min) + ', price_max: ' + str(price_max) + ', page: ' + str(page))            
                request = {
                    'keywords': keyword_1,
                    'categoryId': category_ebay_id,
                    'itemFilter': [
                        {'name': 'Condition', 'value': 'New'},
                        {'name': 'HideDuplicateItems', 'value': 1},
                        {'name': 'MinPrice', 'value': price_min},
                        {'name': 'MaxPrice', 'value': price_max},
                        {'name': 'LocatedIn', 'value': country_loc}
                    ],
                    'descriptionSearch' : 0,
                    'paginationInput': [
                        {'entriesPerPage': 100, 'pageNumber': page}
                    ],
                    'outputSelector': 'SellerInfo'
                }
                
                try:
                    response = api.execute('findItemsAdvanced', request)               
                    time.sleep(delay)
                except:
                    time.sleep(delay)
                    response = api.execute('findItemsAdvanced', request)               
                    time.sleep(8*delay)
                    
                if (int(response.reply.searchResult._count))>0:
                    for item in response.reply.searchResult.item:
                        counter_=counter_+1                                
                        try:
                            pm=str(item.paymentMethod)
                        except: 
                            pm=''
                        try:
                            ship_cost_curr=str(item.shippingInfo.shippingServiceCost._currencyId)
                        except: 
                            ship_cost_curr=''
                        try:
                            ship_cost_value=item.shippingInfo.shippingServiceCost.value
                        except: 
                            ship_cost_value=''                
                        try:
                            watchCount=str(item.listingInfo.watchCount)
                        except: 
                            watchCount=''
                        try:
                            cond_disp_name=item.condition.conditionDisplayName
                        except: 
                            cond_disp_name=''                        
                        lok=item.location
                        lok.encode('utf-8')
                        product[counter_][0]=''
                        product[counter_][1]=site_id
                        product[counter_][2]=item.itemId
                        product[counter_][3]=item.globalId
                        product[counter_][4]=item.country
                        product[counter_][5]=lok
                        product[counter_][6]=str(item.primaryCategory.categoryId)
                        product[counter_][7]=item.primaryCategory.categoryName
                        product[counter_][8]=item.topRatedListing
                        product[counter_][9]=str(item.listingInfo.startTime)
                        product[counter_][10]=str(item.listingInfo.endTime)
                        product[counter_][11]=str(item.shippingInfo.shippingType)
                        product[counter_][12]=item.sellingStatus.currentPrice.value
                        product[counter_][13]=item.title
                        product[counter_][14]=item.sellingStatus.currentPrice._currencyId
                        product[counter_][15]=float(item.sellingStatus.currentPrice.value) * find_currency_rate(item.sellingStatus.currentPrice._currencyId) #price in PLN
                        product[counter_][16]=watchCount
                        product[counter_][17]=item.viewItemURL
                        product[counter_][18]=pm
                        product[counter_][19]=str(item.listingInfo.listingType)
                        product[counter_][20]=cond_disp_name
                        product[counter_][21]=item.sellerInfo.sellerUserName
                        product[counter_][22]=str(item.sellerInfo.positiveFeedbackPercent)
                        product[counter_][23]=item.location
                        product[counter_][24]=item.sellerInfo.feedbackScore
                        product[counter_][25]=item.sellerInfo.feedbackRatingStar
                        product[counter_][26]=item.condition.conditionId
                        product[counter_][27]=str(item.listingInfo.bestOfferEnabled)
                        product[counter_][28]=str(item.listingInfo.buyItNowAvailable)
                        product[counter_][29]=str(item.listingInfo.gift)
                        product[counter_][30]=item.isMultiVariationListing
                        #column 31: does title contain keywords_2?
                        product[counter_][32]=encode_seller(item.sellerInfo.sellerUserName)
   
    #mark records with keywords_2 in item.title
    for ind in range(0,counter_):
        if (product[ind][1]==site_id):
            search_success=1
            title=product[ind][item_title_index]
            for keyword_2 in keywords_2:        
                if(exists_in_txt(title,keyword_2)==False):
                    search_success=0
            product[ind][31]=search_success
    time.sleep(5)        

#leave only records with keywords_2 in item.title
product = [row for row in product if row[31]==1] 
counter_=len(product)   
    
#export to txt
if (export_data_to_txt==1):
    with open(folder_txt + 'ItemListing.txt', "w", encoding="utf-8") as txt_file1:
        for a in range(0,len(product)):            
            product[a][item_title_index]=modify_string(product[a][item_title_index],'|',' ') #conversion due to title issues with '|'
            product[a][20]=modify_string(product[a][20],'|',' ') #conversion due to title issues with '|'
            z = '|'.join(map(str, product[a]))
            line = z
            txt_file1.write("".join(line) + "\n") 

##################################   GetMultipleItems - Details   ###################################################

time.sleep(5)                   
cnt=-1
api = Connection_shopping(config_file=yaml_path)#, https=True)
packages_count=math.ceil(counter_/getMultipleItems_per_call_max)
for product_package in range(0,packages_count): #'getMultipleItems_per_call_max' items in package
    print('Stage 2: GetMultipleItems - Details. Progress: ' + str(product_package+1) + '/ ' + str(packages_count))
    package_size= min(getMultipleItems_per_call_max , counter_ - product_package*getMultipleItems_per_call_max)
    itemId_list = [1 for a in range(0,package_size)]
    for prod in range(0,package_size):
        itemId_list[prod]=product[product_package*getMultipleItems_per_call_max + prod][2]
    r={
       'ItemID': itemId_list,
       'IncludeSelector': 'Details'
    }
    response = api.execute('GetMultipleItems', r)
    time.sleep(3*delay)
    
    if package_size!=1:         
        for item in response.reply.Item:
            cnt=cnt+1
            product_getmultipleitems[cnt][0]=item.ItemID
            product_getmultipleitems[cnt][1]=item.Quantity
            product_getmultipleitems[cnt][2]=item.BidCount
            product_getmultipleitems[cnt][3]=item.QuantitySold
            product_getmultipleitems[cnt][4]=item.HitCount
            product_getmultipleitems[cnt][5]=item.QuantitySoldByPickupInStore
    else:
        cnt=cnt+1
        product_getmultipleitems[cnt][0]=response.reply.Item.ItemID
        product_getmultipleitems[cnt][1]=response.reply.Item.Quantity
        product_getmultipleitems[cnt][2]=response.reply.Item.BidCount
        product_getmultipleitems[cnt][3]=response.reply.Item.QuantitySold
        product_getmultipleitems[cnt][4]=response.reply.Item.HitCount
        product_getmultipleitems[cnt][5]=response.reply.Item.QuantitySoldByPickupInStore        
product_getmultipleitems=[row for row in product_getmultipleitems if row[0]!=0]
product_getmultipleitems=np.array(product_getmultipleitems) #numpy array  

#export to txt
if (export_data_to_txt==1):
    with open(folder_txt + 'ItemDetails.txt', "w", encoding="utf-8") as txt_file2:
        for a in range(0,len(product_getmultipleitems)):
            z = '|'.join(map(str, product_getmultipleitems[a]))
            line = z      
            txt_file2.write("".join(line) + "\n")    

##################################   GetMultipleItems - ItemSpecifics   ###################################################
        
cnt=-1
api = Connection_shopping(config_file=yaml_path)
packages_count=math.ceil(counter_/getMultipleItems_per_call_max)
for product_package in range(0,packages_count): #'getMultipleItems_per_call_max' items in package
    print('Stage 3: GetMultipleItems - ItemSpecifics. Progress: ' + str(product_package+1) + '/ ' + str(packages_count))
    package_size= min(getMultipleItems_per_call_max , counter_ - product_package*getMultipleItems_per_call_max)
    itemId_list = [1 for a in range(0,package_size)]
    for prod in range(0,package_size):
        itemId_list[prod]=product[product_package*getMultipleItems_per_call_max + prod][2]
    r={
       'ItemID': itemId_list,
       'IncludeSelector': 'ItemSpecifics'
    }
    response = api.execute('GetMultipleItems', r)
    time.sleep(delay)
        
    if package_size!=1:
        for item in response.reply.Item:
            #items withount ItemSpecifics may appear   
            if 'ItemSpecifics' in dir(item):            
                try:               
                    for spec in item.ItemSpecifics.NameValueList:
                        cnt=cnt+1
                        product_getmultipleitems_ItemSpecifics[cnt][0]=item.ItemID
                        product_getmultipleitems_ItemSpecifics[cnt][1]=spec.Name
                        product_getmultipleitems_ItemSpecifics[cnt][2]=spec.Value
                except:
                    cnt=cnt+1
                    product_getmultipleitems_ItemSpecifics[cnt][0]=item.ItemID
                    product_getmultipleitems_ItemSpecifics[cnt][1]=spec.Name
                    product_getmultipleitems_ItemSpecifics[cnt][2]=spec.Value  
    else:
        #only 1 item in package
        if 'ItemSpecifics' in dir(item):      
            try:               
                for spec in item.ItemSpecifics.NameValueList:
                    cnt=cnt+1
                    product_getmultipleitems_ItemSpecifics[cnt][0]=item.ItemID
                    product_getmultipleitems_ItemSpecifics[cnt][1]=spec.Name
                    product_getmultipleitems_ItemSpecifics[cnt][2]=spec.Value
            except:
                cnt=cnt+1
                product_getmultipleitems_ItemSpecifics[cnt][0]=item.ItemID
                product_getmultipleitems_ItemSpecifics[cnt][1]=spec.Name
                product_getmultipleitems_ItemSpecifics[cnt][2]=spec.Value                

product_getmultipleitems_ItemSpecifics=[row for row in product_getmultipleitems_ItemSpecifics if row[0]!=0]
product_getmultipleitems_ItemSpecifics=np.array(product_getmultipleitems_ItemSpecifics) #numpy array

#export to txt
if (export_data_to_txt==1):
    with open(folder_txt + 'ItemSpecifics.txt', "w", encoding="utf-8") as txt_file3:
        for a in range(0,len(product_getmultipleitems_ItemSpecifics)):
            z = '|'.join(map(str, product_getmultipleitems_ItemSpecifics[a]))
            line = z      
            txt_file3.write("".join(line) + "\n")  
            
print('product: ' + str(len(product)))
print('product_getmultipleitems_Details: ' + str(len(product_getmultipleitems)))
print('product_getmultipleitems_ItemSpecifics: ' + str(len(product_getmultipleitems_ItemSpecifics)))        
        

