# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

##############
###Stocks
###############
path = os.path.expanduser("~/Documents/SOTrading/stock2/NASDAQ/") # r'C:\DRO\DCL_rawdata_files'                     # use your path
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent
df_from_each_file = (pd.read_csv(f) for f in all_files)
df_all   = pd.concat(df_from_each_file, ignore_index=True)
#############options

import numpy as np # I am importing numpy as np
import pandas as pd
import os
import timeit, time
import glob

##find all files in a directory and subfolder; glob and os.walk
#https://stackoverflow.com/questions/2186525/use-a-glob-to-find-files-recursively-in-python
#
path = os.path.expanduser("~/Documents/SOTrading")#'c:\\' /analysis
#extension = 'csv'
os.chdir(path)
#file_name_all = [i for i in glob.glob('*.{}'.format(extension))]
#cwd = os.getcwd()
#dir_path = os.path.dirname(os.path.realpath(__file__))

#SP100
SP100 = pd.read_csv("SP100_v2.csv")#SP500.head()
SP500 = pd.read_csv("SP500.csv")#SP100.shape
SP100 = SP500.loc[SP500.Symbol.isin(SP100.Symbol.values)]

#fields to read in
col_toread = ['Symbol', 'ExpirationDate', 'AskPrice', 'BidPrice',
       'LastPrice', 'PutCall', 'StrikePrice', 'Volume',
       'ImpliedVolatility',  'Vega', 'OpenInterest',
       'UnderlyingPrice', 'DataDate']

column_types = {   'AskPrice': 'float32',
    'BidPrice': 'float32',
    'ImpliedVolatility': 'float32',
    'LastPrice': 'float32',
    'OpenInterest': 'float32',
    'PutCall': 'category',
    'StrikePrice': 'float32',#'Symbol': 'category',
    'UnderlyingPrice': 'float32',
    'Vega': 'float32',
    'Volume': 'float32'}
#define file paths
path = os.path.expanduser("~/Documents/SOTrading/2017/2017_1") # r'C:\DRO\DCL_rawdata_files' 
os.chdir(path)
all_files_name = glob.glob('**/*.csv', recursive=True)         #file_name = os.walk(path)      # use your path
all_files = glob.glob(os.path.join(path, "**/*.csv"), recursive=True)     # advisable to use os.path.join as this makes concatenation OS independent

start_time = time.time()

#aggregate over time
appended_data = []
for (file_name, file_path) in zip(all_files_name, all_files):
    
    #file_name = '20170103_OData.csv'
    #file_path = os.path.expanduser("~/Documents/SOTrading/2017/firstday/"+file_name)
    
    

    data= pd.read_csv(file_path, usecols=col_toread,dtype=column_types,parse_dates=['ExpirationDate','DataDate'],infer_datetime_format=True) 
    #data= pd.read_csv(file_path, usecols=col_toread) 

    #data = option_df #data_sub.columns data_sub[:2] data_sub.dtypes data.head()

    #filter by SP500, then out of money
    data_sub = data.loc[data.Symbol.isin(SP100.Symbol.values)]
    data_sub['SminusP'] = data_sub.StrikePrice - data_sub.UnderlyingPrice
    data_sub = data_sub.loc[((data_sub.SminusP>=0)&(data_sub.PutCall=='call')|(data_sub.SminusP<0)&(data_sub.PutCall=='put'))]
    
    #define diff between strike and price, expiration and current data

    #data_sub['ExpirationDate'] = pd.to_datetime(data_sub.ExpirationDate)
    #data_sub['DataDate'] = pd.to_datetime(data_sub.DataDate)
    data_sub['ExpireMinusCurrent'] = (data_sub['ExpirationDate']-data_sub['DataDate']).dt.days
    
    #two lines for debugging in group_by
    #new_df = data_sub.groupby(("Symbol","ExpireMinusCurrent")).get_group(('AAPL',3))
    #ExpiryDate = new_df.ExpireMinusCurrent.iloc[0]
    #Symbol = new_df.Symbol.iloc[0]
    #new_df_symbol= data_sub.groupby(("Symbol")).get_group(('AAPL'))
    
    for Symbol, new_df_symbol in data_sub.groupby("Symbol"):
        #print(Symbol)
        appended_data_symbol = []
        for ExpiryDate, new_df in new_df_symbol.groupby(("ExpireMinusCurrent")):
            StockPrice = new_df.UnderlyingPrice.iloc[0].astype(np.float32)
            if (not (0 in new_df.SminusP.values)):
                #interpolate by at the money; new_df.dtypes
                new_df = new_df.append({'Symbol': new_df.Symbol.iloc[0], 'ExpirationDate':new_df.ExpirationDate.iloc[0], 
                   'StrikePrice': StockPrice, 
                   'UnderlyingPrice': StockPrice, 'DataDate': new_df.DataDate.iloc[0], 
                   'SminusP': 0, 
                   'ExpireMinusCurrent': new_df.ExpireMinusCurrent.iloc[0]}, ignore_index=True)
                new_df['ImpliedVolatility'] = new_df.set_index('StrikePrice').ImpliedVolatility.interpolate(method='index').values
            #drop by implied vol
            ImpVol_cutoff = 2*new_df.ImpliedVolatility[new_df.SminusP==0].values[0]*((ExpiryDate/251)**(1/2.0))
            new_df['ImpVol_cutoff'] = ImpVol_cutoff
            new_df_sub = new_df[(new_df.StrikePrice>StockPrice*(1-ImpVol_cutoff))&(new_df.StrikePrice<StockPrice*(1+ImpVol_cutoff))]
            #print([ExpiryDate,ImpVol_cutoff]) #DO NOT turn on print in the for loop, it will slow down a lot!!!
            appended_data_symbol.append(new_df_sub)
        
        appended_data_symbol_all = pd.concat(appended_data_symbol, axis=0)
        #create unique ID by order of expiry and strike
        appended_data_symbol_all = appended_data_symbol_all.reset_index(drop=True)
        
        indexed = {v: i for i, v in enumerate(sorted(set(appended_data_symbol_all['SminusP'])))}
        appended_data_symbol_all['SminusP_order'] = list(map(indexed.get,appended_data_symbol_all['SminusP']))            
        atm_order = appended_data_symbol_all.loc[appended_data_symbol_all.SminusP==0].SminusP_order.values[0]
        appended_data_symbol_all['SminusP_order'] = appended_data_symbol_all['SminusP_order'] - atm_order

        indexed = {v: i for i, v in enumerate(sorted(set(appended_data_symbol_all['ExpireMinusCurrent'])))}
        appended_data_symbol_all['ExpireMinusCurrent_order'] = list(map(indexed.get,appended_data_symbol_all['ExpireMinusCurrent']))            
        
        #string as object is very memory consuming and space consuming, concatenate last in the whole universe
        #appended_data_symbol_all['id']=appended_data_symbol_all['Symbol']+'_'+appended_data_symbol_all['ExpireMinusCurrent_order'].astype(str)+'_'+appended_data_symbol_all['SminusP_order'].astype(str)       
        #appended_data_symbol_all = appended_data_symbol_all.drop(columns=['ExpireMinusCurrent_order', 'SminusP_order'])
        
        appended_data.append(appended_data_symbol_all)
        
appended_data_all = pd.concat(appended_data, axis=0)

appended_data_all['MidPrice'] = (appended_data_all['AskPrice']+appended_data_all['BidPrice'])/2.0
#reduce memeory usage and space
appended_data_all.Symbol = appended_data_all.Symbol.astype('category')
appended_data_all.PutCall = appended_data_all.PutCall.astype('category')
gl_float = appended_data_all.select_dtypes(include=['float'])
appended_data_all[gl_float.columns] = gl_float.apply(pd.to_numeric,downcast='float')
gl_int = appended_data_all.select_dtypes(include=['int'])
appended_data_all[gl_int.columns] = gl_int.apply(pd.to_numeric,downcast='integer')
    
end_time = time.time()
print("total time taken this loop: ", end_time - start_time)

appended_data_all.to_csv('SP100_2017_01', index=False)
    
  
    
appended_data_all = appended_data_all.set_index(['DataDate','Symbol','ExpirationDate','StrikePrice'])
appended_data_all.groupby(['DataDate','Symbol']).count()

os.chdir('SP100')
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent
df_from_each_file = (pd.read_csv(f) for f in all_files)
df_all   = pd.concat(df_from_each_file, ignore_index=True)

df_all.columns

table = pd.pivot_table(df_all, values='Volume', index=['Symbol', 'ExpirationDate'], columns=['DataDate'], aggfunc=np.sum)
table.to_csv("SP100_2017_roll.csv")
################
#for file_name in file_name_all:#file_name=file_name_all[1]
#    file_path = os.path.expanduser("~/Documents/SOTrading/stock2/NASDAQ/AAPL.csv")
#    stock_ts_df = pd.read_csv(file_path)path+result[1]
#    print files 
    

file_path = os.path.expanduser("~/Documents/SOTrading/stock2/NASDAQ/AAPL.csv")
stock_ts_df = pd.read_csv(file_path)

file_path2 = os.path.expanduser("~/Documents/SOTrading/stock2/NASDAQ/AAL.csv")
stock_ts_df2 = pd.read_csv(file_path2)

df3 = pd.concat ([stock_ts_df,stock_ts_df2])
df3 = df3.set_index(['Ticker','Date'])

df3.loc['AAPL','Close'] #time series of one stock & one attribute

df3 = df3.sort_index()
df3.loc[(slice(None), slice(20140810:20140811)), :]
df3.loc[20150910]

df3.head()
stock_ts_df2.dtypes

df3.index()

pd.to_datetime(df3.loc[:5,'Date'])


        dtypes = appended_data_symbol_all.dtypes

        dtypes_col = dtypes.index
        dtypes_type = [i.name for i in dtypes.values]
        
        column_types = dict(zip(dtypes_col, dtypes_type))
        preview = first2pairs = {key:value for key,value in list(column_types.items())}
        import pprint
        pp = pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(preview)
        
        