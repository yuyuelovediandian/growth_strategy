# -*- coding: utf-8 -*-
"""
Created on Sat Mar  4 11:02:12 2017

@author: Administrator
"""

import tushare as ts
import pandas as pd
import sys
import os 
import numpy as np
import statsmodels

os.chdir('C:/Users/sjl_yy/Documents/Python Scripts')
#返回中证500股票每月价格，时间序列
def pre_process_data():
    
    df_sz500_price = pd.read_excel('price_data.xlsx','Sheet1')
    
    df_sz500_price = df_sz500_price.fillna(0)
    index1 = list(df_sz500_price['index'])
    df_sz500_price = df_sz500_price.drop(['index'],axis = 1)
    return df_sz500_price,index1
#返回中证500股票每月收益率
def return_data(df_sz500_price,index_date):
    stocks = df_sz500_price.columns
    #print (df_sz500_price['000006'][1])
    return_data = pd.DataFrame(index = index_date, columns = stocks)
    for stk in stocks:
        i = 0
        for date in index_date[0:96]:
            if date == '2009-01-22' or df_sz500_price[stk][i+1] == 0:
                return_data[stk][date] = 0
            else:
                return_data[stk][date] = df_sz500_price[stk][i]/df_sz500_price[stk][i+1] -1
            i += 1
    return return_data
#获取中证500股票每月价格，并导入到price_data文件中
def get_sz500_month_close_price():
    df_sz500 = ts.get_zz500s()
    sz500_code = list(df_sz500.sort(columns = ['code'], ascending = ['True'])['code'])
    
    df_kdata = ts.get_hist_data(sz500_code[0],start = '2009-01-01',end = '2016-12-31',ktype = 'M')[['open','close']]
    df_kdata.columns = ['open',sz500_code[0]]
    df_kdata = df_kdata[[1]]
    df_kdata['index'] = df_kdata.index
    
    for code in sz500_code[1:]:
        df_kdata1 = ts.get_hist_data(code,start = '2009-01-01',end = '2016-12-31',ktype = 'M')[['open','close']]
        df_kdata1.columns = ['open',code]
        df_kdata1 = df_kdata1[[1]]
        df_kdata1['index'] = df_kdata1.index
        df_kdata = df_kdata.merge(df_kdata1,on = 'index',how = 'left')
    df_kdata.to_excel('price_data.xlsx',encoding='gbk')
#获取上证500市值数据
def get_sz500_market_capital():
#==============================================================================
#     df_sz500_price,index_date = pre_process_data()
#     stocks = list(df_sz500_price.columns)
#     df_sz500_basics = ts.get_stock_basics()
#==============================================================================
    df_market_cap = pd.read_excel('basic_information.xlsx','Sheet1')
    index_date = list(df_market_cap['index'])
    df_market_cap = df_market_cap.drop(['index'],axis=1)
    stocks = df_market_cap.columns
    #print(index_date)
    df_market_cap1 = pd.DataFrame(index = index_date, columns = stocks)
    for stk in stocks:
        i = 0
        for date in index_date:
#==============================================================================
#             print(float(df_market_cap[stk][i])<=0)
#             break
#==============================================================================
            if float(df_market_cap[stk][i]) <= 0:
                df_market_cap1[stk][date] = 0.0
            else:
                df_market_cap1[stk][date] = float(df_market_cap[stk][i])
            i += 1
    return df_market_cap1
#获取上证500市盈率数据
def get_sz500_pe():
    df_pe = pd.read_excel('pe_information.xlsx','Sheet1')
    index_date = list(df_pe['index'])
    df_pe = df_pe.drop(['index'],axis=1)
    stocks = df_pe.columns
    df_pe1 = pd.DataFrame(index = index_date, columns = stocks)
    for stk in stocks:
        i = 0
        for date in index_date:
            if float(df_pe[stk][i]) <= 0:
                df_pe1[stk][date] = 0.0
            else:
                df_pe1[stk][date] = float(df_pe[stk][i])
            i += 1
    return df_pe1

def process_BMS(df_market_cap,df_return_data):
    big_percentage = {}
    sma_percentage = {}
    index_date = df_market_cap.index
    stocks = df_market_cap.columns
    smb = pd.Series(index = index_date)
    #len_row = len(list(df_market_cap.index))
    for date in index_date:
        big_percentage[date] = np.percentile(df_market_cap.loc[date],70)
        sma_percentage[date] = np.percentile(df_market_cap.loc[date],30)
    #print(sma_percentage)
    for date in index_date:
#==============================================================================
#         if first_row:
#             smb[date]=0
#             hmi[date]=0
#             first_row=False
#             continue
#==============================================================================
        small_size = 0.0
        big_size = 0.0
        for stk in stocks:
            #print(df_market_cap[stk][date] <= sma_percentage[date])
            if df_market_cap[stk][date] <= sma_percentage[date]:
                small_size = small_size + df_return_data[stk][date] * df_market_cap[stk][date]
            elif df_market_cap[stk][date] >= big_percentage[date]:
                big_size = big_size + df_return_data[stk][date] * df_market_cap[stk][date]
        market_cap = np.sum(df_market_cap.loc[date])
        smb[date] = (big_size - small_size)/market_cap
    return smb
def process_HMI(df_pe,df_market_cap,df_return_data):
    big_percentage = {}
    sma_percentage = {}
    index_date = df_pe.index
    stocks = df_pe.columns
    hmi = pd.Series(index = index_date) 
    #len_row = len(list(df_market_cap.index))
    for date in index_date:
        big_percentage[date] = np.percentile(df_pe.loc[date],70)
        sma_percentage[date] = np.percentile(df_pe.loc[date],30)
    for date in index_date:
#==============================================================================
#         if first_row:
#             smb[date]=0
#             hmi[date]=0
#             first_row=False
#             continue
#==============================================================================
        small_pe = 0.0
        big_pe = 0.0
        for stk in stocks:
            #print(df_market_cap[stk][date] <= sma_percentage[date])
            if df_pe[stk][date] <= sma_percentage[date]:
                small_pe = small_pe + df_return_data[stk][date] * df_market_cap[stk][date]
            elif df_pe[stk][date] >= big_percentage[date]:
                big_pe = big_pe + df_return_data[stk][date] * df_market_cap[stk][date]
        market_cap = np.sum(df_market_cap.loc[date])
        hmi[date] = (big_pe - small_pe)/market_cap
    #print(hmi)
    return hmi
    
    
    
 
    
    
if __name__ == '__main__':
   df_sz500_price,index = pre_process_data()
   df_return_data = return_data(df_sz500_price,index)
   df_market_cap = get_sz500_market_capital()
   df_pe = get_sz500_pe()
   BMS = process_BMS(df_market_cap,df_return_data)
   HMI = process_HMI(df_pe,df_market_cap,df_return_data)
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   