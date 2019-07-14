#!/usr/bin/env python3
__author__ = 'Hong Kai LEE'
version = '1.0'

# Import packages used commonly
import os, requests, time, urllib
import pandas as pd
from splinter import Browser


# the intraday price
ticker = "AAPL"

#define the endpoint
# endpoint = r"https://api.tdameritrade.com/v1/marketdata/quotes"
endpoint = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(ticker)

# define payload
payload = {'apikey': 'IPVNKJZGPDE5VINELGX3PCA6GWAJDX40',
            'periodType': 'year',
            'frequencyType': 'daily',
            'frequency': '1',
            'period': '2',
            # 'symbol': 'AAPL,AMZN,BABA,CHWY',
            'endDate': '1562083200000'}

# # defining main dataframe
# df_AAPL = pd.DataFrame()
# df_AMZN = pd.DataFrame()
# df_BABA = pd.DataFrame()
# df_CHWY = pd.DataFrame()

# interval = 0

# for i in range(22):
#     payload['startDate'] = int(payload['startDate'])
#     payload['startDate'] += interval
#     payload['startDate'] = str(payload['startDate'])
#     content = requests.get(url = endpoint, params = payload)
#     data = content.json()
#     df_tmp = pd.DataFrame.from_dict(data['candles'])
    
#     # appending retrieve data to main df
#     df = df.append(df_tmp)
#     interval += 900000
#     time.sleep(0.5)
content = requests.get(url = endpoint, params = payload)
data = content.json()
print(data)
df = pd.DataFrame.from_dict(data['candles'])
df.to_csv('Data/inputs/AAPL_eod.csv', sep=',')

# for i in range(46):
#     content = requests.get(url = endpoint, params = payload)
#     data = content.json()
#     df_tmp_AAPL = pd.DataFrame.from_dict(data['AAPL'], orient='index')
#     # df_AAPL = df_AAPL.append(df_tmp_AAPL.T, ignore_index=True)
#     df_tmp_AAPL.T.to_csv('AAPL_5min.csv', mode='a', header=False)
#     df_tmp_BABA = pd.DataFrame.from_dict(data['BABA'], orient='index')
#     # df_BABA = df_BABA.append(df_tmp_BABA.T, ignore_index=True)
#     df_tmp_BABA.T.to_csv('BABA_5min.csv', mode='a', header=False)
#     df_tmp_AMZN = pd.DataFrame.from_dict(data['AMZN'], orient='index')
#     # df_AMZN = df_AMZN.append(df_tmp_AMZN.T, ignore_index=True)
#     df_tmp_AMZN.T.to_csv('AMZN_5min.csv', mode='a', header=False)
#     df_tmp_CHWY = pd.DataFrame.from_dict(data['CHWY'], orient='index')
#     # df_CHWY = df_CHWY.append(df_tmp_CHWY.T, ignore_index=True)
#     df_tmp_CHWY.T.to_csv('CHWY_5min.csv', mode='a', header=False)
#     time.sleep(300)

