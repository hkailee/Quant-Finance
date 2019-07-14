#!/usr/bin/env python3
__author__ = 'Hong Kai LEE'
version = '1.0'

# Import packages used commonly
import os, sys, time, logging

import bs4 as bs
from collections import Counter
import datetime
import os
import numpy as np
import pandas as pd
import pickle
import requests
import pandas_market_calendars as mcal
import shutil
from time import sleep

# Import supplementary data import code tools.py
from utils.sqlitedb import *
from utils.data_preparation import get_trading_close_holidays, process_raw_data, \
        process_raw_data_breakout, ingest_by_zipline, filling_gaps_of_raw_data, \
        combine_csvs, get_data_targeted, traverse, process_raw_data_breakout_appending

from alpha_vantage.timeseries import TimeSeries

ts = TimeSeries(key='NBXHMWG8ZJ80E921', output_format='pandas', indexing_type='date', \
                retries=100)

#######################################################################################

##Functions:
# Checks if in proper number of arguments are passed gives instructions on proper use.
def argsCheck(numArgs):
	if len(sys.argv) < numArgs or len(sys.argv) > numArgs:
		print('DataPrep version 1.0')
		print('Usage:', sys.argv[0], '<Last Trading Date>')
		print('Examples:', sys.argv[0], '2019-04-18')
		exit(1) # Aborts program. (exit(1) indicates that an error occurred)
                
# Arguments check
argsCheck(2) # Checks if the number of arguments are correct.

# Stores file one for input checking.
lastTradingDate  = sys.argv[1]

print('You are downloading data collected as of', \
      datetime.datetime.now().strftime('%Y-%m-%d'))

print('The last trading date for data preparation you are interested is', \
      lastTradingDate)

# Logging events...
logging.basicConfig(filename='Logs/log_' + datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S') + \
                    '.txt', level=logging.INFO, \
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info('Command-line invocation: ' + sys.argv[0] + ' ' + sys.argv[1])

#===================================
# getting data as of date
for i in range(30):
    print('This is step', i)
    get_data_targeted()
    sleep(60)

logging.info('Data Query job completed.')
print('Data Query job completed.')
      
    
# #===================================
# # appending the new raw data to breakout data csvs
# process_raw_data_breakout_appending(enddate=lastTradingDate)

# logging.info('Data Appending job completed.')
# print('Data Appending job completed.')


# #===================================
# # Combining raw data into one csv
# combine_csvs()

# logging.info('Data combining job completed.')
# print('Data combining job completed.')
