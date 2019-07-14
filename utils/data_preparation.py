# importing library packages
import os, pickle, requests
import bs4 as bs
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import re

from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay

ts = TimeSeries(key='NBXHMWG8ZJ80E921', output_format='pandas', indexing_type='date', retries=100)

###==================================================================

# Defining class for US Non-Trading date
class USNonTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

    
# to get trading closed holidays
def get_trading_close_holidays(year):
    inst = USNonTradingCalendar()

    return inst.holidays(datetime(year-1, 12, 31), datetime(year, 12, 31))


# to get and save the companies listed in current sp500
def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[1].text
        tickers.append(ticker)
    
    # define today date
    todaydate = datetime.now().strftime('%Y%m%d')
    
    with open('sp500ticker_{}.pickle'.format(todaydate), 'wb') as f:
        pickle.dump(tickers, f)
    
    return tickers


# to get and save the companies listed in current sp500
def get_data(reload_sp500=False):
    
    init = 0
    
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open('sp500ticker.pickle', 'rb') as f:
            tickers = pickle.load(f)
            
    if not os.path.exists('eod_alphavantage'):
        os.makedirs('eod_alphavantage')
    
    for ticker in tickers:
        
        if init == 45:
            break
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
            
        try:
            if not os.path.exists('eod_alphavantage/{}.csv'.format(ticker)):
                df, meta_data = ts.get_daily_adjusted(ticker, outputsize='full')
                df.to_csv('eod_alphavantage/{}.csv'.format(ticker))
                print(ticker, 'okay')
                init += 1
            else:
                print('Already have {}'.format(ticker))
                
        except:
            print(ticker, '>>>>>>>> bad')
            init += 1
            continue


# to get and save the companies listed in current sp500
def get_data(reload_sp500=False):
    
    init = 0
    
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open('sp500ticker.pickle', 'rb') as f:
            tickers = pickle.load(f)
            
    if not os.path.exists('eod_alphavantage'):
        os.makedirs('eod_alphavantage')
    
    for ticker in tickers:
        
        if init == 45:
            break
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
            
        try:
            if not os.path.exists('eod_alphavantage/{}.csv'.format(ticker)):
                df, meta_data = ts.get_daily_adjusted(ticker, outputsize='full')
                df.to_csv('eod_alphavantage/{}.csv'.format(ticker))
                print(ticker, 'okay')
                init += 1
            else:
                print('Already have {}'.format(ticker))
                
        except:
            print(ticker, '>>>>>>>> bad')
            init += 1
            continue


# to get and save the S&P500 companies of interest (retrieved on 7th April 2019)
def get_data_targeted(csvdir_processed_startdate='2012-05-18'):

    todayDate = datetime.now().strftime('%Y-%m-%d')

    if not os.path.exists('eod_alphavantage/' + todayDate):
        os.makedirs('eod_alphavantage/' + todayDate)
    
    init = 0
    
    # list all the csv files in the target directory
    ###++++++++++++++++++++++++++++++++++++++++####
    csvs_directory = 'csvdir_processed/' + csvdir_processed_startdate \
                        + '_breakout/daily'
    
    # Filing number of unique ticker found in the csvs_directory
    csvFiles = [f for f in os.listdir(csvs_directory) if re.match(r'[A-Z]+\.csv', f)]
    tickerFile_re = re.compile(r'([A-Z]+)\.csv')
    tickers = []
    
    for ticker in csvFiles:
        tickers.append(tickerFile_re.findall(ticker))
    
    tickers_list = list(traverse(tickers))
    
    for ticker in tickers_list:
        
        if init == 100:
            break
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
        
        try:
            
            if not os.path.exists('eod_alphavantage/' + todayDate + '/{}.csv'.format(ticker)):
                df, meta_data = ts.get_daily_adjusted(ticker, outputsize='full')
                df.to_csv('eod_alphavantage/' + todayDate + '/{}.csv'.format(ticker))
                print(ticker, 'okay')
                init += 1
            else:
                print('Already have {}'.format(ticker))
                
        except:
            print(ticker, '>>>>>>>> bad')
            init += 1
            continue
            
            
            
# A function to prepare raw data for zipline bundles
def process_raw_data(tickers, startdate='2010-07-15'):
    
    if not os.path.exists('csvdir_processed/' + startdate + '/daily'):
        os.makedirs('csvdir_processed/' + startdate + '/daily')
    
    parsable_tickers = [ticker for ticker in tickers if ticker not in ['APTV', 'DOW']]
    
    for ticker in parsable_tickers:
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
        
        # read raw data
        df = pd.read_csv('eod_alphavantage/{}.csv'.format(ticker))
        
        # only include those data with the startdate data
        if startdate in set(df['date']):
            df.set_index('date', inplace=True)
            df.columns = ['open', 'high', 'low','close', 'adjusted close', 'volume', \
                          'dividend', 'split']
            df.drop('adjusted close', axis=1, inplace=True)
            df = df.loc[startdate:]
            df.iloc[:-1].to_csv('csvdir_processed/' + startdate + '/daily/{}.csv'.format(ticker))
        
        else:
            print(ticker + ' was excluded as its first tick was collected from ' + df['date'][0])
    

# A function to prepare raw data for breakout strategy input
def process_raw_data_breakout(tickers, startdate='2012-05-18'):
    
    if not os.path.exists('csvdir_processed/' + startdate + '_breakout/daily'):
        os.makedirs('csvdir_processed/' + startdate + '_breakout/daily')
    
    parsable_tickers = [ticker for ticker in tickers if ticker not in ['APTV', 'DOW']]
    
    for ticker in parsable_tickers:
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
        
        # read raw data
        df = pd.read_csv('eod_alphavantage/{}.csv'.format(ticker))
        
        # only include those data with the startdate data
        if startdate in set(df['date']):
            df.set_index('date', inplace=True)
            df.columns = ['open', 'high', 'low','close', 'adjusted close', 'volume', \
                          'dividend', 'split']
            df = df.loc[startdate:]
            df.to_csv('csvdir_processed/' + startdate + '_breakout/daily/{}.csv'.format(ticker))
        
        else:
            print(ticker + ' was excluded as its first tick was collected from ' + df['date'][0])
    

# A function to prepare raw data for breakout strategy input
def process_raw_data_breakout_appending(startdate='2012-05-18', enddate='2019-04-11'):

#     todayDate = datetime.now().strftime('%Y-%m-%d')

    init = 0
    
    # list all the csv files in the target directory
    ###++++++++++++++++++++++++++++++++++++++++####
    csvs_directory = 'csvdir_processed/' + startdate \
                        + '_breakout/daily'
    
    # Filing number of unique ticker found in the csvs_directory
    csvFiles = [f for f in os.listdir(csvs_directory) if re.match(r'[A-Z]+\.csv', f)]
    tickerFile_re = re.compile(r'([A-Z]+)\.csv')
    tickers = []
    
    for ticker in csvFiles:
        tickers.append(tickerFile_re.findall(ticker))
    
    tickers_list = list(traverse(tickers))
    
    for ticker in tickers_list:
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
        
        # read existing ticker dataset to update
        df = pd.read_csv(csvs_directory + '/{}.csv'.format(ticker), index_col='date')
        
        # read new ticker dataset to append
        df_to_append = pd.read_csv('eod_alphavantage/'+ enddate + '/{}.csv'.format(ticker), index_col='date')
        
        # standardize the order of columns of new dataset
        init_cols = ['1. open', '2. high', '3. low', '4. close', \
                     '5. adjusted close', '6. volume', '7. dividend amount', \
                     '8. split coefficient']
        df_to_append = df_to_append[init_cols]
        
        # standardize column names of new to match that of the existing datasets
        df_to_append.columns = ['open', 'high', 'low','close', 'adjusted close', 'volume', \
                          'dividend', 'split']
        
        # replace and append data!
        for idx in df_to_append.index:
            df.loc[idx, df_to_append.columns] = df_to_append.loc[idx, df_to_append.columns]
        
        # overwrite the existing csv
        df.to_csv('csvdir_processed/' + startdate + '_breakout/daily/{}.csv'.format(ticker))


        
# define traverse to unlist the lists in a list
def traverse(o, tree_types=(list, tuple)):
    if isinstance(o, tree_types):
        for value in o:
            for subvalue in traverse(value, tree_types):
                yield subvalue
    else:
        yield o


# a function to combine all ticker csv's from a starting date
def combine_csvs(startdate='2012-05-18'):
    
    # define all the column names for the dataframe
    ###++++++++++++++++++++++++++++++++++++++++####
    init_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adjusted close', \
                 'volume', 'dividend', 'split']
    
    # initiate a new data frame with column names defined
    df = pd.DataFrame(columns=init_cols)
     
    
    # list all the csv files in the target directory
    ###++++++++++++++++++++++++++++++++++++++++####
    csvs_directory = 'csvdir_processed/' + startdate + '_breakout/daily'
    
    # Filing number of unique ticker found in the csvs_directory
    csvFiles = [f for f in os.listdir(csvs_directory) if re.match(r'[A-Z]+\.csv', f)]
    tickerFile_re = re.compile(r'([A-Z]+)\.csv')
    tickers = []
    
    for ticker in csvFiles:
        tickers.append(tickerFile_re.findall(ticker))
    
    tickers_list = list(traverse(tickers))
    
    # read and append each csv file to the main df
    ###++++++++++++++++++++++++++++++++++++++++####    
    for ticker in tickers_list:
        
        # resolve discordance of the BF.B ticker naming between wikipedia and AV
        if ticker == 'BF.B':
            ticker = 'BF-B'
        
        # reading and appending
        df_temp = pd.read_csv('csvdir_processed/' + startdate + \
                              '_breakout/daily/{}.csv'.format(ticker))
        df_temp['ticker'] = [ticker for ti in range(len(df_temp))]
        df_temp = df_temp[init_cols]
        df = df.append(df_temp, ignore_index=True)
    
    # check or create the file directories
    if not os.path.exists('csvdir_processed/combined_csvs'):
        os.makedirs('csvdir_processed/combined_csvs')
    
    # export df to csv file
    df.set_index('ticker', inplace=True)
    df.to_csv('csvdir_processed/combined_csvs/combined_sp500.csv'.format(ticker))
    
    
############################################################################################
## Functions not in use now
############################################################################################

# A function to fill in the gap
def filling_gaps_of_raw_data(ticker, missingDateStr):
    
    # resolve discordance of the BF.B ticker naming between wikipedia and AV
    if ticker == 'BF.B':
        ticker = 'BF-B'
    
    df = pd.read_csv('csvdir_processed/daily/{}.csv'.format(ticker))
    df.set_index('date', inplace=True)
    
    nyse = mcal.get_calendar('NYSE')
    tradingDays = nyse.schedule(start_date='1998-11-01', end_date='2019-07-10')
    tradingDays_str_array = mcal.date_range(tradingDays, frequency='1D').strftime('%Y-%m-%d')
    missingdateindex = np.where(tradingDays_str_array==missingDateStr)
    
    # previous day of missing date
    print(missingdateindex)
    previousDateindex=missingdateindex[0][0] - 1
    previousDate = tradingDays_str_array[previousDateindex]
    
    # next day of missing date
    nextDateindex=missingdateindex[0][0] + 1
    nextDate = tradingDays_str_array[nextDateindex]

    # preparing data in dataframe for the missing date
    df_missing = pd.DataFrame(columns = df.columns)
    df_missing = df_missing.append((df.loc[previousDate] + df.loc[nextDate]) / 2, ignore_index=True)
    df_missing.index = [missingDateStr]
    df_missing.index.name = 'date'
    
    # merging and saving the missing date and main dataframe  
    df_final = pd.concat([df, df_missing]).sort_index()
    df_final.to_csv('csvdir_processed/daily/{}.csv'.format(ticker))
    
    print('Data gap filled for ' +  ticker + ' on missing date ' + missingDateStr)
    


# ingestion by zipline individually 
def ingest_by_zipline(ticker, delta=0):
    
    # resolve discordance of the BF.B ticker naming between wikipedia and AV
    if ticker == 'BF.B':
        ticker = 'BF-B'

    df = pd.read_csv('csvdir_processed/daily/{}.csv'.format(ticker))
    startDate_str = df.iloc[0]['date']
    endDate_obj = datetime.strptime(df.iloc[-1]['date'], '%Y-%m-%d') 
    endDate_final_obj = endDate_obj - timedelta(days=delta)
    endDate_str = endDate_final_obj.strftime('%Y-%m-%d')
    
    with open(os.path.expanduser('~/.zipline/extension.py'), 'r') as fp:
        content = fp.read()
        
    content_n1 = re.sub("start\_session \= pd\.Timestamp\(\'[0-9]{4}\-[0-9]+-[0-9]+", \
                         "start_session = pd.Timestamp(\'" + startDate_str, content, flags = re.M)
    content_n2 = re.sub("end\_session \= pd\.Timestamp\(\'[0-9]{4}\-[0-9]+-[0-9]+", \
                         "end_session = pd.Timestamp(\'" + endDate_str, content_n1, flags = re.M)
    
    with open(os.path.expanduser('~/.zipline/extension.py'), 'w') as fo:
        fo.write(content_n2)
