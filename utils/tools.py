import pandas_datareader.data as web

# Retrieving ticker's data
def data_reader(ticker, start, end):  
    try:
        #print('Retriving data for', ticker, 'from quantdl')
        data = web.DataReader(ticker, 'quantdl', start, end)
    except:
        #print('iex failed, trying iex...')
        try:
            data = web.DataReader(ticker, 'iex', start, end)
        except:
            #print('Google Finance failed, trying moningstar...')
            try:
                data = web.DataReader(ticker, 'morningstar', start, end)
            except:
                #print('iex failed, trying google finance...')
                try:
                    data = web.DataReader(ticker, 'google', start, end)
                except:
                    #print('iex quandl, trying fred...')
                    try:
                        data = web.DataReader(ticker, 'fred', start, end)
                    except:
                        #print('fred quandl, trying MOEX...')
                        try:
                            data = web.DataReader(ticker, 'MOEX', start, end)
                        except:
                            print('all database failed.')
    return data

# reference: https://pydata.github.io/pandas-datareader/stable/remote_data.html#google-finance