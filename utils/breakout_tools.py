import pandas as pd
import numpy as np
import helper
import project_helper
import project_tests

# Compute the Highs and Lows in a Window
def get_high_lows_lookback(high, low, lookback_days):
    """
    Get the highs and lows in a lookback window.
    
    Parameters
    ----------
    high : DataFrame
        High price for each ticker and date
    low : DataFrame
        Low price for each ticker and date
    lookback_days : int
        The number of days to look back
    
    Returns
    -------
    lookback_high : DataFrame
        Lookback high price for each ticker and date
    lookback_low : DataFrame
        Lookback low price for each ticker and date
    """

    # getting max price for high prices excluding present day
    lookback_high = high.rolling(window=lookback_days).max().shift()
    
    # getting min price for low prices excluding present day
    lookback_low = low.rolling(window=lookback_days).min().shift()

    return lookback_high, lookback_low

# Compute Long and Short Signals
def get_long_short(close, lookback_high, lookback_low):
    """
    Generate the signals long, short, and do nothing.
    
    Parameters
    ----------
    close : DataFrame
        Close price for each ticker and date
    lookback_high : DataFrame
        Lookback high price for each ticker and date
    lookback_low : DataFrame
        Lookback low price for each ticker and date
    
    Returns
    -------
    long_short : DataFrame
        The long, short, and do nothing signals for each ticker and date
    """

    # creating signal dataframe with similar datetime as index
    signal_df = pd.DataFrame(columns=close.columns, index=close.index) 
    
    # getting the row and column length of the df
    row_len = close.shape[0]
    col_len = close.shape[1]
    
    # looping through matching datasets for signaling
    for row in range(0, row_len):
        for col in range(0, col_len):
            if lookback_low.iloc[row][col] > close.iloc[row][col]:
                signal_df.iloc[row][col] = -1
            elif lookback_high.iloc[row][col] < close.iloc[row][col]:
                signal_df.iloc[row][col] = 1
            else:
                signal_df.iloc[row][col] = 0
    
    # Converting to int64 datatype
    for index, row in signal_df.iterrows(): 
        signal_df.loc[index] = signal_df.loc[index].astype('int64')
        
    return signal_df

# Remove unnecessary signals
def clear_signals(signals, window_size):
    """
    Clear out signals in a Series of just long or short signals.
    
    Remove the number of signals down to 1 within the window size time period.
    
    Parameters
    ----------
    signals : Pandas Series
        The long, short, or do nothing signals
    window_size : int
        The number of days to have a single signal       
    
    Returns
    -------
    signals : Pandas Series
        Signals with the signals removed from the window size
    """
    # Start with buffer of window size
    # This handles the edge case of calculating past_signal in the beginning
    clean_signals = [0]*window_size
    
    for signal_i, current_signal in enumerate(signals):
        # Check if there was a signal in the past window_size of days
        has_past_signal = bool(sum(clean_signals[signal_i:signal_i+window_size]))
        # Use the current signal if there's no past signal, else 0/False
        clean_signals.append(not has_past_signal and current_signal)
        
    # Remove buffer
    clean_signals = clean_signals[window_size:]

    # Return the signals as a Series of Ints
    return pd.Series(np.array(clean_signals).astype(np.int), signals.index)

# Filter required signals
def filter_signals(signal, lookahead_days):
    """
    Filter out signals in a DataFrame.
    
    Parameters
    ----------
    signal : DataFrame
        The long, short, and do nothing signals for each ticker and date
    lookahead_days : int
        The number of days to look ahead
    
    Returns
    -------
    filtered_signal : DataFrame
        The filtered long, short, and do nothing signals for each ticker and date
    """
    
    # getting signal values for columns and rows
    col_values = signal.columns.values
    index_values = signal.index.values
    
    # getting the short and long dfs
    short_df = pd.DataFrame([], columns=col_values , index=index_values)
    long_df = pd.DataFrame([], columns=col_values , index=index_values)
    
    # iterating through the df, comparing the values of each signal checking and indicating if there is signal(1, -1) or not(0)
    for (idx_l,col_l),(idx_s, col_s),(idx_sig, col_sig) in zip(long_df.iterrows(), short_df.iterrows(), signal.iterrows()):
        for value in col_values:
            if col_sig[value] == -1:
                col_s[value] =-1
            else :
                col_s[value] = 0
            
            if col_sig[value] == 1:
                col_l[value] =1
            else :
                col_l[value] = 0
                    
    # filtering the df for the number of lookahead days and apply the clear_signals function to each column
    # returning a function from another function with lambda (functional programming)
    filtered_long = long_df.apply(lambda x: clear_signals(x,lookahead_days), axis=0)
    filtered_short = short_df.apply(lambda x: clear_signals(x,lookahead_days), axis=0)

    # adding the 2 dfs to obtain 1 df to be returned
    return filtered_long.add(filtered_short)


# Get Lookahead Close Prices
def get_lookahead_prices(close, lookahead_days):
    """
    Get the lookahead prices for `lookahead_days` number of days.
    
    Parameters
    ----------
    close : DataFrame
        Close price for each ticker and date
    lookahead_days : int
        The number of days to look ahead
    
    Returns
    -------
    lookahead_prices : DataFrame
        The lookahead prices for each ticker and date
    """
    
    return close.shift(-lookahead_days)



