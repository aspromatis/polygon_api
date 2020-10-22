#%%
# I'm using my base conda environment for this given the simple requirements
import requests
import pandas as pd
import matplotlib
import os
import math
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


#%%
# Set some constant variables, I could put all of this in a seperate config file
ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY')
START_DATE = '2005-01-03'
END_DATE = '2020-10-21'
# URL for all the tickers on Polygon
POLYGON_TICKERS_URL = 'https://api.polygon.io/v2/reference/tickers?page={}&apiKey={}'
# URL FOR PRICING DATA - Note, getting pricing that is UNADJUSTED for splits, I will try and adjust those manually
POLYGON_AGGS_URL = 'https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/{}/{}?unadjusted=true&apiKey={}'
# URL FOR DIVIDEND DATA
POLYGON_DIV_URL = 'https://api.polygon.io/v2/reference/dividends/{}?apiKey={}'
# URL FOR STOCK SPLITS
POLYGON_SPLIT_URL = 'https://api.polygon.io/v2/reference/splits/{}?apiKey={}'
#URL FOR TICKER TYPES
POLYGON_TYPES_URL = 'https://api.polygon.io/v2/reference/types?apiKey={}'

#%% 
# Get the list of all supported tickers from Polygon.io
def get_tickers(url = POLYGON_TICKERS_URL):
    page = 1

    session = requests.Session()
    # Initial request to get the ticker count
    r = session.get(POLYGON_TICKERS_URL.format(page, ALPACA_API_KEY))
    data = r.json()

    # This is to figure out how many pages to run pagination 
    count = data['count']
    pages = math.ceil(count / data['perPage'])

    # Pull in all the pages of tickers
    # for pages in range (2, pages+1):  # For production
    for pages in range (2, 10):  # For testing
        r = session.get(POLYGON_TICKERS_URL.format(page, ALPACA_API_KEY))
        data = r.json()
        df = pd.DataFrame(data['tickers'])
        df.to_csv('data/tickers/{}.csv'.format(page), index=False)
        print('Page {} processed'.format(page))
        page += 1
        
    return('Processes {} pages of tickers'.format(page-1))


# Stich all of these csv files into one dataframe for analysis
def combine_tickers(directory):

    df = pd.DataFrame()

    for f in os.listdir(directory):
        df2 = pd.read_csv('{}/{}'.format(directory, f))
        df = df.append(df2)
    
    # Read out a copy of the file to a csv for later analysis
    df.set_index('ticker', inplace=True)
    df.drop_duplicates()  # Just in case any tickers get pulled twice
    df.to_csv('polygon_tickers.csv')
        
    return df


def filter_us_exch(ticker_df):
    
    # Keep only U.S. Dollar denominated securities
    df = ticker_df[(ticker_df.currency == 'USD') & (ticker_df.locale == 'US')]
    # Keep only the primary U.S. exchanges
    exch = ['AMX','ARCA','BATS','NASDAQ','NSC','NYE']
    df = df[df['primaryExch'].isin(exch)]
    # Filter out preferred stock, american depositry receipts, closed end funds, reit
    stockTypes = ['PFD','ADR','CEF','MLP','REIT','RIGHT','UNIT','WRT']
    df = df[df['type'].isin(stockTypes) == False]
    
    df.to_csv('polygon_tickers_us.csv')

    # Create a list of symbols to loop thru
    symbols = df.index.tolist()

    return symbols


# Get the aggregated bars for the symbols I need
def get_bars(symbolslist, outdir, start, end):

    session = requests.Session()
    # In case I run into issues, retry my connection
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])

    session.mount('http://', HTTPAdapter(max_retries=retries))
    count = 0
    
    barlog = open("barlog.txt", "w")
    
    for symbol in symbolslist: # ['AAPL']:
    # for symbol in ['AAPL']:
        try:
            r = session.get(POLYGON_AGGS_URL.format(symbol, start, end, ALPACA_API_KEY))
            if r:
                data = r.json()
            
                # create a pandas dataframe from the information
                if data['queryCount'] > 1:
                    df = pd.DataFrame(data['results'])
                    df['date'] = pd.to_datetime(df['t'], unit='ms')
                    df['date'] =  df['date'].dt.date.astype(str)
                    df.set_index('date', inplace=True)
                    df['symbol'] = symbol

                    df.drop(columns=['vw', 't', 'n'], inplace=True)
                    df.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low'}, inplace=True)

                    df.to_csv('{}/{}.csv'.format(outdir, symbol), index=True)
                    count += 1

                    # Logging, I could write a short method for this to reuse
                    msg = (symbol + ' file created with record count ' + str(data['queryCount']))
                    print(msg)
                    barlog.write(msg)
                    barlog.write("\n")

                else:
                    msg = ('No data for symbol ' + str(symbol))
                    print(msg)
                    barlog.write(msg)
                    barlog.write("\n")
            else:
                msg = ('No response for symbol ' + str(symbol))
                print(msg)
                barlog.write(msg)
                barlog.write("\n")
        # Raise exception but continue           
        except:
            msg = ('****** exception raised for symbol ' + str(symbol))
            print(msg)
            barlog.write(msg)
            barlog.write("\n")
    
    barlog.close()
    return ('{} file were exported'.format(count))


# Define a function to pull in the splits data
def get_splits(symbolslist, outdir):

    session = requests.Session()
    # In case I run into issues, retry my connection
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])

    session.mount('http://', HTTPAdapter(max_retries=retries))
    count = 0
    
    # Get the split data
    # for symbol in symbolslist: # ['AAPL']:
    for symbol in symbolslist: # ['AAPL']:
        try:
            r = session.get(POLYGON_SPLIT_URL.format(symbol, ALPACA_API_KEY))
            if r:
                data = r.json()
                if data['count'] > 0:
                    df = pd.DataFrame(data['results'])
                    df.rename(columns={'exDate': 'date', 'declaredDate': 'splitDeclaredDate'}, inplace=True)
                    df.drop(columns=['paymentDate'], inplace=True)
                    df.set_index('date', inplace=True)
                    df.to_csv('{}/{}.csv'.format(outdir, symbol), index=True)
                    
                    print('split file for ' + symbol + ' ' + str(data['count']))
                    count += 1
                else:
                    print('No data for symbol ' + str(symbol))
            else:
                print('No response for symbol ' + str(symbol))
        # Raise exception but continue           
        except:
            print('****** exception raised for symbol ' + str(symbol))
            
    return ('{} file were exported'.format(count))


# Fix erroneous splits from a correction file manually created
def fix_splits(splitpath):
    # Get the split corrections to overwrite
    correct_df = pd.read_csv('split_corrections.csv')
    # create a list of symbols to fix
    symbols = correct_df['ticker'].tolist()
    # remove duplicates
    symbols = list(dict.fromkeys(symbols))

    # for symbol in symbols:
    for symbol in symbols:
        print(symbol)

    # get any splits
        if os.path.isfile('{}/{}.csv'.format(splitpath, symbol)):
            df = pd.read_csv('{}/{}.csv'.format(splitpath, symbol))
            print(df)
            df = pd.merge(df, correct_df, how='left', left_on=['date', 'ticker'], right_on=['date', 'ticker'])
            
            for index, row in df.iterrows():
                # Adjust bad dates
                if not pd.isnull(row.date_adj):
                    df.loc[index, 'date'] = row.date_adj
                # Adjust bad ratios
                if not pd.isnull(row.ratio_adj):
                    df.loc[index, 'ratio'] = row.ratio_adj
                else:
                    df.loc[index, 'ratio'] = row.ratio_x
            
            # Format the dataframe for export
            df = df[['date', 'ticker', 'ratio']]
            df.set_index('date', inplace=True)
            print(df)

            # Overwrite the file with this new file
            df.to_csv('{}/{}.csv'.format(splitpath, symbol))
            print('Split file for {} corrected'.format(symbol))
            
        else:
            print('no file found')
                
    return ('Split file corrections complete')


# Define a function to pull in the splits data
def get_divs(symbolslist, outdir):

    session = requests.Session()
    count = 0
    
    # Get the split data
    for symbol in symbolslist: # ['AAPL']:
        r = session.get(POLYGON_DIV_URL.format(symbol, ALPACA_API_KEY))
        data = r.json()
        if data['count'] > 0:
            df = pd.DataFrame(data['results'])
            # df.rename(columns={'paymentDate': 'date'}, inplace=True)
            df.rename(columns={'exDate': 'date', 'amount': 'dividend',
                               'paymentDate': 'divPaymentDate',
                               'recordDate': 'divRecordDate',
                               'declaredDate': 'divDeclaredDate'}, inplace=True)
            df.set_index('date', inplace=True)
            df = df.groupby(df.index).first()
            df.to_csv('{}/{}.csv'.format(outdir, symbol), index=True)
            
            print('div file for ' + symbol + ' ' + str(data['count']))
            count += 1
            
    return ('{} file were exported'.format(count))


# Combine bars, splits and dividend
def combine_bars(barpath, splitpath, divpath):

    count = 0
    for f in os.listdir(barpath):
        
        symbol = f[:-4]
        print(symbol)
        
        # Get the bar data
        if os.path.isfile('{}/{}.csv'.format(barpath, symbol)):
            bars = pd.read_csv('{}/{}.csv'.format(barpath, symbol), index_col='date')
            
            # get any splits
            if os.path.isfile('{}/{}.csv'.format(splitpath, symbol)):
                splits = pd.read_csv('{}/{}.csv'.format(splitpath, symbol), index_col='date')
                splits.drop(columns=['ticker'], inplace=True)
                
                bars = bars.merge(splits, left_index=True, right_index=True, how='left')

            else:
                
                bars = bars
            
            # get any dividend payments
            if os.path.isfile('{}/{}.csv'.format(divpath, symbol)):
                divs = pd.read_csv('{}/{}.csv'.format(divpath, symbol), index_col='date')
                divs.drop(columns=['ticker'], inplace=True)
            
                bars = bars.merge(divs, left_index=True, right_index=True, how='left')
            
            else:
                
                bars = bars
                
            # Export bars 
            bars.to_csv('data/bars_adj/{}.csv'.format(symbol))
            count += 1
        
    return ('{} adjusted bar file were exported'.format(count))


# Adjust the OHLCV data for stock splits
def adj_bars(directory):

    count = 0
    for f in os.listdir(directory):

        df = pd.read_csv('{}/{}'.format(directory, f), index_col='date')
        
        if 'ratio' in df.columns:
            df['ratio_adj'] = df['ratio']
        else:
             df['ratio_adj'] = 1

        # Create a split factor, shifted to the day earlier.  Also, fill in any missing factors with 1
        df['split_factor'] = (1 / df['ratio_adj'].shift(-1)).fillna(1)
        #  Create a cumulative product of the splits, in reverse order using the []::-1]
        df['split_factor'] = df['split_factor'][::-1].cumprod()

        # Adjust the various OHLCV metrics
        df['volume_adj'] = df['volume'] * df['split_factor']
        df['open_adj'] = df['open'] / df['split_factor']
        df['close_adj'] = df['close'] / df['split_factor']
        df['high_adj'] = df['high'] / df['split_factor']
        df['low_adj'] = df['low'] / df['split_factor']
        df['dollar_volume'] = df['volume'] * df['close']

        df.to_csv('{}/{}'.format(directory, f))
        count += 1
        
    return ('{} files was adjusted'.format(count))



#%%  Get all the tickers on Polygon.io and save them to a data directory
get_tickers()

#%% Combine all the paginated ticker files together into one dataframe
symbols = combine_tickers('data/tickers')

#%%  Filter down to the tickers I'm interestead in (this could also be done by modifying get_tickers)
symbols = filter_us_exch(symbols)

#%% Get all the aggregated bar/pricing data for each symbol in the filtered list
get_bars(symbols, 'data/bars', START_DATE, END_DATE)

#%%  Pull in all the stock splits
get_splits(symbols, 'data/splits')
# Fix data for about 50 splits from a correction file created manually
fix_splits('data/splits')

#%%  Pull in all the dividend data
get_divs(symbols, 'data/divs')

#%%  Combine the bars (pricing data) with any splits and dividend payments
combine_bars('data/bars', 'data/splits', 'data/divs')

#%%  Create new and stock split adjusted OHLCV fields
adj_bars('data/bars_adj')



#%%
bars = pd.read_csv('data/bars_adj/AAPL.csv')
bars['close_adj'].plot()

