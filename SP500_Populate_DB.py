#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
# get_ipython().system('jupyter kernelspec list')
# get_ipython().system('which -a python')
import requests
import yfinance as yf
from pprint import pprint
from bs4 import BeautifulSoup
import sys
# !{sys.executable} -m pip install yfinance --upgrade "pymongo[srv]==3.12" certifi requests-cache requests_ratelimiter
# !{sys.executable} -m pip install --upgrade typing_extensions certifi

from selenium import webdriver
import time
from importlib.metadata import version
print(version("typing_extensions"))


# In[2]:


df= pd.read_csv('datas/constituents.csv')
df = df[['Symbol']]
df


# In[3]:


df['Symbol'] = df['Symbol'].str.replace('.', '-',regex=False)


# In[4]:


df.at[74, 'Symbol']


# In[5]:


df.at[61, 'Symbol']


# In[6]:


# from requests import Session
# from requests_cache import CacheMixin, SQLiteCache
# from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
# from pyrate_limiter import Duration, RequestRate, Limiter
# class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
#     pass

# session = CachedLimiterSession(
#     limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
#     bucket_class=MemoryQueueBucket,
#     backend=SQLiteCache("yfinance.cache"),
# )


# In[7]:


symbol = df['Symbol'][160]
print(symbol)
# ticker = yf.Ticker(symbol, session=session) #yfinance recently switched to using curl_cffi internally 
#                                             # (to bypass Yahoo Finance rate limits), and it no longer supports custom sessions (like the one built with requests_cache, Limiter, etc.).

ticker = yf.Ticker(symbol)
income_stmt = ticker.incomestmt
income_stmt


# In[8]:


def get_dictionary(df):
    df = df.transpose() # transpose the dataframe so that each row becomes a dictionary
    df.reset_index(inplace=True) # reset the index so that it becomes a column
    df.columns = ['date'] + df.columns[1:].tolist() # rename the columns
    # df['date'] = pd.to_datetime(df['date']) # convert the date column to datetime format
    # df.set_index('date', inplace=True)
    return df.to_dict(orient='records') # convert the dataframe to a list of dictionaries
    
income_stmt_dict = get_dictionary(income_stmt)
# income_stmt_dict


# In[9]:


quarterly_income_stmt = ticker.quarterly_incomestmt
# quarterly_income_stmt


# In[10]:


quarterly_income_stmt_dict = get_dictionary(quarterly_income_stmt)
# quarterly_income_stmt_dict


# In[11]:


balanceSheet = ticker.balance_sheet
# balanceSheet


# In[12]:


balanceSheet_dict = get_dictionary(balanceSheet)
balanceSheet_dict


# In[13]:


quarterly_balanceSheet= ticker.quarterly_balance_sheet
quarterly_balanceSheet


# In[14]:


def getBasicInfo(symbol):
    url = f"https://finance.yahoo.com/quote/{symbol}/key-statistics?p={symbol}"
    
    # adding headless options to Chrome so windows stay hidden
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    
    # Open the website using Selenium
    driver = webdriver.Chrome(options=options)
    driver.get(url)

    # Wait for the page to fully load
    time.sleep(5)

    # Parse the HTML code using BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Extract the data from the HTML code
    data = {'Symbol': symbol}
    for tr in soup.find_all('tr'):
        td_key = tr.find('td').find('span').text
        if td_key in ['Market Cap (intraday)', 'Trailing P/E', 'Forward P/E', 'Trailing Annual Dividend Yield', '5 Year Average Dividend Yield', 'Payout Ratio']:
            if td_key == 'Market Cap (intraday)': td_key = 'MarketCap_Billions'
            if td_key == 'Trailing P/E': td_key = 'TrailingPE'
            if td_key == 'Forward P/E': td_key = 'ForwardPE'
            if td_key == 'Trailing Annual Dividend Yield': td_key = 'TrailingAnnualDividendYield'
            if td_key == '5 Year Average Dividend Yield': td_key = 'FiveYearAvgDividendYield'
            if td_key == 'Payout Ratio': td_key = 'PayoutRatio'
            td_value = tr.find('td', {'class': 'Fw(500) Ta(end) Pstart(10px) Miw(60px)'}).text
            if td_value:
                try:
                    value = float(td_value.replace(',', ''))
                    data[td_key] = value
                except ValueError:
                    if td_value.endswith('%'):
                        value = float(td_value.replace('%', '').replace(',', ''))
                    elif td_value.endswith('B'):
                        value = float(td_value.replace('B', '').replace(',', ''))
                    elif td_value.endswith('T'):
                        value = float(td_value.replace('T', '').replace(',', ''))*1000
                    elif td_value.endswith('M'):
                        value = float(td_value.replace('M', '').replace(',', ''))/1000
                    data[td_key] = value

    # Close the web driver
    driver.quit()
    # return the dictionary
    return data


# In[15]:


# import pymongo, certifi, ssl
# print(pymongo.version)       # should be >= 4.0
# print(certifi.where())       # should point to a valid CA bundle
# print(ssl.OPENSSL_VERSION)   # should show OpenSSL 1.1.1 or higher
def get_basic_info_api(symbol):
    ticker = yf.Ticker(symbol)

    info = ticker.info  # This fetches key stats as JSON
    # print(info)
    data = {
        "Symbol": symbol,
        "MarketCap_Billions": info.get("marketCap", 0) / 1e9 if info.get("marketCap") else None,
        "TrailingPE": info.get("trailingPE"),
        "ForwardPE": info.get("forwardPE"),
        "TrailingAnnualDividendYield": info.get("dividendYield"),
        "FiveYearAvgDividendYield": info.get("fiveYearAvgDividendYield"),
        "PayoutRatio": info.get("payoutRatio"),
    }
    # print(data)
    return data

print(get_basic_info_api("TSLA"))


# In[16]:


def add_income_stmts_and_balance_sheets(basic_info, ticker):
    basic_info['AnnualIncomeStatements'] = get_dictionary(ticker.incomestmt)
    basic_info['AnnualBalanceSheets'] = get_dictionary(ticker.balance_sheet)
    basic_info['QuarterlyIncomeStatements'] = get_dictionary(ticker.quarterly_incomestmt)
    basic_info['QuarterlyBalanceSheets'] = get_dictionary(ticker.quarterly_balance_sheet)
    


# In[17]:


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Read the Mongo URI
uri = os.getenv("MONGO_URI")

client = MongoClient(uri, server_api=ServerApi('1'))

db = client["Stocks"]
collection = db["SP500"]
# collection = db["US_S&P500"]
# collection.rename("SP500")
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
        


# In[18]:


def add_to_DB(symbol):
    #getting existing document from Database
    existing_doc = collection.find_one({"Symbol": symbol})
    if existing_doc is None:
        # basic_info = getBasicInfo(symbol)
        basic_info = get_basic_info_api(symbol)
        basic_info['ROE'] = None
        basic_info['ROA'] = None
        basic_info['ROE_TTM'] = None
        basic_info['ROA_TTM'] = None
        print(symbol)
        ticker = yf.Ticker(symbol)
        add_income_stmts_and_balance_sheets(basic_info, ticker)
        #Adding dictionary to database
        collection.insert_one(basic_info)
    else:
        # basic_info = getBasicInfo(symbol)
        basic_info = get_basic_info_api(symbol)

        ticker = yf.Ticker(symbol)
        #Adding income_stmts_and_balance_sheets dictionaries to basic_info dictionary
        add_income_stmts_and_balance_sheets(basic_info, ticker)
        
        collection.update_one(
            {'_id': existing_doc['_id']},
            {'$set': {'MarketCap_Billions': basic_info['MarketCap_Billions'], 
                      'TrailingPE': basic_info['TrailingPE'],
                      'ForwardPE': basic_info['ForwardPE'], 
                      'TrailingAnnualDividendYield': basic_info['TrailingAnnualDividendYield'], 
                      'FiveYearAvgDividendYield': basic_info['FiveYearAvgDividendYield'], 
                      'PayoutRatio': basic_info['PayoutRatio'], 

                      'AnnualIncomeStatements': basic_info['AnnualIncomeStatements'],
                      'AnnualBalanceSheets': basic_info['AnnualBalanceSheets'],
                      'QuarterlyIncomeStatements': basic_info['QuarterlyIncomeStatements'],
                      'QuarterlyBalanceSheets': basic_info['QuarterlyBalanceSheets']
                     }
            }
        )
        print(symbol)
        


# In[19]:


df["basic_info"] = df["Symbol"].apply(add_to_DB)

# df
# add_to_DB("AMCR")


# In[20]:


from collections import deque
def calculate_net_income_ttm(quarterly_income_statements):
    """Calculates Net Income trailing twelve months."""
    
    # Create a deque to store the last four quarters' Net Income
    net_incomes = deque([], maxlen=4)

    # Populate the deque with available Net Income values
    for statement in quarterly_income_statements:
        net_incomes.append(statement.get("Net Income", 0))
        if len(net_incomes) == 4:
            break

    # If we don't have enough statements, fill up the rest with averages
    while len(net_incomes) < 4:
        avg_net_income = sum(net_incomes) / len(net_incomes)
        net_incomes.append(avg_net_income)

    return sum(net_incomes)


# In[21]:


def calculate_stockholder_equity_ttm(quarterly_balance_sheets):
    """Calculates Stockholders Equity trailing twelve months."""
    
    # Create a deque to store the last four quarters' Stockholders Equity
    equity_deque = deque([], maxlen=4)
    
    # Populate the deque with available Stockholders Equity values
    for statement in quarterly_balance_sheets:
        equity_deque.append(statement.get("Stockholders Equity", 0))
        if len(equity_deque) == 4:
            break
            
    # If we don't have enough statements, fill up the rest with averages
    while len(equity_deque) < 4:
        avg_equity = sum(equity_deque) / len(equity_deque)
        equity_deque.append(avg_equity)

    stockholders_equity_ttm = sum(equity_deque) / 4
    return stockholders_equity_ttm


# In[22]:


def calculate_total_assets_ttm(quarterly_balance_sheets):
    """Calculates Total Assets trailing twelve months."""
    
    # Create a deque to store the last four quarters' Total Assets
    assets_deque = deque([], maxlen=4)

    # Populate the deque with available Total Assets values
    for statement in quarterly_balance_sheets:
        assets_deque.append(statement.get("Total Assets", 0))
        if len(assets_deque) == 4:
            break
            
    # If we don't have enough statements, fill up the rest with averages
    while len(assets_deque) < 4:
        avg_assets = sum(assets_deque) / len(assets_deque)
        assets_deque.append(avg_assets)

    total_assets_ttm = sum(assets_deque) / 4
    return total_assets_ttm


# In[23]:


def add_ROE_TTM_and_ROA_TTM_to_DB():
    # Iterate through all documents in the collection
    for doc in collection.find():
        # Extract the required data from the 'Quarterly Income Statements' and 'Quarterly Balance Sheets' arrays
        quarterly_income_statements = doc.get('QuarterlyIncomeStatements', [])
        quarterly_balance_sheets = doc.get('QuarterlyBalanceSheets', [])

        if quarterly_income_statements and quarterly_balance_sheets:
            # calculating TTM Net Income, Stockholder Equity, Total Assets
            net_income_ttm = calculate_net_income_ttm(quarterly_income_statements)
            stockholder_equity_ttm = calculate_stockholder_equity_ttm(quarterly_balance_sheets)
            total_assets_ttm = calculate_total_assets_ttm(quarterly_balance_sheets)

            # Calculate ROE TTM and ROA TTM
            # Check if both net_income_ttm and stockholder_equity_ttm are negative
            if net_income_ttm <= 0 and stockholder_equity_ttm <= 0:
                roe_ttm = None
            else:
                roe_ttm = round((net_income_ttm / stockholder_equity_ttm)*100, 2) if stockholder_equity_ttm else None
                
            # Check if both net_income_ttm and total_assets_ttm are negative
            if net_income_ttm <= 0 and total_assets_ttm <= 0:
                roa_ttm = None
            else:
                roa_ttm = round((net_income_ttm / total_assets_ttm)*100, 2) if total_assets_ttm else None

            # Update the document with the calculated ROE and ROA values
            collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'ROE_TTM': roe_ttm, 'ROA_TTM': roa_ttm}}
            )
add_ROE_TTM_and_ROA_TTM_to_DB()


# In[24]:


def add_ROE_and_ROA_to_DB():
    # Iterate through all documents in the collection
    for doc in collection.find():
        # Extract the required data from the 'Annual Income Statements' and 'Annual Balance Sheets' arrays
        annual_income_statements = doc.get('AnnualIncomeStatements', [])
        annual_balance_sheets = doc.get('AnnualBalanceSheets', [])

        if annual_income_statements and annual_balance_sheets:
            # Use the most recent annual data for the calculations
            latest_income_statement = sorted(annual_income_statements, key=lambda x: x['date'], reverse=True)[0]
            latest_balance_sheet = sorted(annual_balance_sheets, key=lambda x: x['date'], reverse=True)[0]

            net_income = latest_income_statement.get('Net Income', 0)
            stockholders_equity = latest_balance_sheet.get('Stockholders Equity', 0)
            total_assets = latest_balance_sheet.get('Total Assets', 0)

            # Calculate ROE and ROA
            # Check if both net_income and stockholder_equity are negative
            if net_income <= 0 and stockholders_equity <= 0:
                roe = None
            else:
                roe = round((net_income / stockholders_equity)*100, 2) if stockholders_equity else None
                
            # Check if both net_income and total_assets are negative
            if net_income <= 0 and total_assets <= 0:
                roa = None
            else:    
                roa = round((net_income / total_assets)*100, 2) if total_assets else None

            # Update the document with the calculated ROE and ROA values
            collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'ROE': roe, 'ROA': roa}}
            )
add_ROE_and_ROA_to_DB()


# In[25]:


existing_doc = collection.find_one({"Symbol": "AON"})
existing_doc


# In[26]:


print('Database Updated...')


# In[ ]:




