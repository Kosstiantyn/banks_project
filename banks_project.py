import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import unicodedata

# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'D:\\code\\python\\python_co\\banks\\Largest_banks_data.csv'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'

''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
def log_progress(message):
    time_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() 
    time_stamp = now.strftime(time_format) 
    with open('./code_log.txt', 'a') as file:
       file.write(time_stamp + ',' + message + '\n')

''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.'''
def extract(url, table_attribs):
    page = requests.get(url).text
    soup_data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = soup_data.find_all('tbody')
    table_rows = table[0].find_all('tr')
    for row in table_rows:
       if row.find('td') is not None: # check if the current row has child elements with the <td> tag. If so (meaning the row contains table cells), we proceed with the following actions.
          col = row.find_all('td') # find all elements with the <td> tag inside the current row and store them in the variable col
          bank_name = col[1].find_all('a')[1] # extract the bank name. We assume that the second element in col contains a link (<a>)
          market_cap = col[2].contents[0] # extracts the market capitalization. It assumes that the third element in col contains text (e.g., a monetary amount)
          data_dict = {'Name': bank_name,
                       'MC_USD_Billion': float(market_cap)} # create a dictionary called data_dict to store the bank name and market capitalization (converted to a float)
          df1 = pd.DataFrame(data_dict, index=[0]) 
          df = pd.concat([df, df1], ignore_index=True)
    return df

#df = extract(url, table_attribs)
#print(df)

'''This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
def transform(df):
    # read a CSV file containing exchange rates into a DataFrame named exchange_rate.
    exchange_rate = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv') 
    rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate'] # convert this DataFrame into a dictionary named rate_dict, where the key is the currency and the value is the exchange rate.
    '''Create a new column MC_GBP_Billion in the DataFrame df. 
    	 It converts the values in the MC_USD_Billion column to GBP using the exchange rate from the dictionary and rounds the result to 2 decimal places.'''
    df['MC_GBP_Billion'] = [np.round(x*rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*rate_dict['EUR'], 2) for x in df['MC_USD_Billion']] 
    df['MC_INR_Billion'] = [np.round(x*rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df

#euro_symbol = unicodedata.lookup("EURO SIGN")
#print(df, '\n')
#print(f'The market capitalization of the 5th largest bank in billion EUR: {euro_symbol}{df['MC_EUR_Billion'][4]}')

'''This function saves the final data frame as a CSV file in 
	the provided path. Function returns nothing.'''
def load_to_csv(df, output_path):
   df.to_csv(output_path)

'''This function saves the final data frame to a database
   table with the provided name. Function returns nothing.'''
def load_to_db(df, sql_connection, table_name):
   df.to_sql(df, sql_connection, table_name, if_exists ='replace', index = False)

'''This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.'''
def run_query(sql_connection): # Execute 3 function calls using the queries as mentioned below.
   query_statement = 'SELECT * FROM Largest_banks'
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)
   
   query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks' # Print the average market capitalization of all the banks in Billion USD.
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)
   
   query_statement = 'SELECT Name FROM Largest_banks LIMIT 5' # Print only the names of the top 5 banks
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)

'''Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress('Initiating ETL process')
df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')
run_query(sql_connection)

log_progress('Process Complete')
sql_connection.close()
log_progress('Server Connection closed')
