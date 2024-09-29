
import requests

import pandas as pd


def get_all_companies_df():

    #API - GET - /companies get all companies
   
    base_url = 'http://localhost:8080/'
    get_all_companies_url = f'{base_url}companies'  # Example API endpoint
    
    params = {'limit':10, 
             'after-id':1}
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_companies_url,params)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response
        
        get_all_companies_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame

    else:

        return -1

    return get_all_companies_df

def get_company_df():
     #API - GET - /companies/{company-id} get a company
   
    base_url = "http://localhost:8080/"

    company_id = 1
    get_company_url = f'{base_url}companies/{company_id}'  
    
    
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_company_url)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response
        
        get_company_df = pd.DataFrame(data.items())  # Convert JSON data to a DataFrame
        

    else:

        return -1

    return get_company_df

def get_exchange_rates_df():
    #API - GET - /exchange-rates get all exchange rates
   
    base_url = "http://localhost:8080/"
    get_exchange_rates_url = f'{base_url}exchange-rates'  
    
    
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_exchange_rates_url)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_exchange_rates_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame


    else:

        return -1

    return get_exchange_rates_df


def get_all_sepa_transactions_df():
     #API - GET - /transactions/sepa get all Sepa transactions, newest first
   
    base_url = "http://localhost:8080/"
    get_all_sepa_transactions_url = f'{base_url}transactions/sepa'  # Example API endpoint
    
    params = {'limit':5000, 
             'after-timestamp':'2020-09-26T00:00:00Z',
             'after-uuid':'00000000-0000-0000-0000-000000000000',
             'payer': 'DE77193919112673928344',
             'sender': 'NL28SZUK8962850954'}
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_sepa_transactions_url,params)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_all_sepa_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
    

    else:

        return -1

    return get_all_sepa_transactions_df

def get_all_swift_transactions_df():
    #API - GET - /transactions/swift get all Swift transactions, newest first
   
    base_url = "http://localhost:8080/"
    get_all_swift_transactions_url = f'{base_url}transactions/swift'  # Example API endpoint
    
    params = {'limit':5000, 
             'after-timestamp':'2024-09-28T00:00:00Z',
             'after-uuid':00000000-0000-0000-0000-000000000000,
             'sender': 'GB51SJEV83747591864014',
             'beneficiary': 'FR5875918640140818391314958'}
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_swift_transactions_url,params)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_all_swift_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
    
        

    else:

        return -1

    return get_all_swift_transactions_df







#########################################################################################################


# import requests

# # Define the API endpoint
# user_id = 123  # replace with the actual user ID
# base_url = f"http://localhost:8080/"
# #http://localhost:8080/companies?limit=10&after-id=1
# find_all_companies_url = f"{base_url}companies?limit=10&after-id=1"

# try:
#     # Send a GET request to fetch user details
#     response = requests.get(find_all_companies_url)
    
#     # Check if the request was successful
#     if response.status_code == 200:
#         # Parse the JSON response
#         user_data = response.json()
#         print("User Details:", user_data)
#     else:
#         print(f"Failed to fetch user details. Status code: {response.status_code}")

# except requests.exceptions.RequestException as e:
#     # Handle any exceptions (e.g., network issues)
#     print(f"An error occurred: {e}")

#########################################################################################################


# import requests
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("API Data Fetch") \
#     .getOrCreate()
 
# # API endpoint and initial parameters
# url = 'http://localhost:8080/companies'
# params = {'limit': 10,
#           'after-id':1}
 
# all_data = []
 
# # Fetch paginated data from the API
# while True:
#     response = requests.get(url, params=params)
#     if response.status_code == 200:
#         page_data = response.json()
#         if not page_data:  # Break if no more data
#             break
#         all_data.extend(page_data)
#         params['page'] += 1  # Move to next page
#     else:
#         raise Exception(f"Failed to fetch data: {response.status_code}")
 
# # Define schema
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("ibans", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("address", StringType(), True)
# ])
 
# # Create a DataFrame
# df = spark.createDataFrame(all_data, schema)
 
# # Show the DataFrame
# df.show()