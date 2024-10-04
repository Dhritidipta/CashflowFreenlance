##importing libraries
import requests
import pandas as pd


base_url = 'http://localhost:9090/'

#API - GET - /companies get all companies
def get_all_companies_df(limit=-1):
  
    params = {}
    if limit != -1:
        params = {'limit': limit} 

    get_all_companies_url = f'{base_url}companies'  # Example API endpoint
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_companies_url,params)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response
        
        get_all_companies_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
        print("Data Fetched Successfully")

    else:

        return -1

    return get_all_companies_df

get_all_companies_df()

 #API - GET - /companies/{company-id} get a company
def get_company_df(id):
   
    # base_url = "http://localhost:8080/"

    company_id = 1
    get_company_url = f'{base_url}companies/{id}'  
    
    
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_company_url)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response
        
        get_company_df = pd.DataFrame(data.items())  # Convert JSON data to a DataFrame
        print("Data Fetched Successfully")
        

    else:

        return -1

    return get_company_df


#API - GET - /exchange-rates get all exchange rates
def get_exchange_rates_df():

   
    # base_url = "http://localhost:8080/"
    get_exchange_rates_url = f'{base_url}exchange-rates'  
    
    
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_exchange_rates_url)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_exchange_rates_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
        print("Data Fetched Successfully")


    else:

        return -1

    return get_exchange_rates_df

get_exchange_rates_df()

#API - GET - /transactions/sepa get all Sepa transactions, newest first
def get_all_sepa_transactions_df():
     
    # base_url = "http://localhost:8080/"
    get_all_sepa_transactions_url = f'{base_url}transactions/sepa'  # Example API endpoint
    
    params = {'after-uuid':'00000000-0000-0000-0000-000000000000',
            }
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_sepa_transactions_url,params)

    data = response.json()  # Get JSON data from the response

    get_all_sepa_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
    
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_all_sepa_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
        print("Data Fetched Successfully")
    

    else:

        return -1

    return get_all_sepa_transactions_df

get_all_sepa_transactions_df()

#API - GET - /transactions/swift get all Swift transactions, newest first 
def get_all_swift_transactions_df():
     # base_url = "http://localhost:8080/"
    get_all_swift_transactions_url = f'{base_url}transactions/swift'  # Example API endpoint
    
    params = {'after-uuid':'00000000-0000-0000-0000-000000000000',
            }
    # Step 2: Make a GET request to fetch data

    response = requests.get(get_all_swift_transactions_url,params)
    
    # Check if the request was successful

    if response.status_code == 200:

        # Step 3: Convert the JSON response to a DataFrame

        data = response.json()  # Get JSON data from the response

        get_all_swift_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
        print("Data Fetched Successfully")
        

    else:

        return "-1"
    
    return get_all_swift_transactions_df

get_all_swift_transactions_df()

    # data = response.json()  # Get JSON data from the response

    # get_all_swift_transactions_df = pd.DataFrame(data)  # Convert JSON data to a DataFrame
    # print(get_all_swift_transactions_df)

