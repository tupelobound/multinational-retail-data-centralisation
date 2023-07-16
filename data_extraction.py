import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self):
        self.number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        self.get_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        self.api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def read_rds_table(self, connector, table):
        engine = connector.init_db_engine()
        return pd.read_sql_table(table, engine)
    
    def retrieve_pdf_data(self, link):
        return pd.concat(tabula.read_pdf(link, pages='all'))
    
    def list_number_of_stores(self):
        response = requests.get(self.number_of_stores_endpoint, headers=self.api_header)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self):
        stores = pd.DataFrame()
        for store_number in range(0, self.list_number_of_stores()):
            response = requests.get(self.get_store_endpoint + str(store_number), headers=self.api_header)
            new_dataframe = pd.DataFrame(response.json(), index=[0])
            stores = pd.concat([stores, new_dataframe])
        return stores
    
    def extract_from_s3(self, endpoint):
        if endpoint[-3:] == 'csv':
            # need to install s3fs library
            return pd.read_csv(endpoint)
        elif endpoint[-4:] == 'json':
            return pd.read_json(endpoint)
        