import pandas as pd
import tabula
import requests

class DataExtractor:
    def read_rds_table(self, connector, table):
        engine = connector.init_db_engine()
        return pd.read_sql_table(table, engine)
    
    def retrieve_pdf_data(self, link):
        return pd.concat(tabula.read_pdf(link, pages='all'))
    
    def list_number_of_stores(self, endpoint, header_dictionary):
        response = requests.get(endpoint, headers=header_dictionary)
        return response.json()['number_stores']