import os
import pandas as pd
import requests # for making GET requests to api
import tabula # for reading tabular data from .pdf
from dotenv import load_dotenv # for storing api key in .env file

load_dotenv()  # take environment variables from .env.

class DataExtractor:
    ''' This class contains methods for extracting data from various sources.

    Attributes
    ----------
    number_of_stores_endpoint:
        This is the api endpoint that returns the number of stores contained in the stores data.
    get_store_endpoint:
        This is the api endpoint for retrieving any given store, by appending the store number to the endpoint path.
    api_header:
        This is the dictionary that contains the api key for accessing the store api endpoints
    
    Methods
    -------
    __init__(self):
        Initialises an instance of the DataExtractor class with the attributes listed.
    read_rds_table(self, connector, table):
        Reads SQL table from RDS database and returns table as pandas DataFrame.
    retrieve_pdf_data(self, connector, table):
        Retrieves tabular data from cloud-based .pdf file and returns data as pandas DataFrame.
    list_number_of_stores(self):
        Retrieves number of stores from api endpoint.
    retrieve_stores_data(self):
        Iteratively retrieves individual store records and adds them to pandas DataFrame.
    extract_from_S3(self, endpoint):
        Retrieves data from .json or .csv file stored in AWS S3.
    '''
    def __init__(self):
        '''Initialises an instance of the DataExtractor class.'''
        self.number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        self.get_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        self.api_header = {'x-api-key': os.getenv("API_HEADER")}

    def read_rds_table(self, connector, table):
        '''Reads SQL table from RDS database and returns table as pandas DataFrame.
        
        Takes an instance of the DatabaseConnector class and a table name, initialises a connection to a SQL database
        and returns a pandas DataFrame containing the data from the table name passed in as an argument.
        
        Parameters
        ----------
        connector: DatabaseConnector
            Instance of DatabaseConnector class
        table: str
            Name of table to be retreived from RDS database
        
        Returns
        -------
        pandas.core.frame.DataFrame
            DataFrame containing table data from RDS
        '''
        # call init_db_engine() method of DatabaseConnector class
        engine = connector.init_db_engine()
        # read SQL table specified as argument into pandas DataFrame
        return pd.read_sql_table(table, engine)
    
    def retrieve_pdf_data(self, link):
        '''Retrieves tabular data from cloud-based .pdf file and returns data as pandas DataFrame.
        
        Takes a link to a pdf file stored in the cloud and uses read_pdf() method of the tabula module to read the data into
        a pandas DataFrame, which is then returned.
        
        Parameters
        ----------
        link: str
            URL to cloud-based .pdf file.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            DataFrame containing table data from .pdf file
        '''
        return pd.concat(tabula.read_pdf(link, pages='all'))
    
    def list_number_of_stores(self):
        '''Retrieves number of stores from api endpoint.
        
        Utilises requests.get() method to retrieve json data regarding the number of stores from an api endpoint.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        Integer representing the number of store records available on api endpoint
        '''
        response = requests.get(self.number_of_stores_endpoint, headers=self.api_header)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self):
        '''Iteratively retrieves individual store records and adds them to pandas DataFrame.
        
        Initialises an empty pandas dataframe and then iteratively makes GET requests on the retrieve stores api endpoint, up
        to the number of stores returned from calling the list_number_of_stores() method.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        stores: pandas.core.frame.DataFrame
            DataFrame containing all the store data from api endpoint.
        '''
        stores = pd.DataFrame()
        for store_number in range(0, self.list_number_of_stores()):
            response = requests.get(self.get_store_endpoint + str(store_number), headers=self.api_header)
            new_dataframe = pd.DataFrame(response.json(), index=[0])
            stores = pd.concat([stores, new_dataframe], ignore_index=True)
        return stores
    
    def extract_from_s3(self, endpoint):
        '''Retrieves data from .json or .csv file stored in AWS S3.
        
        Retrieves either .csv or .json publicly-available files from Amazon S3 storage and returns a pandas dataframe containing
        the data from endpoint file.
        
        Parameters
        ----------
        endpoint: str
            URL to S3-based file.
        
        Returns
        -------
        pandas.core.frame.DataFrame
            DataFrame containing table data from S3 file'''
        if endpoint[-3:] == 'csv':
            # need to install s3fs library
            return pd.read_csv(endpoint)
        elif endpoint[-4:] == 'json':
            return pd.read_json(endpoint)
        