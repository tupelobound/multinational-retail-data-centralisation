import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def read_db_creds(self):
        '''Opens the YAML file containing the database credentials and returns the credentials as a dictionary'''
        # Use context manager to open file
        with open('db_creds.yaml', 'r') as file:
            # load contents into dictionary and return
            return yaml.safe_load(file)
    
