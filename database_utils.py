import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def read_db_creds(self):
        '''Opens the YAML file containing the database credentials and returns the credentials as a dictionary'''
        # Use context manager to open file
        with open('db_creds.yaml', 'r') as file:
            # load contents into dictionary and return
            return yaml.safe_load(file)
    

    def init_db_engine(self):
        db_credentials = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_credentials['RDS_USER']}:{db_credentials['RDS_PASSWORD']} \
                               @{db_credentials['RDS_HOST']}:{db_credentials['RDS_PORT']}/{db_credentials['RDS_DATABASE']}")
        
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()


test = DatabaseConnector()
test.init_db_engine()
