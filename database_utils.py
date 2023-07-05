import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    def read_db_creds(self):
        '''Opens the YAML file containing the database credentials and returns the credentials as a dictionary.'''
        # Use context manager to open file
        with open('db_creds.yaml', 'r') as file:
            # load contents into dictionary and return
            return yaml.safe_load(file)
    

    def init_db_engine(self):
        '''Creates a connection string of database credentials and uses that string to create and return sqlalchemy database engine.'''
        # Call read_db_creds() method to get database credentials as dictionary
        db_credentials = self.read_db_creds()
        # Construct connection string using contents of dictionary
        connection_string = f"postgresql+psycopg2://{db_credentials['RDS_USER']}:{db_credentials['RDS_PASSWORD']}@" + \
                            f"{db_credentials['RDS_HOST']}:{db_credentials['RDS_PORT']}/{db_credentials['RDS_DATABASE']}"
        # Create new sqlalchemy database engine and return
        return create_engine(connection_string)
    

    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        return inspector.get_table_names()
        

test = DatabaseConnector()
print(test.list_db_tables())
