import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, filename):
        self.filename = filename

    def read_db_creds(self):
        '''Opens the YAML file containing the database credentials and returns the credentials as a dictionary.'''
        # Use context manager to open file
        with open(self.filename, 'r') as file:
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
        '''Gets the table names of a database'''
        # Call inspect() on init_db_engine() class method to create inspector object
        inspector = inspect(self.init_db_engine())
        # Call get_table_names() method on inspector and return
        return inspector.get_table_names()

    def upload_to_db(self, dataframe, table):
        engine = self.init_db_engine()
        dataframe.to_sql(table, engine, index=False, if_exists='replace')