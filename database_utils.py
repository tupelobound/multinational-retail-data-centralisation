import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    '''This class contains methods for connecting to databases.
    
    Attributes
    ----------

    Methods
    -------
    __init__(self, filename):
        Initialises an instance of the DatabaseConnector class.
    read_db_creds(self):
        Retrieves database credentials from the YAML filename passed in upon class instantiation.
    init_db_engine(self):
        Initialises Postgresql database connection.
    list_db_tables(self):
        Gets the table names of a given database.
    upload_to_db(self, dataframe, table):
        Uploads pandas DataFrame to SQL database.
    '''
    def __init__(self, filename):
        '''Initialises an instance of the DatabaseConnector class.
        
        Parameters
        ----------
        filename: str
            Name of YAML file containing database credentials.
        
        Returns
        -------
        None
        '''
        self.filename = filename

    def read_db_creds(self):
        '''Retrieves database credentials from the YAML filename passed in upon class instantiation.
        
        Opens the YAML file containing the database credentials and returns the credentials as a dictionary.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        dict
            Contents of YAML file as dictionary.
        '''
        # Use context manager to open file
        with open(self.filename, 'r') as file:
            # load contents into dictionary and return
            return yaml.safe_load(file)

    def init_db_engine(self):
        '''Initialises Postgresql database connection.
        
        Creates a connection string of database credentials and uses that string to create and return sqlalchemy database engine.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        sqlalchemy.engine.base.Engine
            sqlalchemy engine for database connection.
        '''
        # Call read_db_creds() method to get database credentials as dictionary
        db_credentials = self.read_db_creds()
        # Construct connection string using contents of dictionary
        connection_string = f"postgresql+psycopg2://{db_credentials['RDS_USER']}:{db_credentials['RDS_PASSWORD']}@" + \
                            f"{db_credentials['RDS_HOST']}:{db_credentials['RDS_PORT']}/{db_credentials['RDS_DATABASE']}"
        # Create new sqlalchemy database engine and return
        return create_engine(connection_string)
        
    def list_db_tables(self):
        '''Gets the table names of a given database.
        
        Calls inspect() method on init_db_engine() class method to create inspector object, then calls get_table_names()
        method on inspector and returns table names as list.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        list
            list of table names from Postgresql database connection.
        '''
        inspector = inspect(self.init_db_engine())
        return inspector.get_table_names()

    def upload_to_db(self, dataframe, table):
        '''Uploads pandas DataFrame to SQL database.
        
        Utilises init_db_engine() method to connect to Postgresql database, then pandas to_sql() method to upload DataFrame to
        given database table.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            DataFrame to be uploaded to SQL database.
        table: str
            Name of table in SQL database to upload to.
        
        Returns
        -------
        None
        '''
        engine = self.init_db_engine()
        dataframe.to_sql(table, engine, index=False, if_exists='replace')