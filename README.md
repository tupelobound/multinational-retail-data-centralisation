# multinational-retail-data-centralisation

## Project brief
You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across 
many different data sources making it not easily accessible or analysable by current members of the team. In an effort to 
become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your 
first goal will be to produce a system that stores the current company data in a database so that it's accessed from one 
centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date 
metrics for the business.

The different data sources that need to be extracted from and collected together are:
- two tables of an SQL database hosted on AWS RDS
- one table stored as a .pdf file hosted on AWS S3
- one table stored as a .csv file hosted on AWS S3
- one table stored as a .json file hosted on AWD S3
- a series of JSON objects available via an API

By completing this project, I have built a pipeline for extracting the data from the various sources, transforming (cleaning) 
the data, and loading the data into a new Postgresql database hosted locally. Once extracted and loaded, further transformation 
of the data and database was performed to complete the database schema. Finally, several SQL queries were written to enable 
users of the database to query the data and extract meaningful insights from it.

## Tools used

### SQLAlchemy
[SQLAlchemy](https://www.sqlalchemy.org/) was used to connect to both the AWS and local SQL databases. In `database_utils.py`:

```python
from sqlalchemy import create_engine, inspect
```

From the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/tutorial/engine.html):

> The start of any SQLAlchemy application is an object called the Engine. This object acts as a central source of connections 
> to a particular database, providing both a factory as well as a holding space called a connection pool for these database 
> connections. The engine is typically a global object created just once for a particular database server, and is configured 
> using a URL string which will describe how it should connect to the database host or backend.

For example:

```python
# Construct connection string
connection_string = f"postgresql+psycopg2://{db_username]}:{db_password}@" + f"{db_host]}:{db_port}/{db_database}"
# Create new sqlalchemy database engine
engine = create_engine(connection_string)
```

The `inspect()` method is used to get information about a connected database:

```python
inspector = inspect(engine)
table_name_list = inspector.get_table_names()
```

### PyYAML

The credentials for the databases are stored locally in YAML files. In order to access the credentials to pass into the 
create_engine() method above, [PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation) was used to read the YAML files and load 
the contents into a dictionary:

```python
import yaml
# Use context manager to open file
with open(self.filename, 'r') as file:
    # load contents into dictionary
    contents_dictionary = yaml.safe_load(file)
```

### Tabula

[Tabula](https://tabula-py.readthedocs.io/en/latest/#) is a simple tool for reading tables from pdf files and converting them
to a pandas DataFrame or CSV/TSV/JSON file.

```python
import tabula
dataframe = tabula.read_pdf(link, pages='all')
# depending on the table format, you may need to reset the index of the pandas DataFrame
dataframe.reset_index(inplace=True)
```

### Requests

In order to connect to API endpoints, [Requests](https://pypi.org/project/requests/) was used to make HTTPS GET requests.

```python
import requests
# make HTTPS GET request using URL of API endpoint and any necessary headers, i.e. x-api-key
response = requests.get({API_URL}, headers={HEADER_DICTIONARY})
# convert JSON response to pandas DataFrame
new_dataframe = pd.DataFrame(response.json(), index=[0])
```

### python-dotenv

When hosting code on Github or any other public repository, it's a good idea to keep any API keys or database credentials
separate from the hosted code. This can be done by using a .env file that is added to the .gitignore.
[python-dotenv](https://pypi.org/project/python-dotenv/)
