from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

aws_connector = DatabaseConnector('aws_creds.yaml')
local_connector = DatabaseConnector('local_creds.yaml')
extractor = DataExtractor()
cleaner = DataCleaning()

#print(aws_connector.list_db_tables())
#users = extractor.read_rds_table(aws_connector, 'legacy_users')
#cards = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#stores = extractor.retrieve_stores_data()
products = extractor.extract_from_s3('s3://data-handling-public/products.csv')
#orders = extractor.read_rds_table(aws_connector, 'orders_table')
#date_events = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

#local_connector.upload_to_db(cleaner.clean_user_data(users), 'dim_users')
#local_connector.upload_to_db(cleaner.clean_card_data(cards), 'dim_card_details')
#local_connector.upload_to_db(cleaner.clean_store_data(stores), 'dim_store_details')
local_connector.upload_to_db(cleaner.clean_products_data(products), 'dim_products')
#local_connector.upload_to_db(cleaner.clean_orders_data(orders), 'orders_table')
#local_connector.upload_to_db(cleaner.clean_date_events(date_events), 'dim_date_times')

