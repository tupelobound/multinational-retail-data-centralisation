from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

aws_connector = DatabaseConnector('aws_creds.yaml')
local_connector = DatabaseConnector('local_creds.yaml')
extractor = DataExtractor()
cleaner = DataCleaning()

users = extractor.read_rds_table(aws_connector, 'legacy_users')
cards = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
local_connector.upload_to_db(cleaner.clean_user_data(users), 'dim_users')
local_connector.upload_to_db(cards, 'dim_card_details')

