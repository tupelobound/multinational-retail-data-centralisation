from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

connector = DatabaseConnector()
extractor = DataExtractor()
cleaner = DataCleaning()

users = extractor.read_rds_table(connector, 'legacy_users')
cleaner.clean_user_data(users).to_csv('users.csv')

