import pandas as pd

class DataCleaning:
    def clean_user_data(self, dataframe):
        users = dataframe
        # drop rows that contain 'NULL' strings
        users.drop(users[users.first_name == 'NULL'].index, inplace=True)
        # drop rows where unique user id is not standard 36 characters in length
        users.drop(users[users['user_uuid'].str.len() != 36].index, inplace=True)
        # drop redundant index column
        users.drop('index', axis=1, inplace=True)
        # remove line breaks from addresses
        users['address'] = users['address'].str.replace('\n', ' ')
        # convert date of birth column to datetime type
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'])
        # convert join date column to datetime type
        users['join_date'] = pd.to_datetime(users['join_date'])
        return users