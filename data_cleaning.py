import pandas as pd
import re

class DataCleaning:
    def clean_user_data(self, dataframe):
        users = dataframe
        # drop redundant index column
        users.drop('index', axis=1, inplace=True)
        # drop rows that contain 'NULL' strings
        users.drop(users[users.first_name == 'NULL'].index, inplace=True)
        # drop rows where unique user id is not standard 36 characters in length
        users.drop(users[users['user_uuid'].str.len() != 36].index, inplace=True)
        # remove line breaks from addresses
        users['address'] = users['address'].str.replace('\n', ' ')
        # convert date of birth column to datetime type
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'])
        # convert join date column to datetime type
        users['join_date'] = pd.to_datetime(users['join_date'])
        # drop users whose birthday is later than their join date
        # users.drop(users[users['date_of_birth'] > users['join_date']].index, inplace=True)
        # correct 'GGB' values in country code column
        users['country_code'] = users['country_code'].apply(lambda x: 'GB' if x == 'GGB' else x)
        # remove country codes and/or extensions from phone numbers
        users['phone_number'] = users['phone_number'].str.replace('\+1|\+44|\+49|x\w+', '', regex=True)
        # remove non-numeric characters from phone numbers
        users['phone_number'] = users['phone_number'].str.replace('\D+', '', regex=True)
        # strip remaining country codes from US numbers
        users['phone_number'] = users.apply(lambda x: x['phone_number'][-10:] if x['country_code'] == 'US' else x['phone_number'], axis=1)
        # add missing preceding '0' to GB numbers
        users['phone_number'] = users.apply(lambda x: '0' + x['phone_number'] if x['country_code'] == 'GB' and x['phone_number'][0] != '0' else x['phone_number'], axis=1)
        return users

    def clean_card_data(self, dataframe):
        cards = dataframe
        # drop rows that contain 'NULL' strings
        cards.drop(cards[cards.card_number == 'NULL'].index, inplace=True)
        # drop rows where expiry date is not standard 5 characters in length
        cards.drop(cards[cards['expiry_date'].str.len() != 5].index, inplace=True)
        # cast card numbers as strings
        cards['card_number'] = cards['card_number'].astype(str)
        # remove question marks from card numbers
        cards['card_number'] = cards['card_number'].str.replace('\D+', '', regex=True)
        # convert date payment confirmed column to datetime type
        cards['date_payment_confirmed'] = pd.to_datetime(cards['date_payment_confirmed'])
        return cards
    
    def clean_store_data(self, dataframe):
        stores = dataframe
        # drop redundant index and lat columns
        stores.drop(['index', 'lat'], axis=1, inplace=True)
        # drop rows that contain 'NULL' strings
        stores.drop(stores[stores.country_code == 'NULL'].index, inplace=True)
        # drop rows where country code is not standard 2 characters in length
        stores.drop(stores[stores['country_code'].str.len() != 2].index, inplace=True)
        # convert opening date column to datetime type
        stores['opening_date'] = pd.to_datetime(stores['opening_date'])
        # change null value for web portal store
        stores.loc[[0], ['latitude']] = 'N/A'
        # clean incorrect values in continent column
        stores['continent'] = stores['continent'].apply(lambda x: x[2:] if x[:2] == 'ee' else x)
        return stores

    def clean_products_data(self, dataframe):
        products = dataframe
        # drop rows with null values
        products.dropna(inplace=True)
        # convert product weights to kilogram floats
        self.convert_product_weights(products)
        # drop rows where weights are strings
        products.drop(products[products['weight'].apply(lambda x: isinstance(x, str))].index, inplace=True)
        # drop redundant index column
        products.drop('Unnamed: 0', axis=1, inplace=True)
        # convert date_added column to datetime type
        products['date_added'] = pd.to_datetime(products['date_added'])
        return products
    
    def convert_product_weights(self, dataframe):
        products = dataframe
        def strip_and_convert_to_float(weight: str):
            if weight[-2:] == 'kg':
                return float(weight[:-2])
            elif weight.find(' x ') != -1:
                return eval(weight.replace(' x ', '*')[:-1]) / 1000
            elif weight[-1] == 'g' or weight[-2:] == 'ml' or weight.find('.') != -1:
                return float(re.sub('[^0-9]', '', weight)) / 1000
            elif weight[-2:] == 'oz':
                return float(weight[:-2]) * 0.0283495
            else:
                return weight
        products['weight'] = products['weight'].apply(strip_and_convert_to_float)
        return products
    
    def clean_orders_data(self, dataframe):
        orders = dataframe
        # drop redundant columns
        orders.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        return orders
    
    def clean_date_events(self, dataframe):
        date_events = dataframe
        # drop rows that contain 'NULL' strings
        date_events.drop(date_events[date_events.timestamp == 'NULL'].index, inplace=True)
        # drop rows where date uuid is not standard 36 characters in length
        date_events.drop(date_events[date_events['date_uuid'].str.len() != 36].index, inplace=True)
        return date_events