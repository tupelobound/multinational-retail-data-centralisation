import pandas as pd

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
        # TODO - cleaning of store details
        return stores

    def clean_products_data(self, dataframe):
        products = dataframe
        # TODO - cleaning of product details
        products.dropna(inplace=True)
        self.convert_product_weights(products)
        return products
    
    def convert_product_weights(self, dataframe):
        products = dataframe
        # TODO - conversion logic
        products['weight'] = products['weight'].apply(lambda x: x + '*')
        return products
    
    def clean_orders_data(self, dataframe):
        orders = dataframe
        # drop redundant index column
        orders.drop('level_0', axis=1, inplace=True)
        # TODO - cleaning of orders details
        print(orders.info())
        return orders
    
    def clean_date_events(self, dataframe):
        date_events = dataframe
        # TODO - cleaning of store details
        return date_events