import pandas as pd
import re # for regular expressions

class DataCleaning:
    '''This class contains methods for cleaning data from various sources
    
    Methods
    -------
    clean_user_data(self, dataframe):
        Cleans DataFrame containing business user data.
    clean_card_data(self, dataframe):
        Cleans DataFrame containing credit card data from business transactions.
    clean_store_data(self, dataframe):
        Cleans DataFrame containing details of each of the business' stores
    convert_product_weights(self, dataframe):
        Converts values in weight column of DataFrame to kilograms and to type floating point number
    clean_products_data(self, dataframe):
        Cleans DataFrame containing information about all products sold by the business.
    clean_orders_data(self, dataframe):
        Cleans main orders DataFrame.
    clean_date_events(self, dataframe):
        Cleans DataFrame containing date events data for all orders received by the business.
    '''
    def clean_user_data(self, dataframe):
        '''Cleans DataFrame containing business user data.

        Takes a DataFrame containing user data and cleans redundant columns and rows containing null or incorrect values before
        returning the cleaned DataFrame.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing user data
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
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
        '''Cleans DataFrame containing credit card data from business transactions.
        
        Takes a DataFrame containing credit card data and cleans null and incorrect values and returns the cleaned DataFrame.

        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing credit card data
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
        cards = dataframe
        # reset index
        cards.reset_index(inplace=True, drop=True)
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
        '''Cleans DataFrame containing details of each of the business' stores.
        
        Takes a DataFrame containing store information from different countries, drops redundant columns, cleans null or
        incorrect values, and returns cleaned DataFrame.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing detailed store information
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
        stores = dataframe
        # drop redundant index and lat columns
        stores.drop(['index', 'lat'], axis=1, inplace=True)
        # drop rows that contain 'NULL' strings
        stores.drop(stores[stores.country_code == 'NULL'].index, inplace=True)
        # drop rows where country code is not standard 2 characters in length
        stores.drop(stores[stores['country_code'].str.len() != 2].index, inplace=True)
        # convert opening date column to datetime type
        stores['opening_date'] = pd.to_datetime(stores['opening_date'])
        # change N/A longitude value for web portal store
        stores.loc[[0], ['longitude']] = pd.NA
        # change location values for web portal store
        stores.loc[[0], ['country_code', 'continent']] = 'N/A'
        # clean incorrect values in continent column
        stores['continent'] = stores['continent'].apply(lambda x: x[2:] if x[:2] == 'ee' else x)
        # clean text from staff_numbers column
        stores['staff_numbers'] = stores['staff_numbers'].str.replace('[^0-9]', '', regex=True)
        return stores

    def convert_product_weights(self, dataframe):
        '''Converts values in weight column of DataFrame to kilograms and to type floating point number.
        
        Takes a DataFrame of products and utilises an internal method to strip weights of unit strings and convert weights to
        floats before returning the DataFrame with converted weights. 
        
         Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing product information, including 'weight' column
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            pandas DataFrame with cleaned 'weight column'
        '''
        products = dataframe
        def strip_and_convert_to_float(weight: str):
            '''Method to strip unit strings, convert to float data type, and convert to kilograms.'''
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
        # apply strip_and_convert_to_float() method to dataframe 'weight' column
        products['weight'] = products['weight'].apply(strip_and_convert_to_float)
        return products
    
    def clean_products_data(self, dataframe):
        '''Cleans DataFrame containing information about all products sold by the business.
        
        Takes a DataFrame containing information about the products sold by the business, utilises the convert_product_weights()
        method to convert all weights to kilograms, cleans null or incorrect values and drops redundant columns before returning
        cleaned DataFrame.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing detailed product information
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
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
    
    def clean_orders_data(self, dataframe):
        '''Cleans main orders DataFrame.
        
        Drops redundant columns from DataFrame containing all the orders for the business before returning cleaned DataFrame.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing order data
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
        orders = dataframe
        # drop redundant columns
        orders.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        return orders
    
    def clean_date_events(self, dataframe):
        '''Cleans DataFrame containing date events data for all orders received by the business.
        
        Drops rows containing null or incorrect values from DataFrame containing sales event time data before returning the
        cleaned DataFrame.
        
        Parameters
        ----------
        dataframe: pandas.core.frame.DataFrame
            pandas DataFrame containing order date and time event data
        
        Returns
        -------
        users: pandas.core.frame.DataFrame
            Cleaned pandas DataFrame
        '''
        date_events = dataframe
        # drop rows that contain 'NULL' strings
        date_events.drop(date_events[date_events.timestamp == 'NULL'].index, inplace=True)
        # drop rows where date uuid is not standard 36 characters in length
        date_events.drop(date_events[date_events['date_uuid'].str.len() != 36].index, inplace=True)
        return date_events