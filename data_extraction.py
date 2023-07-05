from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:
    def read_rds_table(self, connector, table):
        engine = connector().init_db_engine()
        return pd.read_sql_table(table, engine)


if __name__ == "__main__":
    test = DataExtractor()
    print(test.read_rds_table(DatabaseConnector, 'legacy_users').info())