import pandas as pd
import tabula

class DataExtractor:
    def read_rds_table(self, connector, table):
        engine = connector.init_db_engine()
        return pd.read_sql_table(table, engine)
    
    def retrieve_pdf_data(self, link):
        return pd.concat(tabula.read_pdf(link, pages='all'))