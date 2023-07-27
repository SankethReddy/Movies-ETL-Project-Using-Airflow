# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import pandas as pd

class Load:
    
    def __init__(self, connection_string, table_name):
        self.df = pd.read_csv('transformed_df.csv')
        self.connection_string = connection_string
        self.table_name = table_name
        
    def load_to_sql(self):
        engine = create_engine(self.connection_string)
        print('Load Engine Connection Successful') 
        self.df.to_sql(self.table_name, engine, if_exists='replace', index=False)
        print('Dataframe has been loaded into MySQL DB') 