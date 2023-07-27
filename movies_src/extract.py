# -*- coding: utf-8 -*-
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

try:
    mysql_conn = mysql.connector.connect(
        host = 'oscarvalles.com',
        user = 'oscarval_user',
        password = 'learnsql123',
        database = 'oscarval_sql_course'
    )
    print('Connection Successful')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check Credentials')
    else:
        print('Error')

class Extract:
    
    def __init__(self):
        query = """
                select year, title, country, genre, avg_vote, duration
                from imdb_movies;
                """
        self.df = pd.read_sql(query, mysql_conn)
        self.df.to_csv('extracted_df.csv', index = False)
        print("Exported the extracted data")
        
    def get_df(self):
        return self.df
