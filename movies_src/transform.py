# -*- coding: utf-8 -*-
import pandas as pd

class Transform:
    
    def __init__(self):
        self.df = pd.read_csv('extracted_df.csv')
        self.df['movie_rating'] = [self.get_movie_rating(v) for v in self.df['avg_vote']]
        self.df['watchability'] = [self.get_watchability(d) for d in self.df['duration']]
        self.df.to_csv('transformed_df.csv', index = False)
        print("Exported the transformed data")
    
    def get_movie_rating(self, vote):
        if vote < 3:
            return 'Bad'
        elif vote < 6:
            return 'Okay'
        else:
            return 'Good'
    
    def get_watchability(self, duration):
        if duration < 60:
            return 'Short Movie'
        elif duration < 90:
            return 'Average Length Movie'
        elif duration < 5000:
            return 'Really Long Movie'
        else:
            return 'No Data'
        
    def get_df(self):
        return self.df