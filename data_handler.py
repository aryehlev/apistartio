import pandas as pd
from pathlib import Path

class data_handler():
    def setup_session_df(self):
        headers_requests = ['timestamp_requset', 'session_id', 'partner', 'user_id', 'bid', 'win']
        path = Path.cwd() / 'data'

        
        df_requests = pd.read_csv(path / 'requests.csv', names = headers_requests)

        headers_impressions =  ['timestamp_impression', 'session_id','duration']

        df_impressions = pd.read_csv(path / 'impressions.csv', names = headers_impressions)
        # print(df_impressions.head(10))

        headers_clicks = ['timestamp_clicks', 'session_id','time']

        df_clicks = pd.read_csv(path / 'clicks.csv', names = headers_clicks)
        

        df = df_requests.merge(right=df_impressions, how='outer', on='session_id', indicator='requests_and_impressions')

        self.session_df = df.merge(right=df_clicks, how='outer', on='session_id', indicator='all')

        # with open('check1.txt', 'w') as f:
        #     f.write(str(self.session_df.iloc[100]))

        # with open('check3.txt', 'w') as f:
        #     f.write(str(self.session_df.loc[self.session_df['requests_and_impressions'] == 'right_only']))
    

    def setup_user_df(self):
        all_user_ids = self.session_df['user_id'].unique().to_list()
        self.user_df = pd.DataFrame() 

        for user_id in all_user_ids:
            user = {}
            user['user_id'] = user_id
            rows_with_user_id = self.session_df.loc[self.session_df['user_id'] == user_id]
            user['num_of_requests'] = len(rows_with_user_id)
            
            rows_with_impression = rows_with_user_id.loc[self.session_df['requests_and_impressions'] == 'both']
            user['num_of_impression'] = len(rows_with_impression)

            rows_with_clicks = rows_with_user_id.loc[self.session_df['all'] == 'both']
            user['num_of_clicks'] = len(rows_with_clicks)
            
            rows_won = rows_with_user_id.loc[self.session_df['win'] == True]
            user['avg_price_bid'] = rows_won['bid'].sum() / len(rows_won)

            user['median_impression_duration'] = rows_with_impression['duration'].median(skipna=True)

            user['max_time_passed'] = rows_with_clicks['time'].max(skipna=True)

            self.user_df.append(user, ignore_index=True)



            


