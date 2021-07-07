import pandas as pd
from pathlib import Path
import time
import numpy as np

class data_handler():
    
    def setup_df(self):
        
        start_time = time.time()


        headers_requests = ['timestamp_requset', 'session_id', 'partner', 'user_id', 'bid', 'win']
        path = Path.cwd() / 'data'

        
        df_requests = pd.read_csv(path / 'requests.csv', names = headers_requests)

        headers_impressions =  ['timestamp_impression', 'session_id','duration']

        df_impressions = pd.read_csv(path / 'impressions.csv', names = headers_impressions)
        

        headers_clicks = ['timestamp_clicks', 'session_id','time']

        df_clicks = pd.read_csv(path / 'clicks.csv', names = headers_clicks)
        

        df = df_requests.merge(right=df_impressions, how='outer', on='session_id', indicator='requests_and_impressions')

        self.session_df = df.merge(right=df_clicks, how='outer', on='session_id', indicator='all')

       
        print("--- %s seconds ---" % (time.time() - start_time))
        
        
    async def get_user_info(self, user_id):
        start_time = time.time()
        user = {}
        rows_with_user_id = self.session_df.loc[self.session_df['user_id'] == user_id]
        user['num_of_requests'] = len(rows_with_user_id)
        
        rows_with_impression = rows_with_user_id.loc[self.session_df['requests_and_impressions'] == 'both']
        user['num_of_impression'] = len(rows_with_impression)

        rows_with_clicks = rows_with_user_id.loc[self.session_df['all'] == 'both']
        user['num_of_clicks'] = len(rows_with_clicks)
        
        rows_won = rows_with_user_id.loc[self.session_df['win'] == True]
       
        if  len(rows_won) == 0:
            user['avg_price_bid'] = None
        else:
            user['avg_price_bid'] = rows_won['bid'].sum() / len(rows_won)


        user['median_impression_duration'] = rows_with_impression['duration'].median(skipna=True)

        user['max_time_passed'] = rows_with_clicks['time'].max(skipna=True)
        print("--- %s seconds ---" % (time.time() - start_time))
        
        return user
    
        
    async def get_session_info(self, session_id):
        start_time = time.time()
        session = {}
        session_row = self.session_df.loc[self.session_df['session_id'] == session_id].values[0]
      
        request_timestamp = session_row[0]

        impression_timestamp = session_row[6]

        click_timestamp = session_row[9]

        # print([click_timestamp, request_timestamp, impression_timestamp])
        max_timestamp = np.nanmax([click_timestamp, request_timestamp, impression_timestamp])

        session['begin'] = request_timestamp

        session['finish'] = max_timestamp

        session['partner'] = session_row[2]
        print("--- %s seconds ---" % (time.time() - start_time))
        return session
     

if __name__ == '__main__':
    dh = data_handler()
    dh.setup_df()

    dh.get_user_info('efb64b4e-3655-4a4a-af2d-4d62945eb6d0')

    dh.get_session_info('8df4f33b-ba8b-4d82-ab42-46d5fc72f8d0')

    

