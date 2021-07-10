"""module that contains the sql commands
"""
from collections import OrderedDict

TABLES = OrderedDict()

TABLES['requests'] = '''
    CREATE TABLE requests
    (timestamp_requests int,
    session_id VARCHAR(60) PRIMARY KEY,
    partner VARCHAR(20),
    user_id VARCHAR(60),
    bid FLOAT, 
    win VARCHAR(6))
     '''

TABLES['impressions'] = '''
    CREATE TABLE impressions
    (timestamp_impressions int, 
    session_id VARCHAR(60) PRIMARY KEY, 
    duration int,
    FOREIGN KEY (session_id)
    REFERENCES requests(session_id)
    ON DELETE CASCADE)
      '''

TABLES['clicks'] = '''
    CREATE TABLE clicks
    (timestamp_clicks int, 
    session_id VARCHAR(60) PRIMARY KEY,
    time int,
    FOREIGN KEY (session_id)
    REFERENCES requests(session_id)
    ON DELETE CASCADE)
      '''

load_files_querys = OrderedDict()

load_files_querys['requests'] = '''
                LOAD DATA LOCAL INFILE  
                'data/requests.csv'
                INTO TABLE requests 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'

                ( timestamp_requests, session_id , partner ,user_id , bid , win);'''

load_files_querys['clicks'] = '''
                LOAD DATA LOCAL INFILE  
                'data/clicks.csv'
                INTO TABLE clicks 
                FIELDS TERMINATED BY ',' 
            
                LINES TERMINATED BY '\n'

                ( timestamp_clicks , session_id , time);'''

load_files_querys['impressions'] = '''
                LOAD DATA LOCAL INFILE  
                'data/impressions.csv'
                INTO TABLE impressions 
                FIELDS TERMINATED BY ',' 
                
                LINES TERMINATED BY '\n'

                ( timestamp_impressions , session_id , duration);'''
