import mysql.connector
from mysql.connector import errorcode
import time
from data.sql_commands import TABLES, load_files_querys
DB_NAME = 'startio'




class data_handler():

    def setup_schema(self, host, user, password):
        self.connect_to_schema(host, user, password)

        self.create_tables()
        self.load_data_into_tables()

        self.set_user_id_index()

    def connect_to_schema(self ,host, user, password):
        try:
            print("Conncting to database startio: ", end='')
            self.db = mysql.connector.connect(
                                    host=host,
                                    user=user,
                                    port=3306,
                                    password=password,
                                    allow_local_infile=True
                                    )
            
            self.cursor = self.db.cursor(buffered=True)
            self.connect_to_database()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                print("does not exist.")
            else:
                print(err.msg)
        else:
            print("done")
            
       
    def connect_to_database(self): 
        try:
            self.cursor.execute("USE {}".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Database {} does not exists.".format(DB_NAME))
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database()
                print("Database {} created successfully.".format(DB_NAME))
                self.db.database = DB_NAME
            else:
                print(err)
                exit(1)
    
    def create_database(self):
            try:
                self.cursor.execute(
                    "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
            except mysql.connector.Error as err:
                print("Failed creating database: {}".format(err))
                exit(1)        

    
    def create_tables(self):
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                self.cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

    def load_data_into_tables(self):
        
        # self.cursor.execute("SET GLOBAL local_infile=1")

        for table_name in load_files_querys:
            query = load_files_querys[table_name]
            try:
                print("loading data in table {}: ".format(table_name), end='')
                self.cursor.execute(query)
                self.db.commit()
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ERROR_ON_READ:
                    print("cant read file!.")
                else:
                    print(err.msg)
            else:
                print("done")
    
    def set_user_id_index(self):
        try:
            create_index_for_user_id = "CREATE INDEX idx_user ON requests (user_id)"
            print("Creating index for user_id in table: ", end='')
            self.cursor.execute(create_index_for_user_id)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_KEYNAME:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("done")

    async def get_session_info(self, session_id):
        start_time = time.time()

        query = f'''
                SELECT requests.timestamp_requests, clicks.timestamp_clicks, impressions.timestamp_impressions, requests.partner
                FROM requests 
                LEFT OUTER JOIN impressions
                ON requests.session_id = impressions.session_id
                LEFT OUTER JOIN clicks
                ON requests.session_id = clicks.session_id
                WHERE requests.session_id='{session_id}'; 
                '''

        self.cursor.execute(query)
        
        row = self.cursor.fetchone()
        result = {}


        result['begin'] = row[0]

        result['finish'] = max(filter(None, row[:3]))
        
        result['partner'] = row[3]

        print("--- %s seconds ---" % (time.time() - start_time))

        return result


    async def get_user_info(self, user_id):
        start_time = time.time()
        
        
        query = f'''
            SELECT requests.bid,  impressions.duration, clicks.time, requests.win
            FROM requests 
            LEFT OUTER JOIN impressions
            ON requests.session_id = impressions.session_id
            LEFT OUTER JOIN clicks
            ON requests.session_id = clicks.session_id
            WHERE requests.user_id='{user_id}'; 
        '''
        self.cursor.execute(query)

        rows = self.cursor.fetchall()

        result = {}

        result['num_requests'] = len(list(filter(lambda row: row[0], rows)))

        result['num_impressions'] = len(list(filter(lambda row: row[1], rows)))

        result['num_clicks'] = len(list(filter(lambda row: row[2], rows)))

        list_won_bids = list(filter(lambda row: 'True' in row[3], rows))
        result['avg_price_for_bid_won'] = sum([row[0] for row in list_won_bids]) / len(list_won_bids)
        
        import statistics
        durations_in_list = map(lambda row: row[1], list(filter(lambda row: row[1], rows)))
        
        result['mediun_impression_duration'] = statistics.median(durations_in_list)


        result['max_time_passed_till_click'] = max(map(lambda row: row[2], list(filter(lambda row: row[2], rows))))



        print("--- %s seconds ---" % (time.time() - start_time))

        return result

    def close_all(self):
        self.cursor.close()
        self.db.close()


if __name__ == '__main__':

    dh = data_handler()
    dh.setup_schema(host="stario.cna1qj9bze8h.us-east-2.rds.amazonaws.com", user="admin",password="lqw120&8%mna")

    # dh.connect_to_schema(host="database-2.cna1qj9bze8h.us-east-2.rds.amazonaws.com", user="root",password="lqw120&8%mna")

    print(dh.get_user_info('efb64b4e-3655-4a4a-af2d-4d62945eb6d0'))

    # print(dh.get_session_info('8df4f33b-ba8b-4d82-ab42-46d5fc72f8d0'))
    dh.close_all()



