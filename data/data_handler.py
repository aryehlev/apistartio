"""module that deals with the data operations

contains DataHandler class
"""
import time
import statistics
import sys
import mysql.connector
from mysql.connector import errorcode
from data.sql_commands import TABLES, load_files_querys  # pylint: disable=import-error

DB_NAME = 'startio'




class DataHandler():
    """deals with all data operations
    """
    def __init__(self):
        """initializes object variables to empty for later debuging purposes
        """
        self.database = None
        self.cursor = None


    def setup_schema(self, host, user, password):
        """function tha deals with the whole schema setting up process by initiating
            connection to local/remote database and loading data into it

        Args:
            host (string): host name or ip address
            user (string): user name
            password (string): the users password
        """
        self.connect_to_schema(host, user, password)

        self.create_tables()
        self.load_data_into_tables()

        self.set_user_id_index()

    def connect_to_schema(self ,host, user, password):
        """trys connecting to the given schema and sets up cursor and database object

        Args:
            host (string): host name or ip address
            user (string): user name
            password (string): the users password
        """
        try:
            print("Conncting to database startio: ", end='')
            self.database = mysql.connector.connect(
                                    host=host,
                                    user=user,
                                    port=3306,
                                    password=password,
                                    allow_local_infile=True
                                    )

            self.cursor = self.database.cursor(buffered=True)
            self.connect_to_database()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                print("does not exist.")
            else:
                print(err.msg)
        else:
            print("done")


    def connect_to_database(self):
        """trys conecting to actual startio database in the schema and calls the function to create
        a database if does not exist
        """
        try:
            self.cursor.execute("USE {}".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Database {} does not exists.".format(DB_NAME))
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database()
                print("Database {} created successfully.".format(DB_NAME))
                self.database.database = DB_NAME
            else:
                print(err)
                sys.exit(1)

    def create_database(self):
        """creats a startio database in schema
        """
        try:
            self.cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            sys.exit(1)


    def create_tables(self):
        """creates the tables 'requests', 'impressions', 'clicks' in the database
        the sql commands are taken from the sql_commands file
        """
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
        """loads the data from 'requests.csv', 'impressions.csv', 'clicks.csv'
        into the corresponding tables
        in the schema the sql commands are taken from the sql_commands file
        """

        for table_name in load_files_querys:
            query = load_files_querys[table_name]
            try:
                print("loading data in table {}: ".format(table_name), end='')
                self.cursor.execute(query)
                self.database.commit()
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ERROR_ON_READ:
                    print("cant read file!.")
                else:
                    print(err.msg)
            else:
                print("done")

    def set_user_id_index(self):
        """sets an index on the user_id in the requests file to speed up search
        """
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
        """gets session info (called from api handler)
            1.Begin: request timestamp
            2.Finish: latest timestamp (request/click/impression)
            3.Partner name

        Args:
            session_id (string): session id to get info from
        Returns:
            dict: the resulting data in dictionary form with these corresponding keys
            1.begin
            2.finish
            3.partner
        """
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
        """function that gets a users  info based on id.
            gets this info:
            1.Num of requests
            2.Num of impressions
            3.Num of clikcs
            4.Average price for bid (include only wins)
            5.Median Impression duration
            6.Max time passed till click

        Args:
            user_id (string): the user id to get the info for

        Returns:
            dict: returns dictionary with these keys corresponding to the values stated above
            1.num_of_requests
            2.num_of_impression
            3.num_of_clicks
            4.avg_price_bid
            5.median_impression_duration
            6.max_time_passed
        """
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
        result['avg_price_for_bid_won'] = sum([row[0] for row in list_won_bids])\
            / len(list_won_bids)


        durations_in_list = map(lambda row: row[1], list(filter(lambda row: row[1], rows)))

        result['mediun_impression_duration'] = statistics.median(durations_in_list)


        result['max_time_passed_till_click'] = max(map(lambda row: row[2],
                                    list(filter(lambda row: row[2], rows))))



        print("--- %s seconds ---" % (time.time() - start_time))

        return result

    def close_all(self):
        """closes connection to database
        """
        self.cursor.close()
        self.database.close()
