'''
@author: Pruthviraj R Patil
@NetID: prp7650
'''

import datetime
import time
from math import sqrt
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class Authentication:
    # Init all the necessary variables when instantiating the class
    def __init__(self, currency_pairs):
        self.currency_pairs = currency_pairs
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"

    # Function slightly modified from polygon sample code to format the date string
    def ts_to_datetime(self, ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("DROP TABLE " + curr[0] + curr[1] + "_raw;"))
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
    def initialize_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE " + curr[0] + curr[
                    1] + "_agg(inserttime text, avgfxrate  numeric, stdfxrate numeric);"))

    # This function is called every 6 minutes to aggregate the data, store it in the aggregate table, and then delete the raw data
    def aggregate_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, COUNT(fxrate) as tot_count FROM " + curr[0] + curr[1] + "_raw;"))
                for row in result:
                    avg_price = row.avg_price
                    tot_count = row.tot_count
                std_res = conn.execute(text(
                    "SELECT SUM((fxrate - " + str(avg_price) + ")*(fxrate - " + str(avg_price) + "))/(" + str(
                        tot_count) + "-1) as std_price FROM " + curr[0] + curr[1] + "_raw;"))
                for row in std_res:
                    std_price = sqrt(row.std_price)
                date_res = conn.execute(text("SELECT MAX(ticktime) as last_date FROM " + curr[0] + curr[1] + "_raw;"))
                for row in date_res:
                    last_date = row.last_date
                conn.execute(text("INSERT INTO " + curr[0] + curr[
                    1] + "_agg (inserttime, avgfxrate, stdfxrate) VALUES (:inserttime, :avgfxrate, :stdfxrate);"),
                             [{"inserttime": last_date, "avgfxrate": avg_price, "stdfxrate": std_price}])

    def getData(self):
        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        # Open a RESTClient for making the api calls
        # Loop that runs until the total duration of the program hits 24 hours.
        with RESTClient(self.key) as client:
            while count < 86400:  # 86400 seconds = 24 hours
                # Make a check to see if 6 minutes has been reached or not
                if agg_count == 360:
                    # Aggregate the data and clear the raw data tables
                    self.aggregate_raw_data_tables()
                    self.reset_raw_data_tables()
                    agg_count = 0
                # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15 seconds to run
                time.sleep(0.75)
                # Increment the counters
                count += 1
                agg_count += 1
                # Loop through each currency pair
                for currency in self.currency_pairs:
                    # Set the input variables to the API
                    from_ = currency[0]
                    to = currency[1]
                    # Call the API with the required parameters
                    try:
                        resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
                    except:
                        continue
                    # This gets the Last Trade object defined in the API Resource
                    last_trade = resp.last
                    # Format the timestamp from the result
                    dt = self.ts_to_datetime(last_trade["timestamp"])
                    # Get the current time and format it
                    insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # Calculate the price by taking the average of the bid and ask prices
                    avg_price = (last_trade['bid'] + last_trade['ask']) / 2
                    # Write the data to the SQLite database, raw data tables
                    with self.engine.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),
                                     [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])