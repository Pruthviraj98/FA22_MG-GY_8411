# '''
# @author: Pruthviraj R Patil
# @NetID: prp7650
# '''

import collections
import datetime
import time
import csv
from math import sqrt
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class Authentication:
    """
    Fetch Data from polygon API and store in sqllite database

    :param key: polygon.io key store in library credentials
    :type key: string

    :param currency_pairs: A dictionary defining the set of currency pairs we will be pulling data for.
    :type currency_pairs: dictionary

    :param count : counter in seconds to check program hits 24 hours.
    :type count:  int

    :param agg_count: counter in seconds to check if 6 minutes has been reached or not
    :type agg_count: int

    :param engine : Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    :type engine: sqlalchemy.create_engine
    """

    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.currency_pairs = [["AUD", "USD"],
                               ["GBP", "EUR"],
                               ["USD", "CAD"],
                               ["USD", "JPY"],
                               ["USD", "MXN"],
                               ["EUR", "USD"],
                               ["USD", "CNY"],
                               ["USD", "CZK"],
                               ["USD", "PLN"],
                               ["USD", "INR"]
                               ]
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)


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
    def calc_keltner_bonds(self, volatility, average):
        upper_bounds = []
        lower_bounds = []
        for i in range(100):
            upper_bounds.append(average + (i + 1) * 0.025 * volatility)
            lower_bounds.append(average - (i + 1) * 0.025 * volatility)
        return upper_bounds, lower_bounds


    def aggregate_raw_data_tables(self):
        low_bound_dictionary = collections.defaultdict(list)
        upper_bound_dictionary = collections.defaultdict(list)
        result_dictionary = collections.defaultdict(list)
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + curr[0] + curr[1] + "_raw;"))

                #getting avg, max, min for every curr in 6 minutes
                stats_vals = []
                for row in result:
                    stats_vals.append(row.avg_price)
                    stats_vals.append(row.min_price)
                    stats_vals.append(row.max_price)
                    stats_vals.append(row.max_price - row.min_price)

                #Get the bounds for every currency in 6 minutes
                upper_bounds, lower_bounds = self.calc_keltner_bonds(stats_vals[3], stats_vals[0])

                #get all data in the dictionary
                low_bound_dictionary[curr[0] + curr[1]] = lower_bounds
                upper_bound_dictionary[curr[0] + curr[1]] = upper_bounds

        return low_bound_dictionary, upper_bound_dictionary


    def compute_fd(self, lower_bounds, upper_bounds):
        # print(lower_bounds, upper_bounds)
        #start the connections
        with self.engine.begin() as conn:
            file = open('output.csv', 'w', newline='')
            header = ['Min', 'Max', 'Mean', "Vol", "FD"]
            writer = csv.DictWriter(file, fieldnames=header)

            for curr in self.currency_pairs:
                key = curr[0] + curr[1]
                result = conn.execute(text("SELECT fxrate from " + key + "_raw;"))
                result_stat = conn.execute(text("SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + key + "_raw;"))

                # for every bound, check how many data points will cross it
                for i in range(100):
                    count = 0
                    # iterate through each row and check if it passes the current bound
                    for row in result:
                        if upper_bounds[key][i] <= row.fxrate or lower_bounds[key][i] >= row.fxrate:
                            # if crossed, increment
                            count += 1

                    #for every bound, we check for every data point to check if they violate
                    for row in result_stat:
                        max_price = row.max_price
                        avg_price = row.avg_price
                        min_price = row.min_price
                        volatility = row.max_price - row.min_price
                        fd = count
                        if volatility != 0:
                            fd = count/volatility

                    # writing data row-wise into the csv file
                    writer.writerow([min_price,max_price,avg_price,volatility,fd])

                    # for every 6 minutes, we put 100 data points with min, max, mean, vol, fd to CSV. So, we will have 99 * 100 data points after 10 hours

    def getData(self):
        file = open('output.csv', 'w', newline='')
        header = ['Min', 'Max', 'Mean', "Vol", "FD"]
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()

        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0
        temp = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        # Open a RESTClient for making the api calls
        client = RESTClient(self.key)
        # Loop that runs until the total duration of the program hits 24 hours.
        # NOTE :  (86400 changed to 4 here for testing)
        previous_lower_bounds, previous_upper_bounds = [], []
        while count < 86400:  # 86400 seconds = 24 hours
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 60:
                # aggregate and get upper and lower bounds
                lower_bounds, upper_bounds = self.aggregate_raw_data_tables()
                # print(lower_bounds, upper_bounds)
                # if count is 0, we cannot calculate violations.
                if temp == 0:
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    self.reset_raw_data_tables()
                    agg_count = 0
                else:
                    # if count is greater than 1, then we can calculate the violations using the previously stored data points.
                    self.compute_fd(previous_lower_bounds, previous_upper_bounds)
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    self.reset_raw_data_tables()
                    agg_count = 0
                temp += 1

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
                # print(type(last_trade), last_trade.timestamp)
                # Format the timestamp from the result
                dt = self.ts_to_datetime(last_trade.timestamp)
                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Calculate the price by taking the average of the bid and ask prices
                avg_price = (last_trade.bid + last_trade.ask) / 2
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text(
                        "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),
                                 [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
                    # print("inserted", from_+to, dt, avg_price, insert_time)
