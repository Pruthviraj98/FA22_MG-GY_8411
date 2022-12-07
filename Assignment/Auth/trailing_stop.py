# '''
# @author: Pruthviraj R Patil
# @NetID: prp7650
# '''


import collections
import datetime
import time
import csv
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class Trailing_Stop:
    # the currency pair dictionary is of the type: [curr 1, curr 2, 100 (initial bought curr), bool (0: alive, 1: stopped)]
    def __init__(self, shorts, longs):
        # EURUSD, GBPUSD, USDCHF, USDCAD, USDHKD, USDAUS, USDNZD, and USDSGD.
        self.currency_pairs = [["EUR", "USD", 100, 0],
                               ["GBP", "USD", 100, 0],
                               ["USD", "CHF", 100, 0],
                               ["USD", "CAD", 100, 0],
                               ["USD", "HKD", 100, 0],
                               ["USD", "AUS", 100, 0],
                               ["USD", "NZD", 100, 0],
                               ["USD", "SGD", 100, 0]]
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)
        self.short_indices = shorts
        self.long_indices = longs

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
        mean_dictionary = collections.defaultdict(float)
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

                #getting mean
                mean_dictionary[curr[0] + curr[1]] = stats_vals[0]

                #get all data in the dictionary
                low_bound_dictionary[curr[0] + curr[1]] = lower_bounds
                upper_bound_dictionary[curr[0] + curr[1]] = upper_bounds

        return low_bound_dictionary, upper_bound_dictionary, mean_dictionary

    '''
    This function computes returns for every currency and returns it in the form of dictionary.
    '''
    def compute_r_i(self, previous_mean, current_mean):
        ri_dictionary = collections.defaultdict(float)
        for curr in self.currency_pairs:
            key = curr[0] + curr[1]
            prev_mean = previous_mean[key]
            curr_mean = current_mean[key]
            ri = 0
            if prev_mean != 0:
                ri = (curr_mean - prev_mean) / prev_mean
            ri_dictionary[key] = ri
        return ri_dictionary


    def compute_fd(self, iteration, lower_bounds, upper_bounds, r_is, outputFileName):
        with self.engine.begin() as conn:
            file = open(outputFileName, 'a')
            writer = csv.writer(file)

            for curr in self.currency_pairs:
                key = curr[0] + curr[1]
                result = conn.execute(text("SELECT fxrate from " + key + "_raw;"))
                result_stat = conn.execute(text("SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + key + "_raw;"))

                # for every bound, check how many data points will cross it
                count = 0
                for i in range(100):
                    # iterate through each row and check if it passes the current bound
                    for row in result:
                        if upper_bounds[key][i] <= row.fxrate or lower_bounds[key][i] >= row.fxrate:
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
                writer.writerow([iteration, key, min_price, max_price, avg_price, volatility, fd, r_is[key]])


    def write_to_csv(self, outputFile, interval, key, pl, status):
        file = open(outputFile, 'a')
        writer = csv.writer(file)
        writer.writerow([interval, key, pl, status])

    '''
    first layer = loss tolerance = 0.250% ; invest if pass = 100
    second layer = loss tolerance = 0.150% ; invest if pass = 100
    third layer = loss tolerance = 0.100% ; invest if pass = 100
    fourth layer = loss tolerance = 0.050% ; invest if pass = 100
    fifth layer or more = loss tolerance = 0.050% ; invest if pass = 0
    
    About layers: 
    1. Goes through all the currency pairs and operates only if the current pair is alive
    2. If alive, parses if the currency is in the short or long
    3. If short, checks against negative return and checks if passes the loss tolerance
    4. If long, checks against positive return and checks if passes the loss tolerance
    5. If passed on either cases, the corresponding short or long csv files are appended with the profit values and metadata.
    '''

    def layers_check(self, layer_number, ri_sums, longFileName, shortFileName, tolerence):
        for i in range(len(self.currency_pairs)):
            index = 0
            key = self.currency_pairs[i][0] + self.currency_pairs[i][1]
            if i in self.short_indices:
                index = 1
            if not self.currency_pairs[i][3] == 1:
                current_ri_sum = ri_sums[key]
                # if the current pair is not dead, proceed
                # now check if the current curr falls in long or short
                if index == 1:
                    # check if the return is negative and percentage is less than tolerence
                    if current_ri_sum <= 0 and abs(current_ri_sum) * 100 < tolerence:
                        # Invest only if layer is <= 4
                        if layer_number <= 4:
                            self.currency_pairs[i][2] += current_ri_sum*100 + 100
                        else:
                            self.currency_pairs[i][2] += current_ri_sum*100
                    else:
                        # not then, cut the trade
                        self.currency_pairs[i][3] = 1
                    # after calculating profit/loss, enter it into csv file
                    self.write_to_csv(shortFileName, "T"+str(layer_number*10), key, self.currency_pairs[i][2], "Live")
                else:
                    # check if the return is positive and percentage is greater than 0.250%
                    if current_ri_sum <= 0 and abs(current_ri_sum) * 100 > tolerence:
                        # Invest only if layer is <= 4. Else, just add profit
                        if layer_number <= 4:
                            self.currency_pairs[i][2] += current_ri_sum*100 + 100
                        else:
                            self.currency_pairs[i][2] += current_ri_sum*100
                    else:
                        # not then, cut the trade
                        self.currency_pairs[i][3] = 1
                    # after calculating profit/loss, enter it into csv file
                    self.write_to_csv(longFileName, "T"+str(layer_number*10), key, self.currency_pairs[i][2], "Live")
            else:
                if index == 1:
                    self.write_to_csv(shortFileName, "T"+str(layer_number*10), key, self.currency_pairs[i][2], "Stopped")
                else:
                    self.write_to_csv(longFileName, "T"+str(layer_number*10), key, self.currency_pairs[i][2], "Stopped")


    '''
    Steps followed by getData module:
    1. Gets forex data every second.
    2. In the 6th minute, it calculates the Keltner Bounds, violations, and returns starting from the 12th minute in 6 minute intervals.
    3. Adds the returns to the returns sums dictionary.
    4. In the every 60th minute, using returns sums dictionary, it checks for the calculates profits and either stops/continues trade by executing trailing stop layers employing respective loss tolerances 
    5. In step 2 and step 4, corresponding data and metadata are attached to the csv files. 
    6. Every hour, the returns sums dictionary is re initialized for fresh summation.
    The in depth comments are found in the module
    '''

    def getData(self, outputFileName, longFileName, shortFileName):
        count = 0
        agg_count = 0
        iteration = 0

        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        client = RESTClient(self.key)
        previous_lower_bounds, previous_upper_bounds = [], []
        ri_sum = collections.defaultdict(float)
        previous_means = None

        while count <= 3600*10:  # 10 hours
            if count % 100 == 0:
                print(str(count) + " seconds passed")

            # first layer if 1 hour passed
            if count == 3600:
                self.layers_check(1, ri_sum, longFileName, shortFileName, 0.250)
            # second layer if 2 hour passed
            elif count == 3600*2:
                self.layers_check(2, ri_sum, longFileName, shortFileName, 0.150)
            # third layer if 3 hour passed
            elif count == 3600*3:
                self.layers_check(3, ri_sum, longFileName, shortFileName, 0.100)
            # fourth layer if 4 hour passed
            elif count == 3600*4:
                self.layers_check(4, ri_sum, longFileName, shortFileName, 0.050)
            # fifth layer if more than or equal to 5 hour passed
            elif count > 3600*4 and count % 3600 == 0:
                self.layers_check((count//3600), ri_sum, longFileName, shortFileName, 0.050)

            ri_sum = collections.defaultdict(float)
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # aggregate and get upper, lower bounds and means
                lower_bounds, upper_bounds, means = self.aggregate_raw_data_tables()
                # in the first iteration, we cannot calculate violations. So, if iteration is zero, just take bounds and skip.
                if iteration == 0:
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    previous_means = means
                    self.reset_raw_data_tables()
                    agg_count = 0
                else:
                    # from the second iteration,if count is greater than 1,
                    # 1. compute r_i's for every currency
                    r_is = self.compute_r_i(previous_means, means)
                    # 2. add the current r_i's to the summations
                    for curr in self.currency_pairs:
                        ri_sum[curr[0] + curr[1]] += r_is[curr[0] + curr[1]]
                    # 3. calculate the violations and ri's using the previously stored data points.
                    self.compute_fd(iteration + 1, previous_lower_bounds, previous_upper_bounds, r_is, outputFileName)
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    previous_means = means
                    self.reset_raw_data_tables()
                    agg_count = 0

                iteration += 1
                print("Iteration - " + str(iteration) + " completed.")

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
