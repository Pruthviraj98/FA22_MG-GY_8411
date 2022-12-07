# '''
# @author: Pruthviraj R Patil
# @NetID: prp7650
# '''


import collections
import csv
import datetime
import math
import os
import time

from polygon import RESTClient
from pycaret.regression import *
from sqlalchemy import create_engine
from sqlalchemy import text


class Trailing_Stop_2:
    # the currency pair dictionary is of the type: [curr 1, curr 2, 100 (initial bought curr), bool (0: alive,
    # 1: stopped)]
    def __init__(self):
        # EURUSD, GBPUSD, USDCHF, USDCAD, USDHKD, USDAUS, USDNZD, and USDSGD.
        self.currency_pairs = [["EUR", "USD", 100, 0],
                               ["GBP", "USD", 100, 0],
                               ["USD", "CHF", 100, 0],
                               ["USD", "CAD", 100, 0],
                               ["USD", "HKD", 100, 0],
                               ["USD", "AUD", 100, 0],
                               ["USD", "NZD", 100, 0],
                               ["USD", "SGD", 100, 0]]

        self.suffix_info = {
            "EURUSD" : "1",
            "GBPUSD" : "2",
            "USDCHF" : "3",
            "USDCAD" : "4",
            "USDHKD" : "5",
            "USDAUD" : "6",
            "USDNZD" : "7",
            "USDSGD" : "8"
        }
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)

        self.VolBounds = dict()
        self.FDBounds = dict()

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

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the
    # SQLite database
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE " + curr[0] + curr[
                    1] + "_agg(inserttime text, avgfxrate  numeric, stdfxrate numeric);"))

    # This function is called every 6 minutes to aggregate the data, store it in the aggregate table, and then delete
    # the raw data
    def calc_keltner_bonds(self, volatility, average):
        upper_bounds = []
        lower_bounds = []
        for i in range(100):
            upper_bounds.append(average + (i + 1) * 0.025 * volatility)
            lower_bounds.append(average - (i + 1) * 0.025 * volatility)
        return upper_bounds, lower_bounds


    # This function calculates bounds and does all the aggregations.
    def aggregate_raw_data_tables(self):
        low_bound_dictionary = collections.defaultdict(list)
        upper_bound_dictionary = collections.defaultdict(list)
        mean_dictionary = collections.defaultdict(float)
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + curr[
                        0] + curr[1] + "_raw;"))

                # getting avg, max, min for every curr in 6 minutes
                stats_vals = []
                for row in result:
                    stats_vals.append(row.avg_price)
                    stats_vals.append(row.min_price)
                    stats_vals.append(row.max_price)
                    stats_vals.append(row.max_price - row.min_price)

                # Get the bounds for every currency in 6 minutes
                upper_bounds, lower_bounds = self.calc_keltner_bonds(stats_vals[3], stats_vals[0])

                # getting mean
                mean_dictionary[curr[0] + curr[1]] = stats_vals[0]

                # get all data in the dictionary
                low_bound_dictionary[curr[0] + curr[1]] = lower_bounds
                upper_bound_dictionary[curr[0] + curr[1]] = upper_bounds

        return low_bound_dictionary, upper_bound_dictionary, mean_dictionary

    '''
    @Briefdescription: 
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


    '''
    @params:
    timestamp: Time at which the data is collected.
    lower_bounds: The lower bounds for checking the count to calculate fd
    upper_bounds: The upper bounds for checking the count to calculate fd
    r_is: returns dicionary
    predict: True/False. True if this is real time prediction. Else, False if we are collecting 10 hours data.
    '''
    def compute_fd(self, timestamp, lower_bounds, upper_bounds, r_is, predict = False):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                file = open('realtime_' + curr[0] + curr[1] + ".csv", 'a')
                writer = csv.writer(file)

                key = curr[0] + curr[1]
                result = conn.execute(text("SELECT fxrate from " + key + "_raw;"))
                result_stat = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + key + "_raw;"))

                # for every bound, check how many data points will cross it
                count = 0
                for i in range(100):
                    # iterate through each row and check if it passes the current bound
                    for row in result:
                        if upper_bounds[key][i] <= row.fxrate or lower_bounds[key][i] >= row.fxrate:
                            count += 1

                    # for every bound, we check for every data point to check if they violate
                for row in result_stat:
                    max_price = row.max_price
                    avg_price = row.avg_price
                    min_price = row.min_price
                    volatility = row.max_price - row.min_price
                    fd = count
                    if volatility != 0:
                        fd = count / volatility

                # writing data row-wise into the csv file

                writer.writerow(
                    [timestamp.strftime("%H:%M:%S"), min_price, max_price, avg_price, volatility, fd, r_is[key]])

                if predict:
                    self.predictData(curr[0]+curr[1], volatility, fd, avg_price, r_is[key])



    '''
    
    @brief description:
    Steps followed by getData module: 
    1. Gets forex data every second. 
    2. In the 6th minute, it calculates the Bounds, and returns starting from the 12th minute in 6 minute intervals. 
    3. In step 2, corresponding data and metadata are attached to the csv files. The in depth comments found in module 
    '''

    def getData(self, predict= False):
        count = 0
        agg_count = 0
        iteration = 0

        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        client = RESTClient(self.key)
        previous_lower_bounds, previous_upper_bounds = [], []
        previous_means = None

        while count <= 3600 * 10:  # 10 hours
            if count % 100 == 0:
                print(str(count) + " seconds passed")
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # aggregate and get upper, lower bounds and means
                lower_bounds, upper_bounds, means = self.aggregate_raw_data_tables()

                # in the first iteration, we cannot calculate violations. So, if iteration is zero, just take bounds
                # and skip.
                if iteration == 0:
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    previous_means = means
                    self.reset_raw_data_tables()
                    agg_count = 0
                else:
                    r_is = self.compute_r_i(previous_means, means)
                    timestamp = datetime.datetime.now()
                    self.compute_fd(timestamp, previous_lower_bounds, previous_upper_bounds, r_is, predict = predict)
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    previous_means = means
                    self.reset_raw_data_tables()
                    agg_count = 0

                iteration += 1
                print("Iteration - " + str(iteration) + " completed.")

            # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15
            # seconds to run
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
                        "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, "
                                                      ":inserttime)"),
                        [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
                    # print("inserted", from_ + to, dt, avg_price, insert_time, currency[0] + currency[1])

    '''
    @brief description
    
    This method does the following by going through the downloaded data for all the currencies.
    a. sorts the all the datasets using three types of sorting : 
        a. Vol and FD independent
        b. First Vol and then FD 
        c. First FD and then Vol
    b. for every sorting output the labelling is done. For first 33 points of Vol, suffix+1, Second 34 points of Vol, 
    Suffix+2 and last 33 values of Vol, suffix+3 is assigned. For first 33 points of FD, suffix+4, Second 34 points of 
    FD, Suffix+5 and last 33 values of FD, suffix+6 is assigned.
    c. Range (bounds) for the first 33, second 34 and third 33 values are stored in self.VolBounds, self.FDBounds dict
    d. Saves the sorted data in csv file. So, for every currency pair, we will have 3 datasets to work on.
    '''
    def sort_and_classify_data(self):
        for curr in self.currency_pairs:
            current_file_name =  curr[0] + curr[1] + ".csv"

            df = pd.read_csv('data_10_hours/' + current_file_name)
            df['return'] = df['return'].apply(lambda x: x*100000)

            sort_type_a_data = df.sort_values(by = ['Vol'], ascending = True)
            sort_type_a_data = sort_type_a_data.sort_values(by = ['FD'], ascending = True)

            sort_type_b_data = df.sort_values(by = ['Vol', 'FD'], ascending = [True, True])
            sort_type_c_data = df.sort_values(by = ['FD', 'Vol'], ascending = [True, True])

            # print(sort_type_a_data, sort_type_b_data, sort_type_c_data)

            #declare essentials for modification
            suffix = self.suffix_info[curr[0]+curr[1]]
            vHigh = int(suffix + "3")
            vMid = int(suffix + "2")
            vLow = int(suffix + "1")
            fdHigh = int(suffix + "6")
            fdMid = int(suffix + "5")
            currFileName = "modified"+curr[0]+curr[1]
            fdLow = int(suffix + "4")

            ### VOL MAX MIN
            self.VolBounds['sortA_max_0_33_Vol'] = sort_type_a_data['Vol'][32]
            self.VolBounds['sortB_max_0_33_Vol'] = sort_type_b_data['Vol'][32]
            self.VolBounds['sortC_max_0_33_Vol'] = sort_type_c_data['Vol'][32]

            self.VolBounds['sortA_max_34_67_Vol'] = sort_type_a_data['Vol'][66]
            self.VolBounds['sortB_max_34_67_Vol'] = sort_type_b_data['Vol'][66]
            self.VolBounds['sortC_max_34_67_Vol'] = sort_type_c_data['Vol'][66]

            self.VolBounds['sortA_max_68_100_Vol'] = sort_type_a_data['Vol'][99]
            self.VolBounds['sortB_max_68_100_Vol'] = sort_type_a_data['Vol'][99]
            self.VolBounds['sortC_max_68_100_Vol'] = sort_type_a_data['Vol'][99]

            self.VolBounds['sortA_min_0_33_Vol'] = sort_type_a_data['Vol'][0]
            self.VolBounds['sortB_min_0_33_Vol'] = sort_type_a_data['Vol'][0]
            self.VolBounds['sortC_min_0_33_Vol'] = sort_type_a_data['Vol'][0]

            self.VolBounds['sortA_min_34_67_Vol'] = sort_type_a_data['Vol'][33]
            self.VolBounds['sortB_min_34_67_Vol'] = sort_type_a_data['Vol'][33]
            self.VolBounds['sortC_min_34_67_Vol'] = sort_type_a_data['Vol'][33]

            self.VolBounds['sortA_min_68_100_Vol'] = sort_type_a_data['Vol'][67]
            self.VolBounds['sortB_min_68_100_Vol'] = sort_type_a_data['Vol'][67]
            self.VolBounds['sortC_min_68_100_Vol'] = sort_type_a_data['Vol'][67]

            ### FD MAX MIN
            self.FDBounds['sortA_max_0_33_FD'] = sort_type_a_data['FD'][32]
            self.FDBounds['sortB_max_0_33_FD'] = sort_type_b_data['FD'][32]
            self.FDBounds['sortC_max_0_33_FD'] = sort_type_c_data['FD'][32]

            self.FDBounds['sortA_max_34_67_FD'] = sort_type_a_data['FD'][66]
            self.FDBounds['sortB_max_34_67_FD'] = sort_type_b_data['FD'][66]
            self.FDBounds['sortC_max_34_67_FD'] = sort_type_c_data['FD'][66]

            self.FDBounds['sortA_max_68_100_FD'] = sort_type_a_data['FD'][99]
            self.FDBounds['sortB_max_68_100_FD'] = sort_type_a_data['FD'][99]
            self.FDBounds['sortC_max_68_100_FD'] = sort_type_a_data['FD'][99]

            self.FDBounds['sortA_min_0_33_FD'] = sort_type_a_data['FD'][0]
            self.FDBounds['sortB_min_0_33_FD'] = sort_type_a_data['FD'][0]
            self.FDBounds['sortC_min_0_33_FD'] = sort_type_a_data['FD'][0]

            self.FDBounds['sortA_min_34_67_FD'] = sort_type_a_data['FD'][33]
            self.FDBounds['sortB_min_34_67_FD'] = sort_type_a_data['FD'][33]
            self.FDBounds['sortC_min_34_67_FD'] = sort_type_a_data['FD'][33]

            self.FDBounds['sortA_min_68_100_FD'] = sort_type_a_data['FD'][67]
            self.FDBounds['sortB_min_68_100_FD'] = sort_type_a_data['FD'][67]
            self.FDBounds['sortC_min_68_100_FD'] = sort_type_a_data['FD'][67]

            #create new Vol and FD list
            newVol = [vLow]*33 + [vMid]*34 + [vHigh]*33
            newFD = [fdLow]*33 + [fdMid]*34 + [fdHigh]*33

            #replace old Vol with newVol
            sort_type_a_data["Vol"] = newVol
            sort_type_b_data["Vol"] = newVol
            sort_type_c_data["Vol"] = newVol

            #replace old FD with newFD
            sort_type_a_data["FD"] = newFD
            sort_type_b_data["FD"] = newFD
            sort_type_c_data["FD"] = newFD

            if not os.path.isdir("/Users/pruthvirajpatil/Desktop/dataengg_Tandon/FA22_MG-GY_8411/Assignment/" + currFileName):
                os.makedirs("/Users/pruthvirajpatil/Desktop/dataengg_Tandon/FA22_MG-GY_8411/Assignment/" + currFileName)

            sort_type_a_data.to_csv(currFileName+"/Sort_type_A" + curr[0]+curr[1] + ".csv", index = False)
            sort_type_b_data.to_csv(currFileName+"/Sort_type_B" + curr[0]+curr[1] + ".csv", index = False)
            sort_type_c_data.to_csv(currFileName+"/Sort_type_C" + curr[0]+curr[1] + ".csv", index = False)


    '''
    @params 
    pair: the currency pair for which the models to be created 
    data: csv file path
    modelname: the name using which the models are saved for real-time prediction purpose
    
    @brief description
    This module takes in the individual sorted data and creates data model for it and saves it. So, for every currency
    pair, we will have 3 data models. 
    '''
    def trainModels(self, pair, data, modelName):
        data = pd.read_csv(data)
        setup(data=data, target="return", ignore_features=['Timestamp', 'Min', 'Max'])
        huber = create_model('huber', verbose=False)
        lasso = create_model('lasso', verbose=False)
        lr = create_model('lr', verbose=False)
        ridge = create_model('ridge', verbose=False)
        stack_1 = stack_models([huber, lr, lasso], meta_model = ridge)
        final_stack_1 = finalize_model(stack_1)
        save_model(final_stack_1, modelName)


    '''
    @params
    pair: the currency pair for which the return prediction is done
    volatility: volatality for the current pair
    fd: fd for the current pair
    avg_price: mean price for the current pair,
    ret: return for the current pair
    
    @brief description
    This takes the real time data of vol, fd, mean, and predicts return. Before predicting, it converts the fd and vol 
    to their specific classes based on the self.VolBound and self.FDBound dictionaries data.
    Further compares the given return with the predicted return and saves it to the csv file accordingly. 
    
    IMPORTANT NOTE: So, per currency, we have 3 output files.
    '''
    def predictData(self, pair, volatility, fd, avg_price, ret):
        print("Predicting for this data: ", pair, volatility, fd, avg_price, ret)

        modified_volatalities = [0,0,0]
        modified_fds = [0,0,0]
        avgs = [avg_price, avg_price, avg_price]
        returns = [ret, ret, ret]

        temp = ["sortA", "sortB", "sortC"]

        print(self.VolBounds)
        print(self.FDBounds)

        for i in range(len(temp)):
            if self.VolBounds[temp[i] + "_min_0_33_Vol"] < volatility < self.VolBounds[temp[i] + "_max_0_33_Vol"]:
                modified_volatalities[i] = int(self.suffix_info[pair] + "1")
            elif self.VolBounds[temp[i] + "_min_34_67_Vol"] < volatility < self.VolBounds[temp[i] + "_max_34_67_Vol"]:
                modified_volatalities[i] = int(self.suffix_info[pair] + "2")
            elif self.VolBounds[temp[i] + "_min_68_100_Vol"] < volatility < self.VolBounds[temp[i] + "_max_68_100_Vol"]:
                modified_volatalities[i] = int(self.suffix_info[pair] + "3")

            if self.FDBounds[temp[i] + "_min_0_33_FD"] < fd < self.FDBounds[temp[i] + "_max_0_33_FD"]:
                modified_fds[i] = int(self.suffix_info[pair] + "1")
            elif self.FDBounds[temp[i] + "_min_34_67_FD"] < fd < self.FDBounds[temp[i] + "_max_34_67_FD"]:
                modified_fds[i] = int(self.suffix_info[pair] + "2")
            elif self.FDBounds[temp[i] + "_min_68_100_FD"] < fd < self.FDBounds[temp[i] + "_max_68_100_FD"]:
                modified_fds[i] = int(self.suffix_info[pair] + "3")

        print(modified_volatalities, modified_fds)

        model_names = ["sorted_a" + pair, "sorted_b" + pair, "sorted_c" + pair]

        for i in range(len(model_names)):
            curr_data = {'Mean':[avgs[i]], 'Vol': [modified_volatalities[i]], 'FD': [modified_volatalities[i]], 'return': [returns[i]]}
            df = pd.DataFrame(data=curr_data)
            saved_model = load_model(model_names[i])
            pred_unseen = predict_model(saved_model, data = df)
            print("prediction for : ", model_names[i], pred_unseen, df['return'][0])

            currFileName = 'prediction_'+ model_names[i] + ".csv"
            file = open(currFileName, 'a')
            writer = csv.writer(file)

            print(pred_unseen["Label"][0]/100000, pred_unseen["return"][0]/100000)
            writer.writerow([pred_unseen["Label"][0]/100000, pred_unseen["return"][0], abs(pred_unseen["Label"][0]/100000) - abs(pred_unseen["return"][0])])

            # , math.sqrt(abs(pred_unseen["return"][0]) ** 2 - abs(pred_unseen["Label"][0] * 100000) ** 2)
