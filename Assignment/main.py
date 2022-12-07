import os
from Auth import Authentication, Trailing_Stop, Trailing_Stop_2
import random
import csv
if __name__ == '__main__':

########### assignment 2:
    # outputFileName1 = 'output.csv'
    # auth.getData(outputFileName1)


########### assignment 3:
    # outputFileName2 = 'longs.csv'
    # outputFileName3 = 'shorts.csv'
    # file1 = open(outputFileName1, 'w')
    # header1 = ['Iteration', 'Key', 'Min', 'Max', 'Mean', "Vol", "FD", "return"]
    # writer1 = csv.DictWriter(file1, fieldnames=header1)
    # writer1.writeheader()
    #
    # file2 = open(outputFileName2, 'w')
    # header2 = ['Interval', 'Key', 'Profit/Loss', 'Status']
    # writer2 = csv.DictWriter(file2, fieldnames=header2)
    # writer2.writeheader()
    #
    # file3 = open(outputFileName3, 'w')
    # header3 = ['Interval', 'Key', 'Profit/Loss', 'Status']
    # writer3 = csv.DictWriter(file3, fieldnames=header3)
    # writer3.writeheader()
    #
    # data = list(range(10))
    # random.shuffle(data)
    # buy_long_indices = data[:5]
    # sell_short_indices = data[5:]
    #
    # auth = Authentication()
    # trailing_stop_obj = Trailing_Stop(sell_short_indices, buy_long_indices)
    # trailing_stop_obj.getData(outputFileName1, outputFileName2, outputFileName3)

###### ***** #########
    #assignment 4:
    #STEP : A GETTING 10 HOURS DATA
    currency_pairs = [["EUR", "USD"],
                      ["GBP", "USD"],
                      ["USD", "CHF"],
                      ["USD", "CAD"],
                      ["USD", "HKD"],
                      ["USD", "AUD"],
                      ["USD", "NZD"],
                      ["USD", "SGD"]]

    # INIT AND WRITE HEADERS FOR ALL THE FILES FOR 10 HOURS DATASET. SAVE THE DATA MANUALLY IN THE FOLDER: 'data_10_hours'
    for curr in currency_pairs:
        file1 = open(curr[0]+curr[1]+".csv", 'w')
        header1 = ['Timestamp', 'Min', 'Max', 'Mean', "Vol", "FD", "return"]
        writer1 = csv.DictWriter(file1, fieldnames=header1)
        writer1.writeheader()

    auth = Trailing_Stop_2()
    auth.getData(predict= False);


    #NOTE: UNCOMMENT THESE AND COMMENT THE ABOVE PART AFTER COLLECTING DATA.

    #STEP 2: SORT AND LABEL THE VOL AND FD DATA. THE DATA IS MANUALLY SAVED IN  "modified" FOLDER.
    auth = Trailing_Stop_2()
    auth.sort_and_classify_data()

    #STEP 3: CREATE THE DATA MODELS USING THE SORTED DATA PRESENT IN "modified" FOLDER. ALSO, SAVE THE DATA MODELS.
    for curr in currency_pairs:
        pair = curr[0] + curr[1]
        fileName_A = "modified"+curr[0] + curr[1] + "/" + "Sort_type_A"+curr[0] + curr[1]+".csv"
        fileName_B = "modified"+curr[0] + curr[1] + "/" + "Sort_type_B"+curr[0] + curr[1]+".csv"
        fileName_C = "modified"+curr[0] + curr[1] + "/" + "Sort_type_C"+curr[0] + curr[1]+".csv"

        files = [fileName_A, fileName_B, fileName_C]

        model_names = ["sorted_a"+pair, "sorted_b"+pair, "sorted_c"+pair]

        for i in range(len(files)):
            auth.trainModels(pair, files[i], model_names[i])

    #STEP 4: PREDICT THE DATA FOR THE REAL TIME DATA AND SAVE THE OUTPUTS.
    auth.getData(predict= True)