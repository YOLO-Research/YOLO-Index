# my packages
from robin_stocks import stocks, helper, urls, authentication
import sqlite, index

# system packages
import json, csv, time, sys, os
from datetime import datetime

collect = False

def init(db):
    sqlite.create_table(sqlite.create_connection(db))

def collect_instruments(file):
    """ 
    Collect all valid instruments and create necessary tables 
    Estimated Time (1 minute)

    :param file: json file path
    :type file_path: str
    """

    with open(file, "w") as file:
        
        results = stocks.get_instruments()
        file.write(json.dumps(results, indent=4))

def collect_data(file):
    """ 
    Collects popularity and price data for all instruments in file
    Estimated Time (5 minutes)

    :param file: json file path
    :type file_path: str
    """
    cooldown = 1
    with open(file, "r") as file:
        results = json.load(file)

        with sqlite.create_connection("data.sqlite") as conn:
            print("Beginning data collection...")

            # iterate through all instruments
            queue = []
            for res in results:

                if len(queue) < 25:
                    queue.append(res["id"])

                else:
                    pop = stocks.get_popularity_by_ids(queue)
                    authentication.login("b15c@ymail.com", "Hockeyfreak15")
                    price = stocks.get_quotes_by_ids(queue)

                    # handle API throttling
                    while pop.get("detail") or price[0].get("detail"):

                        if pop.get("detail"):
                            cooldown = helper.parse_throttle_res(pop.get("detail"))
                            print("API Throttling... waiting {} seconds.".format(cooldown))
                            time.sleep(cooldown)

                        elif price.get("detail"):
                            cooldown = helper.parse_throttle_res(price.get("detail"))
                            print("API Throttling... waiting {} seconds.".format(cooldown))
                            time.sleep(cooldown)

                        # recollect data after cooldown
                        pop = stocks.get_popularity_by_ids(queue)
                        price = stocks.get_quotes_by_ids(queue)

                    for i in range(len(queue)):
                        
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        sqlite.insert(conn, queue[i], timestamp, 
                            pop['results'][i]['num_open_positions'], 
                            price[i]["last_trade_price"])
                    queue = []

            authentication.logout()

######## CSV IMPORT FUNCTIONS ########

def import_csv(dir):
    """
    Import an entire directory of CSV files (requires authentication)
    """

    instruments = []
    with open("instruments.json", "r") as instruments_f:
        instruments = json.load(instruments_f)

    for instrument in instruments:
        filename = instrument["symbol"] + ".csv"
        if filename in os.listdir(dir):
            data = read_csv(instrument["symbol"], dir + '/' + filename)
            with sqlite.create_connection("data.sqlite") as conn:
                for d in data:
                    sqlite.insert(conn, instrument["id"], d[0], d[1], d[2])


def read_csv(symbol, path):
    """
    Read a singular CSV file named {symbol}.csv at path (requires authentication)
    """

    with open(path, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        final_data = []
        count = 0
        data = stocks.get_historicals(symbol, span="year")
        for d in data:
            matches = [row for row in reader if row[0][1:] in d["begins_at"]]
            if len(matches) > 0:
                final_data.append((matches[-1][0][1:], matches[-1][1].split(',')[1], float(d["close_price"])))
            csvfile.seek(0)
        return final_data

######################################

def main():
    
    print("Beginning execution...")
    t1 = datetime.now()

    ######## PUT CODE BELOW ########

    if len(sys.argv) > 1:
        # Initialize database and collect instruments.
        if '0' in sys.argv[1]:
            print("Initiating database...")
            init("data.sqlite")
            print("Initialization complete.")

            print("Collecting instruments. Expected time: 1 minute")
            collect_instruments("instruments.json")
            print("Instrument collection complete.")

        # Collect price and popularity data.
        if '1' in sys.argv[1]:
            print("Collecting data. Expected time: 5 minutes")
            collect_data("instruments.json")
            print("Data collection complete.")

    ######## TEST CODE ########


    ######## PUT CODE ABOVE ########
    
    dt = datetime.now() - t1
    print("Execution lasted", (datetime.min + dt).strftime("%H hours %M minutes %S seconds."))

if __name__ == '__main__':
    main()