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

    results = stocks.get_instruments()
    if results == {}:
        print("Collection Failed. Aborting")
        return None
    with open(file, "w") as file:   
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

        print("Beginning data collection...")

        # iterate through all instruments
        queue = []
        for res in results:

            if len(queue) < 25:
                queue.append(res["id"])

            else:
                process_queue(queue)
                queue = []

        if len(queue) > 0: process_queue(queue)

def process_queue(queue):
    """
    Process a queue of stock ids by fetching recent price and popularity data
    :param queue: queue of ids to be processed
    :type queue: list
    """

    pop = stocks.get_popularity_by_ids(queue)
    authentication.login(os.environ.get("ROBIN_USER"), os.environ.get("ROBIN_PASS"))
    price = stocks.get_quotes_by_ids(queue)

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
        authentication.login(os.environ.get("ROBIN_USER"), os.environ.get("ROBIN_PASS"))
        price = stocks.get_quotes_by_ids(queue)

    print(len(pop), len(price))
    for i in range(len(price)):
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite.create_connection("data.sqlite") as conn:
            sqlite.insert(conn, queue[i], timestamp, 
                pop['results'][i]['num_open_positions'], 
                price[i]["last_trade_price"])

######## CSV IMPORT FUNCTIONS ########

def import_csv(directory):
    """
    Import an entire directory of CSV files (requires authentication)
    """

    instruments = []
    with open("instruments.json", "r") as instruments_f:
        instruments = json.load(instruments_f)

    queue = []
    for instrument in instruments:
        if len(queue) < 50:
            filename = instrument["symbol"] + ".csv"
            if filename in os.listdir(directory):
                queue.append(instrument["symbol"])
        else:
            data = read_csv(queue, directory)
            for datum in data:
                with sqlite.create_connection("data.sqlite") as conn:
                    sqlite.insert(conn, datum[0], datum[1], datum[2], datum[3])
            queue = []
    if len(queue) > 0:
        data = read_csv(queue, directory)
        for datum in data:
                with sqlite.create_connection("data.sqlite") as conn:
                    sqlite.insert(conn, datum[0], datum[1], datum[2], datum[3])


def read_csv(symbols, directory):
    """
    Read a batch of CSV files named {symbol}.csv at path (requires authentication)
    """

    final_data = []
    results = stocks.get_historicals(symbols, span="year")
    for result in results:
        res_id = result["InstrumentID"]
        path = directory + "/" + result["symbol"] + ".csv"
        with open(path, "r") as csvfile:
            reader = csv.reader(csvfile)

            for quote in result["historicals"]:
                matches = []
                found = False
                for row in reader:
                    if row[0][:10] in quote["begins_at"]:
                        matches.append(row)
                        found = True
                    elif found == True:
                        break
                if len(matches) > 0:
                    final_data.append((res_id, matches[-1][0], matches[-1][1], float(quote["close_price"])))
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