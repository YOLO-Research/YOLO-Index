# my packages
from robin_stocks import stocks, helper, authentication
import sqlite, index

# system packages
import json, time, sys, os, time

db_file = 'data.sqlite'

def init(db):
    sqlite.create_table(sqlite.create_connection(db))

def collect_instruments(file):
    """ 
    Collect all valid instruments and create necessary tables 
    Estimated Time (1 minute)

    :param file: json file path
    :type file_path: string
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
    :type file: string
    """
    cooldown = 1
    with open(file, "r") as f:
        results = json.load(f)

        print("Beginning data collection...")

        # iterate through all instruments
        queue = []
        for res in results:

            if len(queue) < 50:
                queue.append(res["id"])

            else:
                process_queue(db_file, queue)
                queue = []

        if len(queue) > 0: process_queue(db_file, queue)

def process_queue(file, queue):
    """
    Process a queue of stock ids by fetching recent price and popularity data
    
    :param file: SQLite database file path
    :type file: string
    :param queue: queue of ids to be processed
    :type queue: list
    """
    pop = stocks.get_popularity_by_ids(queue, errors=False)
    authentication.login(username=os.environ.get("ROBIN_USER"), password=os.environ.get("ROBIN_PASS"))
    price = stocks.get_quotes_by_ids(queue, errors=False)

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
        pop = stocks.get_popularity_by_ids(queue, errors=False)
        authentication.login(username=os.environ.get("ROBIN_USER"), password=os.environ.get("ROBIN_PASS"))
        price = stocks.get_quotes_by_ids(queue, errors=False)

    results = {}
    for res in pop['results']:
        if res is not None:
            ins = helper.id_from_url(res["instrument"])
            if not results.get(ins):
                results[ins] = {}
            results[ins]["pop"] = res['num_open_positions']
    for res in price:
        if res is not None:
            ins = helper.id_from_url(res["instrument"])
            if not results.get(ins):
                results[ins] = {}
            results[ins]["price"] = res["last_trade_price"]
    
    bad_keys = []
    for instrument, res in results.items():
        if not res.get("price"): 
            print("Failed to fetch price for ", instrument)
            bad_keys.append(instrument)
    for k in bad_keys: del results[k]

    with sqlite.create_connection(file) as conn:
        sqlite.index_insert_many(conn, results)

######################################

def main():
    
    print("Beginning execution...")
    t1 = time.time()

    ######## PUT CODE BELOW ########

    if len(sys.argv) > 1:
        # Initialize database and collect instruments.
        if '0' in sys.argv[1]:
            print("Initiating database...")
            init(db_file)
            print("Initialization complete.")

            print("Collecting instruments. Expected time: 1 minute")
            collect_instruments("instruments.json")
            print("Instrument collection complete.")

        # Collect price and popularity data.
        if '1' in sys.argv[1]:
            print("Collecting data. Expected time: 5 minutes")
            collect_data("instruments.json")
            print("Data collection complete.")

        # Compose the index or update weights
        if '2' in sys.argv[1]:
            print("Composing Index.")
            index.collect_index(db_file)
            print("Index Composition complete.")
        else:
            print("Updating weights.")
            index.update_weights(db_file)
            print("Weights updated.")

        # Collect the value of the index        
        print("Valuing Index.")
        index.collect_index_value(db_file)
        print("Index valuation complete")

    ######## TEST CODE ########

    ######## PUT CODE ABOVE ########
    
    dt = round(time.time() - t1)
    m, s = divmod(dt, 60)
    h, m = divmod(m, 60)
    print("Execution lasted", h, "hours", m, "minutes", s, "seconds.")

if __name__ == '__main__':
    main()
