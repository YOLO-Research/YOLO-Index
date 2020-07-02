# my packages
from robin_stocks import stocks, helper, urls, authentication
import sqlite, index

# system packages
import json, csv, time, sys, os
from datetime import datetime, timedelta

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
    with open(file, "r") as f:
        results = json.load(f)

        print("Beginning data collection...")

        # iterate through all instruments
        queue = []
        for res in results:

            if len(queue) < 25:
                queue.append(res["id"])

            else:
                process_queue("data.sqlite", queue)
                queue = []

        if len(queue) > 0: process_queue("data.sqlite", queue)

def process_queue(file, queue):
    """
    Process a queue of stock ids by fetching recent price and popularity data
    :param queue: queue of ids to be processed
    :type queue: list
    """
    print("Processing Queue.")
    pop = stocks.get_popularity_by_ids(queue, errors=False)
    authentication.login(username=os.environ.get("ROBIN_USER"), password=os.environ.get("ROBIN_PASS"))
    price = stocks.get_quotes_by_ids(queue, errors=False)
    with sqlite.create_connection(file) as conn:
        weights = index.get_latest_weights(conn, queue)

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
        with sqlite.create_connection(file) as conn:
            weights = index.get_latest_weights(conn, queue)

    results = {}
    for res in pop['results']:
        ins = helper.id_from_url(res["instrument"])
        if not results.get(ins):
            results[ins] = {}
        results[ins]["pop"] = res['num_open_positions']
    for res in price:
        ins = helper.id_from_url(res["instrument"])
        if not results.get(ins):
            results[ins] = {}
        results[ins]["price"] = res["last_trade_price"]
    for res in weights:
        ins = res["id"]
        if not results.get(ins):
            results[ins] = {}
        results[ins]["weight"] = res["weight"]
    
    for instrument,res in results.items():
        if res.get('price'):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not res.get('weight'): res['weight'] = 0.0
            with sqlite.create_connection(file) as conn:
                sqlite.index_insert(conn, instrument, timestamp, res['pop'], res['price'], res['weight'])
        else:
            print("Failed to fetch price for ", instrument)

######## INDEX FUNCTIONS ########

def collect_index(file, 
    t1=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H"), t2=datetime.now().strftime("%Y-%m-%d %H")):

    with sqlite.create_connection(file) as conn:
        data = index.compose_index(conn, index.value_index(conn, t1), t1, t2)
        index.update(conn, data)
        s = 0
        for d in data:
            s += d["price"] * d["weight"]
        print(s)

def collect_index_value(file, time=datetime.now().strftime("%Y-%m-%d %H:")):
    conn = sqlite.create_connection(file)
    value = index.value_index(conn, time)
    sqlite.insert_value(conn, time, value)

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

        # Collect price and popularity data.
        if '2' in sys.argv[1]:
            print("Composing Index.")
            collect_index("data.sqlite")
            collect_index_value("data.sqlite")
            print("Index Composition Complete.")

    ######## TEST CODE ########

    ######## PUT CODE ABOVE ########
    
    dt = datetime.now() - t1
    print("Execution lasted", (datetime.min + dt).strftime("%H hours %M minutes %S seconds."))

if __name__ == '__main__':
    main()
