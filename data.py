# my packages
from robin_stocks import stocks, helper, urls, authentication
import sqlite, index

# system packages
import json, csv, time, sys, os
from datetime import datetime, timedelta

db_file = 'data.sqlite'
value_export_file = os.getenv('HOME') + "/public_html/values.json"

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

            if len(queue) < 50:
                queue.append(res["id"])

            else:
                process_queue(db_file, queue)
                queue = []

        if len(queue) > 0: process_queue(db_file, queue)

def process_queue(file, queue):
    """
    Process a queue of stock ids by fetching recent price and popularity data
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

######## INDEX FUNCTIONS ########

def collect_index(file, 
    t1=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H"), t2=datetime.now().strftime("%Y-%m-%d %H")):

    with sqlite.create_connection(file) as conn:
        data = index.compose_index(conn, index.get_value(conn), t1, t2)
        index.update(conn, data)

def collect_index_value(file, time=datetime.now().strftime("%Y-%m-%d %H")):
    conn = sqlite.create_connection(file)
    with conn:
        value = index.value_index(conn, time)
        print(value)
        sqlite.insert_value(conn, time, value)

def update_weights(file, date=datetime.now()):
    conn = sqlite.create_connection(file)
    comp = index.get_composition(conn, date.strftime("%Y-%m-%d 00"))
    for c in comp:
        c["tm"] = date.strftime("%Y-%m-%d %H")
    index.updates(conn, comp)

def value_to_json(db, file):
    conn = sqlite.create_connection(db)
    data = sqlite.get_all_values(conn)
    with open(file, 'w') as f:
        f.write(json.dumps(data, indent=2))

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
                with sqlite.create_connection(db_file) as conn:
                    sqlite.insert(conn, datum[0], datum[1], datum[2], datum[3])
            queue = []
    if len(queue) > 0:
        data = read_csv(queue, directory)
        for datum in data:
                with sqlite.create_connection(db_file) as conn:
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

def epoch_conversion(db):
    query_value = [
    """ALTER TABLE index_value ADD tim timestamp NOT NULL DEFAULT 0""",
    """UPDATE index_value SET tm = tm || ':00:00'""",
    """UPDATE index_value SET tim = CAST(strftime('%s', tm) as integer)""",
    """ALTER TABLE index_value RENAME TO value_old""",
    """ CREATE TABLE IF NOT EXISTS 'index_value' (
                             tim timestamp NOT NULL,
                             value float NOT NULL
                        ); 
                        """,
    """INSERT INTO index_value (tim, value) SELECT tim, value FROM value_old"""
    ]

    query_data = [
    """ALTER TABLE index_data ADD tim timestamp NOT NULL DEFAULT 0""",
    """UPDATE index_data SET tim = CAST(strftime('%s', tm) as integer)""",
    """ALTER TABLE index_data RENAME TO data_old""",
    """ CREATE TABLE IF NOT EXISTS 'index_data' (
                             id integer NOT NULL,
                             tim timestamp NOT NULL,
                             popularity int NOT NULL,
                             price float NOT NULL,
                             weight float NOT NULL
                        ); 
                        """,
    """INSERT INTO index_data (id, tim, popularity, price, weight) SELECT id, tim, popularity, price, weight FROM data_old"""
    ]

    conn = sqlite.create_connection(db)
    for q in query_value:
        sqlite.execute(conn, q)
    for q in query_data:
        sqlite.execute(conn, q)

######################################

def main():
    
    print("Beginning execution...")
    t1 = datetime.now()

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

        # Collect price and popularity data.
        if '2' in sys.argv[1]:
            print("Composing Index.")
            collect_index(db_file)
            print("Index Composition complete.")
        else:
            print("Updating weights.")
            update_weights(db_file)
            print("Weights updated.")

        print("Valuing Index.")
        collect_index_value(db_file)
        value_to_json(db_file, value_export_file)
        print("Index valuation complete")

    ######## TEST CODE ########

    epoch_conversion('data.sqlite')

    ######## PUT CODE ABOVE ########
    
    dt = datetime.now() - t1
    print("Execution lasted", (datetime.min + dt).strftime("%H hours %M minutes %S seconds."))

if __name__ == '__main__':
    main()
