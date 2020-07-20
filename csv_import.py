# my packages
import sqlite
from robin_stocks import stocks

# system packages
import json, csv

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