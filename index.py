# my packages
import sqlite

# system packages
import sqlite3, time
from sqlite3 import Error

def collect_index(file, 
    t1=(time.time() - (time.time() % 3600) - 604800), t2=(time.time())):
    """
    Compose the index and set weights in the SQLite DB
    
    :param t1: Earlier Unix Epoch
    :type t1: Unix Epoch
    :param t2: Later Unix Epoch
    :type t2: Unix Epoch
    """
    with sqlite.create_connection(file) as conn:
        value = get_value(conn)
        print(value)
        data = compose_index(conn, value, t1, t2)
        updates(conn, data)

def collect_index_value(file, time=time.time()):
    """
    Value the index given a time
    
    :param file: Database file
    :type file: SQLite Database file
    :param time: Unix Epoch
    :type time: integer
    """
    conn = sqlite.create_connection(file)
    with conn:
        value = value_index(conn, time)
        print(value)
        sqlite.insert_value(conn, time, value)

def update_weights(file, date=time.time()):
    """
    Update the latest stock weights using the day's composition 
    
    :param file: Database file
    :type file: SQLite Database file
    :param time: Unix Epoch
    :type time: integer
    """
    conn = sqlite.create_connection(file)
    comp = get_composition(conn, date - ((date - 14400) % 86400))
    for c in comp:
        c["tim"] = date
    updates(conn, comp)

def sort_by_popularity(conn, date):
    """
    Calculate and sort stock popularity at date. 
    Returns a sorted list of dictionaries containing {id, time, popularity, price}
    
    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param date: Unix Epoch
    :type date: integer
    """
    output = []
    
    tim1 = date - (date % 3600)
    tim2 = tim1 + 3600

    query = "SELECT * FROM index_data WHERE tim BETWEEN {} AND {} ORDER BY popularity desc".format(tim1, tim2)
    rows = sqlite.execute(conn, query)
    
    for row in rows:
        output.append({
            "id": row[0],
            "tim": row[1],
            "popularity": row[2],
            "price": row[3],
            "weight": row[4]
            })
    
    return output

def sort_by_largest_change(conn, date1, date2):
    """
    Calculate and sort the largest changes in popularity between date1 and date2.
    Returns a dictionary such that "{id}" : {popularity}
    
    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param date1: Earlier date
    :type date1: Unix Epoch
    :param date2: Later date
    :type date2: Unix Epoch
    """
    d1 = sort_by_popularity(conn, date1)
    d2 = sort_by_popularity(conn, date2)

    output = d2
    for d in output:
        try:
            items = [item for item in d1 if item["id"] == d["id"]]
            past_item = items[0]
            d["change"] = d["popularity"] - past_item["popularity"]
            d["tim1"] = past_item["tim"]
        except IndexError:
            d["change"] = 0
            d["tim1"] = d["tim"]

    return sorted(output, key=lambda item: item["change"], reverse=True)

def compose_index(conn, value, date1, date2):
    """
    Composes the index stocks
    Returns a dictionary such that "{id}" : {share_weight}
    
    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param value: Total value of index
    :type value: nonnegative float
    :param date1: Earlier date
    :type date1: String "YYYY-MM-DD"
    :param date2: Later date
    :type date2: String "YYYY-MM-DD"
    """
    # number of total stocks in the index
    num_stocks = 50

    # price per stock
    pps = value/num_stocks

    # percentage of index based on movement
    percent_change = 0.8

    # percentage of index based on total popularity
    percent_pop = 1 - percent_change

    num_change = round(percent_change * num_stocks)
    num_pop = num_stocks - num_change

    composition = []
    ids = []
    most_popular = sort_by_popularity(conn, date2)
    largest_change = sort_by_largest_change(conn, date1, date2)
    for stk in largest_change:
        if len(composition) >= num_change:
            break
        if not stk["id"] in ids: 
            stk["weight"] = pps / stk["price"]
            composition.append(stk)
            ids.append(stk["id"])
    for stk in most_popular:
        if len(composition) >= num_stocks:
            break
        if not stk["id"] in ids:
            stk["weight"] = pps / stk["price"]
            composition.append(stk)
            ids.append(stk["id"])
    print(composition)
    return composition

def update(conn, data):
    """
    Update the weights in the index_data table

    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param data: Array of weights to be updated 
    :type data: Array of Dictionaries {id: _, tim: _, weight: _}
    """

    for datum in data:
        sqlite.index_update(conn, datum["id"], datum["tim"], datum["weight"])

def updates(conn, data):
    if len(data) == 0:
        print("No data to update.")
        return

    query = "UPDATE index_data SET weight = CASE "

    tim = 0
    for datum in data:
        tim = datum['tim']

        tim1 = tim - (tim % 3600)
        tim2 = tim1 + 3600

        string = "WHEN (id = '" + datum['id'] + "') THEN "
        string += str(datum['weight']) + " "
        query = ''.join([query, string])
    string = "ELSE weight END WHERE tim BETWEEN {} AND {};".format(tim1, tim2) 
    query = ''.join([query, string])
    
    sqlite.execute(conn, query)

def value_index(conn, date):
    """
    Value the index at a given date, assuring no stock is repeated
    
    :param conn: SQLite database Connection
    :type conn: SQLite Connection
    :param date: Unix Epcoh
    :type date: integer 
    """
    tim1 = date - (date % 3600)
    tim2 = tim1 + 3600

    query = "SELECT * FROM index_data WHERE tim BETWEEN {} AND {} AND weight != 0".format(tim1, tim2)
    data = sqlite.execute(conn, query)
 
    value = 0
    stocks = []
    for datum in data:
        if not datum[0] in stocks:
            stocks.append(datum[0])
            value += datum[3] * datum[4]

    return value

def get_value(conn, date=None):
    """
    Get the value of the index at a given time
    :param conn: SQLite database Connection
    :type conn: SQLite database connection
    :param date: Unix Epoch
    :type date: integer
    """
    i = -1
    query = "SELECT * FROM index_value"

    if date is not None:
        i = 0
        tim1 = date - (date % 3600)
        tim2 = tim1 + 3600
        query = "SELECT * FROM index_value WHERE tim BETWEEN {} AND {}".format(tim1, tim2)
    
    data = sqlite.execute(conn, query)
    
    value = 100
    if len(data) > 0:
        value = data[i][1]
    return value

def get_values(conn, tim1, tim2):
    """
    Get all index values between tim1 and tim2
    :param conn: SQLite database Connection
    :type conn: SQLite database connection
    :param tim1: Earlier Unix Epoch
    :type tim1: integer
    :param tim2: Earlier Unix Epoch
    :type tim2: integer
    """
    query = "SELECT * FROM index_value WHERE tim BETWEEN {} AND {}".format(tim1, tim2)
    
    data = sqlite.execute(conn, query)
    
    return data

def get_composition(conn, date):
    """
    Return the index composition at a time

    :param conn: SQLite database Connection
    :type conn: SQLite database connection
    :param date: Unix Epoch
    :type date: integer
    """
    tim1 = date - (date % 3600)
    tim2 = tim1 + 3600

    query = "SELECT * FROM index_data WHERE tim BETWEEN {} AND {} AND NOT weight = 0".format(tim1, tim2)
    data = sqlite.execute(conn, query)

    results = []
    for row in data:
        results.append({
            "id": row[0],
            "tim": row[1],
            "popularity": row[2],
            "price": row[3],
            "weight": row[4]
            })
    return results
