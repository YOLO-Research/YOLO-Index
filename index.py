import sqlite3
from sqlite3 import Error

def sort_by_popularity(conn, date):
    """
    Calculate and sort stock popularity at date. 
    Returns a sorted list of dictionaries containing {id, time, popularity, price}
    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param date: Date to be searched for, approximate
    :type date: String "YYYY-MM-DD H:M:S"
    """
    output = []
    with conn:
        cursor = conn.cursor()
        query = "SELECT * FROM index_data WHERE tm LIKE '{}%' ORDER BY popularity desc".format(date)
        rows = cursor.execute(query).fetchall()
        for row in rows:
            output.append({
                "id": row[0],
                "tm": row[1],
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
    :type date1: String "YYYY-MM-DD"
    :param date2: Later date
    :type date2: String "YYYY-MM-DD"
    """

    d1 = sort_by_popularity(conn, date1)
    d2 = sort_by_popularity(conn, date2)

    output = d2
    for d in output:
        try:
            items = [item for item in d1 if item["id"] == d["id"]]
            past_item = items[0]
            d["change"] = d["popularity"] - past_item["popularity"]
            d["tm1"] = past_item["tm"]
        except IndexError:
            d["change"] = 0
            d["tm1"] = d["tm"]

    # for d in output:
    #     try:
    #         res = next(item for item in d1 if item["id"] == d["id"])
    #         d["change"] = d["popularity"] - res["popularity"]
    #         d["tm2"] = res["tm"]
    #     except StopIteration:
    #         d["change"] = d["popularity"]
    #         d["tm2"] = ""

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
    return composition

def update(conn, data):
    """
    Update the index table
    """

    for datum in data:
        sqlite.index_update(conn, datum["id"], datum["tm"], datum["weight"])

def updates(conn, data):
    query = "UPDATE index_data SET weight = CASE "

    tm = ""
    for datum in data:
        tm = datum['tm']
        string = "WHEN (id = '" + datum['id'] + "' AND tm LIKE '" + datum['tm'] + "%') THEN " 
        string += str(datum['weight']) + " "
        query = ''.join([query, string])
    string = "ELSE weight END WHERE tm LIKE '" + tm + "%';" 
    query = ''.join([query, string])
    
    with conn:
        try:
            c = conn.cursor()
            c.execute(query)
        except Error as e:
            print(e)

def value_index(conn, date):
    """
    Value the index at a given date, assuring no stock is repeated
    :param conn: SQL Connection
    :param date: Date to value index format "YYYY-MM-DD HH"
    """

    query = "SELECT * FROM index_data WHERE tm LIKE '{}%' AND NOT weight=0.0".format(date)
    with conn:
        cursor = conn.cursor()
        data = cursor.execute(query).fetchall()
        value = 0
        stocks = []
        for datum in data:
            if not datum[0] in stocks:
                stocks.append(datum[0])
                value += datum[3] * datum[4]

    return value

def get_value(conn, date=None):
    i = 0
    query = "SELECT * FROM index_value WHERE tm LIKE '{}%'".format(date)
    if date is None:
        query = "SELECT * FROM index_value"
        i = -1
    with conn:
        cursor = conn.cursor()
        data = cursor.execute(query).fetchall()
        value = 100
        if len(data) > 0:
            value = data[i][1]
    return value

def get_composition(conn, date):
    query = "SELECT * FROM index_data WHERE tm LIKE '{}%' AND NOT weight = 0".format(date)
    results = []
    with conn:
        data = conn.cursor().execute(query).fetchall()
        for row in data:
            results.append({
                "id": row[0],
                "tm": row[1],
                "popularity": row[2],
                "price": row[3],
                "weight": row[4]
                })
        return results

def get_latest_composition(conn, date):
    query = "SELECT * FROM index_data WHERE tm LIKE '{}%' AND NOT weight = 0".format(date)
    results = []
    with conn:
        data = conn.cursor().execute(query).fetchall()
        for row in data:
            results.append({
                "id": row[0],
                "tm": row[1],
                "popularity": row[2],
                "price": row[3],
                "weight": row[4]
                })
        return results
