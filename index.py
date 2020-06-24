import sqlite

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
            past_item = next(item for item in d1 if item["id"] == d["id"])
            d["change"] = d["popularity"] - past_item["popularity"]
            d["tm1"] = past_item["tm"]
        except StopIteration:
            d["change"] = d["popularity"]
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

def compose_index(conn, date1, date2):
    """
    Composes the index stocks
    Returns a dictionary such that "{id}" : {share_weight}
    :param conn: SQLite Connection
    :type conn: SQLite Connection
    :param date1: Earlier date
    :type date1: String "YYYY-MM-DD"
    :param date2: Later date
    :type date2: String "YYYY-MM-DD"
    """
    # number of total stocks in the index
    num_stocks = 10

    # percentage of index based on movement
    percent_change = 0.8

    # percentage of index based on total popularity
    percent_pop = 1 - percent_change

    num_change = round(percent_change * num_stocks)
    num_pop = num_stocks - num_change

    composition = []
    most_popular = sort_by_popularity(conn, date2)
    largest_change = sort_by_largest_change(conn, date1, date2)

    for stk in largest_change:
        if len(composition) >= num_change:
            break
        if not stk["id"] in composition: 
            stk["weight"] = round(100 / stk["price"], 4)
            # composition[stk["id"]] = stk
            composition.append(stk)
    
    for stk in most_popular:
        if len(composition) >= num_stocks:
            break
        if not stk["id"] in composition:
            stk["weight"] = round(100 / stk["price"], 4)
            # composition[stk["id"]] = stk
            composition.append(stk)

    return composition

