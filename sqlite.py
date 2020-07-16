import sqlite3, datetime
from robin_stocks import stocks
from sqlite3 import Error
from datetime import datetime

def execute(conn, query, params=None):
    with conn:
        try:
            if params is None:
                res = conn.cursor().execute(query).fetchall()
            else:
                res = conn.cursor().execute(query, params).fetchall()
        except Error as e:
            print(e)
    return res

def create_connection(db_file):
    """
    Create a database connection to a SQLite database 
    """
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

def create_table(conn):
    """
    Create a database table 
    """

    sql_create_table = """ CREATE TABLE IF NOT EXISTS 'index_data' (
                             id integer NOT NULL,
                             tm datetime NOT NULL,
                             popularity int NOT NULL,
                             price float NOT NULL,
                             weight float NOT NULL
                        ); 
                        """
    sql_create_value_table = """ CREATE TABLE IF NOT EXISTS 'index_value' (
                             tm datetime NOT NULL,
                             value float NOT NULL
                        ); 
                        """

    with conn:
        try:
            c = conn.cursor()
            c.execute(sql_create_table)
            c.execute(sql_create_value_table)
        except Error as e:
            print(e)

def index_insert(conn, id, timedate, popularity, price, weight):
    """
    Insert an SQL entry for an id 
    """

    query = """ INSERT INTO 'index_data' 
                       (id, tm, popularity, price, weight)
                       VALUES (?, ?, ?, ?, ?); 
                """
    
    execute(conn, query, (id, timedate, popularity, price, weight))

def index_insert_many(conn, data):
    query = """ INSERT INTO 'index_data' 
                       (id, tm, popularity, price, weight)
                       VALUES """
    if isinstance(data, dict):
        timestamp = "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        for instrument, result in data.items():
            items = ["'" + instrument + "'", timestamp, str(result["pop"]), str(result["price"]), str(0)]
            query += "(" + ', '.join(items) + "), "

        query = query[:-2] + ";"

    elif isinstance(data, list):
        pass

    execute(conn, query)

def index_update(conn, id, timestamp, weight):
    """
    Update an entry's weight
    """   
    query = "UPDATE index_data SET weight = ? WHERE id = ? AND tm LIKE '{}%'".format(timestamp)

    execute(conn, query, (weight, id))

def insert_value(conn, timestamp, value):

    query = "INSERT INTO index_value (tm, value) VALUES (?, ?);"

    execute(conn, query, (timestamp, value))

def get_all_values(conn):
    query = "SELECT * FROM index_value"

    return execute(conn, query)
