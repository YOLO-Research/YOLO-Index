import sqlite3, time
from robin_stocks import stocks
from sqlite3 import Error

def execute(conn, query, params=None):
    """
    Execute an SQLite Query
    """
    res = None
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
    sql_create_table = """ CREATE TABLE IF NOT EXISTS 'stock_data' (
                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                             uid VARCHAR(255) NOT NULL,
                             tim TIMESTAMP NOT NULL,
                             popularity INTEGER NOT NULL,
                             price FLOAT NOT NULL,
                             weight FLOAT DEFAULT NULL,
                             comp_id INTEGER DEFAUL NULL
                        ); 
                        """
    sql_create_comp_table = """ CREATE TABLE IF NOT EXISTS 'compositions' (
                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                             tim TIMESTAMP NOT NULL,
                             value FLOAT NOT NULL
                        ); 
                        """

    with conn:
        try:
            c = conn.cursor()
            c.execute(sql_create_table)
            c.execute(sql_create_comp_table)
        except Error as e:
            print(e)

def index_insert(conn, id, timestamp, popularity, price, weight):
    """
    Insert an SQL entry for an id 
    """

    query = """ INSERT INTO 'index_data' 
                       (id, tim, popularity, price, weight)
                       VALUES (?, ?, ?, ?, ?); 
                """
    
    execute(conn, query, (id, timestamp, popularity, price, weight))

def index_insert_many(conn, data):
    query = """ INSERT INTO 'index_data' 
                       (id, tim, popularity, price, weight)
                       VALUES """
    if isinstance(data, dict):
        timestamp = round(time.time())
        for instrument, result in data.items():
            items = ["'" + instrument + "'", str(timestamp), str(result["pop"]), str(result["price"]), str(0)]
            query += "(" + ', '.join(items) + "), "

        query = query[:-2] + ";"

    elif isinstance(data, list):
        pass

    execute(conn, query)

def index_update(conn, id, timestamp, weight):
    """
    Update an entry's weight
    """   

    tim1 = timestamp - (timestamp % 3600)
    tim2 = tim1 + 3600

    query = "UPDATE index_data SET weight = ? WHERE id = ? AND tim BETWEEN {} AND {}".format(tim1, tim2)

    execute(conn, query, (weight, id))

def insert_value(conn, timestamp, value):

    query = "INSERT INTO index_value (tim, value) VALUES (?, ?);"

    execute(conn, query, (int(timestamp), value))

def get_all_values(conn):
    query = "SELECT * FROM index_value"

    return execute(conn, query)
