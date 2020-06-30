import sqlite3, datetime
from robin_stocks import stocks
from sqlite3 import Error

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

    sql_create_table = """ CREATE TABLE IF NOT EXISTS 'stock_info' (
                             id integer NOT NULL,
                             tm datetime NOT NULL,
                             popularity integer NOT NULL,
                             price float NOT NULL
                        ); 
                        """
    with conn:
        try:
            c = conn.cursor()
            c.execute(sql_create_table)
        except Error as e:
            print(e)

def create_index_table(conn):
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

    sql_insert = """ INSERT INTO 'index_data' 
                       (id, tm, popularity, price, weight)
                       VALUES (?, ?, ?, ?, ?); 
                """
    with conn:
        try:
            c = conn.cursor()
            c.execute(sql_insert, (id, timedate, popularity, price, weight))
        except Error as e:
            print(e)

def index_update(conn, id, timedate, weight):
    """
    Update an entry's weight
    """   
    sql_update = "UPDATE index_data SET weight = ? WHERE id = ? AND tm = ?"

    with conn:
        try:
            conn.cursor().execute(sql_update, (weight, id, timedate))
        except Error as e:
            print(e)

def insert_value(conn, timedate, value):

    sql_insert = "INSERT INTO index_value (tm, value) VALUES (?, ?);"

    with conn:
        try:
            conn.cursor().execute(sql_insert, (timedate, value))
        except Error as e:
            print(e)

def insert(conn, id, timedate, popularity, price):
    """
    Insert an SQL entry for a ticker 
    """

    sql_insert = """ INSERT INTO 'stock_info' 
                       (id, tm, popularity, price)
                       VALUES (?, ?, ?, ?); 
                """
    with conn:
        try:
            c = conn.cursor()
            c.execute(sql_insert, (id, timedate, popularity, price))
        except Error as e:
            print(e)

def timestamp(year, month, day, hr=0, min=0, sec=0):
    """ 
    Generate a formatted timestamp 
    """

    return datetime.datetime(year, month, day, hr, min, sec).strftime("%Y-%m-%d %H:%M:%S")
