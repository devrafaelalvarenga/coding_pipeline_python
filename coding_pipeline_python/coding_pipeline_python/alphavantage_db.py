import sqlite3
import datetime

database_name = 'alphavantage.db'
current_time = datetime.datetime.now()


def get_time() -> str:
    """
    Get the current time in the format YYYY-MM-DD HH:MM:SS.
    """
    current_time = datetime.datetime.now()
    current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return current_time


# Create a database connection


def create_database(database_name):
    # Create a database connection
    con = sqlite3.connect(database_name)
    cur = con.cursor()
    return con, cur

# Create a table to store the data


def create_table(con):
    """ create a table to store the data """
    try:
        cur = con.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS raw_time_series_intraday (
                extract_data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT
            )
        ''')
    except sqlite3.Error as e:
        print(e)


def create_connection(database_name):
    """ create a database connection to the SQLite database specified by database_name """
    con = None
    try:
        con = sqlite3.connect(database_name)
        return con
    except sqlite3.Error as e:
        print(e)
    return con


def close_connection(con):
    """ close the database connection """
    if con:
        con.close()


def insert_data(con, data_time, data):
    # Insert the data into the table
    cur = con.cursor()
    cur.execute('''
        INSERT INTO raw_time_series_intraday (extract_data, raw_data)
        VALUES (?, ?)
    ''', (data_time, data))
    # Commit the changes
    con.commit()


def select_data(con):
    # Select the data from the table
    cur = con.cursor()
    cur.execute('''
        SELECT * FROM raw_time_series_intraday
    ''')
    rows = cur.fetchall()
    for row in rows:
        print(row)


def delete_data(con):
    # Delete the data from the table
    cur = con.cursor()
    cur.execute('''
        DELETE FROM raw_time_series_intraday
    ''')
    # Commit the changes and close the connection
    con.commit()
    con.commit()


if __name__ == '__main__':
    pass
