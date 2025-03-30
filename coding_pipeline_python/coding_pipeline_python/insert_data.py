from alphavantage_db import create_connection, close_connection, insert_data, current_time
from extract_data import get_data
import sqlite3
import datetime
import json
from sqlite3 import Error


def main():
    # Fetch the data from the API
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'
    data = get_data(url)

    # Connect to the database
    con = create_connection(database_name='alphavantage.db')

    # Insert the data into the table
    insert_data(con, data_time=current_time, data=json.dumps(data))

    # Close the connection
    close_connection(con)


if __name__ == '__main__':
    main()
