import pandas as pd
from datetime import datetime
import sqlite3
from sqlite3 import Error


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def execute_many_sql_statement(sql_statement, conn, values):
    cur = conn.cursor()

    cur.executemany(sql_statement, values)

    rows = cur.fetchall()

    return rows


fields = ['accident_index', 'urban_or_rural_area','road_type', 'road_surface_conditions', 'light_conditions', 'weather_conditions','accident_severity']
df = pd.read_csv('accident-data.csv', skipinitialspace=True, usecols=fields)

#df['TimeStamp'] = df['date'] + ' ' + df['time']

#for i in range(len(df['TimeStamp'])):
 #   df['TimeStamp'][i] = datetime.strptime(df['TimeStamp'][i], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M')

df = df[['accident_index','urban_or_rural_area','road_type', 'road_surface_conditions', 'light_conditions', 'weather_conditions','accident_severity']]

conn = create_connection('Road_safety.db')

execute_many_sql_statement("INSERT INTO Accident VALUES(?,?,?,?,?,?,?);", conn, df.to_numpy())

conn.commit()