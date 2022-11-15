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


def create_table(conn, create_table_sql, drop_table_name=None):
    if drop_table_name:  # You can optionally pass drop_table_name to drop the table.
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()

    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def execute_many_sql_statement(sql_statement, conn, values):
    cur = conn.cursor()

    cur.executemany(sql_statement, list(map(tuple, values)))

    rows = cur.fetchall()

    return rows


def insertAccident(conn):
    fields = ['accident_index', 'latitude', 'longitude', 'date', 'time', 'day_of_week', 'number_of_vehicles',
              'number_of_casualties']
    df = pd.read_csv('accident-data.csv', skipinitialspace=True, usecols=fields)

    df['TimeStamp'] = df['date'] + ' ' + df['time']
    for i in range(len(df['TimeStamp'])):
        df['TimeStamp'][i] = datetime.strptime(df['TimeStamp'][i], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M')

    df = df[['accident_index', 'TimeStamp', 'latitude', 'longitude', 'number_of_vehicles', 'number_of_casualties',
             'day_of_week']]

    execute_many_sql_statement("INSERT INTO Accident VALUES(?,?,?,?,?,?,?);", conn, df.to_numpy())

    conn.commit()


def insertAccidentDetails(conn):
    fields = ['accident_index', 'urban_or_rural_area', 'road_type', 'road_surface_conditions', 'light_conditions',
              'weather_conditions', 'accident_severity']
    df = pd.read_csv('accident-data.csv', skipinitialspace=True, usecols=fields)

    df = df[['accident_index', 'urban_or_rural_area', 'road_type', 'road_surface_conditions', 'light_conditions',
             'weather_conditions', 'accident_severity']]

    values = df.to_numpy()
    val_array = []
    for val in values:
        val_array.append((int(val[0]), int(val[1]), int(val[2]), int(val[3]), int(val[4]), int(val[5]), int(val[6])))

    execute_many_sql_statement("INSERT INTO AccidentDetails VALUES(?,?,?,?,?,?,?);", conn, val_array)

    conn.commit()


def insertRoadType(conn):
    execute_sql_statement("insert into RoadType values(1,'Roundabout');", conn)
    execute_sql_statement("insert into RoadType values(2,'One way street');", conn)
    execute_sql_statement("insert into RoadType values(3,'Dual carriageway');", conn)
    execute_sql_statement("insert into RoadType VALUES(6,'Single carriageway');", conn)
    execute_sql_statement("insert into RoadType VALUES(7,'Slip road');", conn)
    execute_sql_statement("insert into RoadType VALUES(9,'Unknown');", conn)
    execute_sql_statement("insert into RoadType VALUES(12,'One way street/Slip road');", conn)
    execute_sql_statement("insert into RoadType VALUES(-1,'Data missing or out of range');", conn)
    conn.commit()


def insertRoadSurfaceConditions(conn):
    execute_sql_statement("insert into RoadSurfaceConditions values(1, 'Dry');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(2, 'Wet or Damp');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(3, 'Snow');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(4, 'Frost or Ice');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(5, 'Flood over 3cm. deep');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(6, 'Oil or Diesel');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(7, 'Mud');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(-1, 'Data missing or out of range');", conn)
    execute_sql_statement("insert into RoadSurfaceConditions values(9, 'unknown');", conn)
    conn.commit()


def insertLightConditions(conn):
    execute_sql_statement("insert into LightConditions values(1, 'Daylight');", conn)
    execute_sql_statement("insert into LightConditions values(4, 'Darkness - lights lit');", conn)
    execute_sql_statement("insert into LightConditions values(5, 'Darkness - lights unlit');", conn)
    execute_sql_statement("insert into LightConditions values(6, 'Darkness - no lighting');", conn)
    execute_sql_statement("insert into LightConditions values(7, 'Darkness - lighting unknown');", conn)
    execute_sql_statement("insert into LightConditions values(-1, 'Data missing or out of range');", conn)
    conn.commit()


def insertWeatherConditions(conn):
    execute_sql_statement("insert into  WeatherConditions values(1, 'Fine no high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(2, 'Raining no high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(3, 'Snowing no high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(4, 'Fine + high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(5, 'Raining + high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(6, 'Snowing + high winds');", conn)
    execute_sql_statement("insert into  WeatherConditions values(7, 'Fog or mist');", conn)
    execute_sql_statement("insert into  WeatherConditions values(8, 'Other');", conn)
    execute_sql_statement("insert into  WeatherConditions values(9, 'Unknown');", conn)
    execute_sql_statement("insert into  WeatherConditions values(-1, 'Data missing or out of range');", conn)
    conn.commit()


def insertSeverity(conn):
    execute_sql_statement("insert into Severity values(1, 'Fatal');", conn)
    execute_sql_statement("insert into Severity values(2, 'Serious');", conn)
    execute_sql_statement("insert into Severity values(3, 'Slight');", conn)
    conn.commit()

def populateTables(conn):
    create_table(conn,
                 "create table Accident(AccidentID Integer PRIMARY KEY AUTOINCREMENT, TimeStamp DATETIME, Lattitude varchar(20), Longitude varchar(20), NoOfVehicles Integer, NoOfCasualities Integer, DayOfWeek Integer);    ",
                 "Accident")
    create_table(conn,
                 "create table RoadType(RoadTypeID Integer NOT NULL PRIMARY KEY, RoadTypeDesc varchar(50) NOT NULL);",
                 "RoadType")
    create_table(conn,
                 "create table RoadSurfaceConditions(RoadSurfaceID Integer NOT NULL PRIMARY KEY, RoadSurfaceDesc varchar(50) NOT NULL);",
                 "RoadSurfaceConditions")
    create_table(conn,
                 "create table LightConditions(LightConditionID Integer NOT NULL PRIMARY KEY, LightConditionDesc varchar(50) NOT NULL);",
                 "LightConditions")
    create_table(conn,
                 "create table WeatherConditions(WeatherConditionID Integer NOT NULL PRIMARY KEY, WeatherConditionDesc varchar(50) NOT NULL);",
                 "WeatherConditions")
    create_table(conn,
                 "create table Severity(SeverityID Integer NOT NULL PRIMARY KEY, SeverityDesc varchar(50) NOT NULL);",
                 "Severity")
    create_table(conn,
                 "create table AccidentDetails(AccidentID Integer NOT NULL PRIMARY KEY, UrbanOrRural Integer NOT NULL, RoadTypeID Integer NOT NULL, RoadSurfaceID Integer NOT NULL, LightConditionID Integer NOT NULL, WeatherConditionID Integer NOT NULL, SeverityID Integer NOT NULL,FOREIGN KEY(AccidentID) REFERENCES Accident(AccidentID), FOREIGN KEY(RoadTypeID) REFERENCES RoadType(RoadTypeID), FOREIGN KEY(RoadSurfaceID) REFERENCES RoadSurfaceConditions(RoadSurfaceID),FOREIGN KEY(LightConditionID) REFERENCES LightConditions(LightConditionID), FOREIGN KEY(WeatherConditionID) REFERENCES WeatherConditions(WeatherConditionID), FOREIGN KEY(SeverityID) REFERENCES Severity(SeverityID));",
                 "AccidentDetails")


conn = create_connection('RoadSafety.db')
populateTables(conn)
insertAccident(conn)
insertRoadType(conn)
insertRoadSurfaceConditions(conn)
insertLightConditions(conn)
insertWeatherConditions(conn)
insertSeverity(conn)
insertAccidentDetails(conn)
