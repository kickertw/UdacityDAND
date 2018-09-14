import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def df2sqlite(dataframe, db_name, tbl_name):
    conn=sqlite3.connect(db_name)
    cur = conn.cursor()                                 
 
    wildcards = ','.join(['?'] * len(dataframe.columns))              
    data = [tuple(x) for x in dataframe.values]
 
    cur.execute("drop table if exists %s" % tbl_name)

    col_str = '"' + '","'.join(dataframe.columns) + '"'
    cur.execute("create table %s (%s)" % (tbl_name, col_str))

    cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), data)

    conn.commit()
    conn.close()

def getLocalTemperatures(p1, db_name):
    conn=sqlite3.connect(db_name)
    cur = conn.cursor()                                 
 
    cur.execute("SELECT year, avg_temp FROM city_data WHERE city = ? AND avg_temp IS NOT NULL", [p1])
    retVal = cur.fetchall()

    conn.commit()
    conn.close()

    return retVal

def getGlobalTemperatures(db_name):
    conn=sqlite3.connect(db_name)
    cur = conn.cursor()                                 
 
    cur.execute("SELECT year, avg_temp FROM global_data WHERE avg_temp IS NOT NULL")
    retVal = cur.fetchall()

    conn.commit()
    conn.close()

    return retVal

# Calculating moving averages of size N
# (Credit to https://stackoverflow.com/questions/13728392/moving-average-or-running-mean)
def calculateMovingAvg(dates, values, N = 100):
    csum, moving_avg_list, dates_list = [0], [], []
    for ii, val in enumerate(values, 1):
        csum.append(csum[ii-1] + val)
        if ii >= N:
            dates_list.append(dates[ii-1])
            moving_avg = (csum[ii] - csum[ii-N])/N
            moving_avg_list.append(moving_avg)

    return dates_list, moving_avg_list

def graph_data(N = 100):
    data = getLocalTemperatures("Raleigh", "proj_1.db")
    gdata = getGlobalTemperatures("proj_1.db")
    dates, gdates = [], []
    values, gvalues = [], []
    
    # putting data in lists
    for row in data:
        dates.append(row[0])
        values.append(row[1])

    for row in gdata:
        gdates.append(row[0])
        gvalues.append(row[1])

    # Calculating moving averages for local data
    localDates, localMovingAvgs = calculateMovingAvg(dates, values, N)
    globalDates, globalMovingAvgs = calculateMovingAvg(gdates, gvalues, N)

    # Plotting the moving avg
    plt.plot(localDates, localMovingAvgs, label="Raleigh, NC")
    plt.plot(globalDates, globalMovingAvgs, label="Global")
    plt.legend()
    plt.ylabel("Temp (C)")
    plt.xlabel("Year")
    plt.title("Average Temperature Trends \n (Raleigh, NC USA vs Global)")
    plt.show()

# Reading CSVs to a dataframe
df = pd.read_csv("./data/city_data.csv")
df2 = pd.read_csv("./data/global_data.csv")

# Loading data to SQLite DB
df2sqlite(df, "proj_1.db", "city_data")
df2sqlite(df2, "proj_1.db", "global_data")

# Plotting data
N = input("Enter the desired size (N) when calculating the moving average - ")
graph_data(int(N))