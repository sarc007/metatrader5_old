import csv
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
from _datetime import datetime, timedelta
from pandas.plotting import register_matplotlib_converters
import MetaTrader5 as mt5
import psycopg2
from postgres_ddl import create_part_tbl_spc as cpts
from utils import remove_dash_add_underscore as rdaus
from psycopg2.extensions import register_adapter, AsIs
import numpy
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)
register_matplotlib_converters()

now = datetime.now() - timedelta(days=5)
# connect to MetaTrader 5
if not mt5.initialize():
    print("initialize() failed")
    mt5.shutdown()

# request connection status and parameters
print(mt5.terminal_info())
# get data on MetaTrader 5 version
print(mt5.version())
print('Year, Month, Day, Hour, Minute, WeekDay ', now.year, now.month, now.day, now.hour, now.minute, now.weekday())
# Weekend End Enable / Disable Code
if now.weekday() > 4:
    mt5.shutdown()
    print("Week End No Rates")
    exit(0)
# end

december_month = 12
# request 1000 ticks from EURAUD
# euraud_ticks = mt5.copy_ticks_from("EURAUD", datetime(now.year, now.month, now.day, now.hour), 1000, mt5.COPY_TICKS_ALL)
# request ticks from AUDUSD within 2019.04.01 13:00 - 2019.04.02 13:00
# if now.day > 1:
#     audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year, now.month, now.day-1, now.hour), datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
Currency_List = ["EURUSD", "GBPUSD"]


# ,"AUDUSD","USDJPY","USDCHF", "USDCAD"]

def bulkInsert(records, currency, cnn):
    sql_insert_query = """ INSERT INTO """ + currency + """ (time_fld, bid_fld, ask_fld, last_fld, volume_fld, time_msc_fld, flags_fld, volume_real_fld, year_month_fld) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """

    # executemany() to insert multiple rows rows
    # result = cursor.execute(sql_insert_query, record)
    result = cursor1.executemany(sql_insert_query, records)
    connection.commit()
    print(cursor1.rowcount, "Record inserted successfully into mobile table")


def loop_in_currency(conn):
    for currency in Currency_List:
        if now.weekday() == 0 and now.day == 1 and now.month > 1:
            currency_ticks = mt5.copy_ticks_range(currency, datetime(now.year, now.month - 1, now.day - 3, now.hour),
                                                  datetime(now.year, now.month, now.day, now.hour, now.minute,
                                                           now.second), mt5.COPY_TICKS_ALL)
        elif now.weekday() > 0 and now.day == 1 and now.month > 1:
            currency_ticks = mt5.copy_ticks_range(currency, datetime(now.year, now.month - 1, now.day - 1, now.hour),
                                                  datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
        elif now.weekday() == 0 and now.day == 1 and now.month == 1:
            currency_ticks = mt5.copy_ticks_range(currency,
                                                  datetime(now.year - 1, december_month, now.day - 3, now.hour),
                                                  datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
        elif now.weekday() > 0 and now.day == 1 and now.month == 1:
            currency_ticks = mt5.copy_ticks_range(currency,
                                                  datetime(now.year - 1, december_month, now.day - 1, now.hour),
                                                  datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
        else:
            currency_ticks = mt5.copy_ticks_range(currency, datetime(now.year, now.month, now.day - 1, now.hour),
                                                  datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)

        ticks_frame = pd.DataFrame(currency_ticks)
        ticks_frame['time'] = ticks_frame['time'].values.astype('int64')
        ticks_frame['bid'] = ticks_frame['bid'].values.astype(float)
        ticks_frame['ask'] = ticks_frame['ask'].values.astype(float)
        ticks_frame['last'] = ticks_frame['last'].values.astype(float)
        ticks_frame['volume'] = ticks_frame['volume'].values.astype(float)
        ticks_frame['flags'] = ticks_frame['flags'].values.astype('int64')
        ticks_frame['time_msc'] = ticks_frame['time_msc'].values.astype('int64')
        ticks_frame['volume_real'] = ticks_frame['volume_real'].values.astype(float)
        ticks_frame['year_month'] = str(pd.to_datetime(ticks_frame['time'], unit='s').dt.to_period('M')[0])
        ticks_frame = ticks_frame.drop_duplicates(subset=['time_msc'], keep="last")
        print(ticks_frame.columns.values)
        distinct_date_part_frames = ticks_frame.drop_duplicates(subset=['year_month'])['year_month']
        # print(distinct_date_part_frames.columns.values)
        print(distinct_date_part_frames.values.astype(str))
        for date_part in distinct_date_part_frames.values.astype(str):
            cpts(connection, 'f:\\', currency, rdaus(date_part), 'forex')
        records = ticks_frame.to_records(index=False)

        tuple(map(tuple, records))
        # result = list(records)
        # print(result)
        # file = open('f:\\g4g_'+currency+'.csv', 'w+', newline='')
        #
        # # writing the data into the file
        # with file:
        #     write = csv.writer(file)
        #     write.writerows(result)
        #     # records_to_insert =

        bulkInsert(records=records, currency=currency.lower() + "_tbl", cnn=connection)
            

connection = psycopg2.connect(user="forex",
                              password="Matrix@2020",
                              host="127.0.0.1",
                              port="5432",
                              database="MQLDB")
try:

    cursor1 = connection.cursor()
    connection.autocommit = True

    loop_in_currency(conn=connection)
except (Exception, psycopg2.Error) as error:
    print("Failed inserting record into currency table {}".format(error))

finally:
    # closing database connection.
    if cursor1:
        cursor1.close()
        print("PostgreSQL cursor is closed")

    if connection:
        connection.close()
        print("PostgreSQL connection is closed")




mt5.shutdown()
