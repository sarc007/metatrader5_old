from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
from _datetime import datetime,timedelta
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()
import MetaTrader5 as mt5
now = datetime.now() - timedelta(days=3)
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
# if now.weekday() > 4:
#     mt5.shutdown()
#     print("Week End No Rates")
#     exit(0)
# end

december_month = 12
# request 1000 ticks from EURAUD
euraud_ticks = mt5.copy_ticks_from("EURAUD", datetime(now.year, now.month, now.day, now.hour), 1000, mt5.COPY_TICKS_ALL)
# request ticks from AUDUSD within 2019.04.01 13:00 - 2019.04.02 13:00
# if now.day > 1:
#     audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year, now.month, now.day-1, now.hour), datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)

if now.weekday() == 0 and now.day == 1 and now.month > 1:
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year, now.month-1, now.day - 3, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
elif now.weekday() > 0 and now.day == 1 and now.month > 1:
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year, now.month - 1, now.day - 1, now.hour),
                                            datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
elif now.weekday() == 0 and now.day == 1 and now.month == 1:
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year-1, december_month, now.day - 3, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
elif now.weekday() > 0 and now.day == 1 and now.month == 1:
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year-1, december_month, now.day - 1, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)
else:
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(now.year, now.month, now.day - 1, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour), mt5.COPY_TICKS_ALL)

# get bars from different symbols in a number of ways
eurusd_rates = mt5.copy_rates_from("EURUSD", mt5.TIMEFRAME_M1, datetime(now.year, now.month, now.day, now.hour), 1000)
eurgbp_rates = mt5.copy_rates_from_pos("EURGBP", mt5.TIMEFRAME_M1, 0, 1000)

if now.day == 1 and now.month > 1:
    eurcad_rates = mt5.copy_rates_range("EURCAD", mt5.TIMEFRAME_M1, datetime(now.year, now.month-1, now.day - 1, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour))
elif now.day == 1 and now.month == 1:
    eurcad_rates = mt5.copy_rates_range("EURCAD", mt5.TIMEFRAME_M1, datetime(now.year-1, december_month, now.day - 1, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour))
else:
    eurcad_rates = mt5.copy_rates_range("EURCAD", mt5.TIMEFRAME_M1, datetime(now.year, now.month, now.day - 1, now.hour),
                                        datetime(now.year, now.month, now.day, now.hour))

# shut down connection to MetaTrader 5
mt5.shutdown()

# DATA
print('euraud_ticks(', len(euraud_ticks), ')')
for val in euraud_ticks[:10]: print(val)

print('audusd_ticks(', len(audusd_ticks), ')')
for val in audusd_ticks[:10]: print(val)

print('eurusd_rates(', len(eurusd_rates), ')')
for val in eurusd_rates[:10]: print(val)

print('eurgbp_rates(', len(eurgbp_rates), ')')
for val in eurgbp_rates[:10]: print(val)

print('eurcad_rates(', len(eurcad_rates), ')')
for val in eurcad_rates[:10]: print(val)

# PLOT
# create DataFrame out of the obtained data
ticks_frame = pd.DataFrame(audusd_ticks)
# convert time in seconds into the datetime format
ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
# display ticks on the chart
plt.plot(ticks_frame['time'], ticks_frame['ask'], 'r-', label='ask')
plt.plot(ticks_frame['time'], ticks_frame['bid'], 'b-', label='bid')

# display the legends
plt.legend(loc='upper left')

# add the header
plt.title('AUDUSD ticks')

# display the chart
plt.show()
