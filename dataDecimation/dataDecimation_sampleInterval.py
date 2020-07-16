import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/dataDecimation/')

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldg = input("Input building ID: ").upper()
bldg='D'
bldgID = "'" + bldg + "'"
#    dates
beginDate = "'2019-03-28T00:00:00Z'"
endDate = "'2019-04-04T00:00:00Z'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final') # Set database.

query = """SELECT "hotUse_energy"
      FROM "WaWRE"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format


print('Data retrieved! \n')

df['time2'] = df['time'].shift(periods=1) # shift copied time column by 1 to do time math
df['delta'] = (df['time'] - df['time2']) / np.timedelta64(1,'s') # calculated time between timestamps and convert to int

df.set_index('time', inplace=True) # Set time as index
df.drop(columns='time2',inplace=True)

df['hotUse_energy_disag'] = df['hotUse_energy']/df['delta'] # divide pulse aggregated data by time period to get
                                                            # avg second value

# resample data
print('Decimating Data...')
df_30s = df.resample('30S').first() # take first obs to simulate a lower sampling frequency
df_1t = df.resample('1T').first()   # repeat for all sample freq: 30sec, 1min, 5min, 30min, 1h
df_5t = df.resample('5T').first()
df_30t = df.resample('30T').first()
df_1h = df.resample('1h').first()

df_30s = df_30s * 30 # multiply first obs by period value represents
df_1t = df_1t * 60 # i.e. a 1 sec obs representing 1 min = obs * 60s
df_5t = df_5t* 300
df_30t = df_30t * 1800
df_1h = df_1h * 3600

# cumulatively sum water related energy use
print('Summing results...\n')
df_sum = df.cumsum() # .cumsum() to cumulatively sum water-related energy use
df_30s = df_30s.cumsum()
df_1t = df_1t.cumsum()
df_5t = df_5t.cumsum()
df_30t = df_30t.cumsum()
df_1h = df_1h.cumsum()

# print final data
print('Plotting data...')
# Full dataset viz
fig,ax = plt.subplots(figsize=(14, 10))

fig.autofmt_xdate()

ax.plot(df_sum['hotUse_energy'], color='red', label='Pulse Aggregated Data')
ax.plot(df_30s['hotUse_energy_disag'], color='blue', label = '30sec Sample Interval')
ax.plot(df_1t['hotUse_energy_disag'], color='green', label = '1min Sample Interval')
ax.plot(df_5t['hotUse_energy_disag'], color='orange', label = '5min Sample Interval')
ax.plot(df_30t['hotUse_energy_disag'], color='black', label = '30min Sample Interval')
ax.plot(df_1h['hotUse_energy_disag'], color='purple', label = '1h Sample Interval')

ax.legend( fontsize=14)
ax.grid(True)

ax.set_xlim(beginDate, endDate)
ax.set_ylim(0,)
ax.set_ylabel('Cumulative Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax.set_xlabel('Date', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.savefig('dataDecimation_sampleInterval-week.png')

plt.show()

print('Saving fig...')



# Window dataset viz
fig2, ax2 = plt.subplots(figsize=(14, 8))
fig2.autofmt_xdate()

ax2.plot(df_sum['hotUse_energy'], color='red', label='Pulse Aggregated Data')
ax2.plot(df_30s['hotUse_energy_disag'], color='blue', label = '30sec Sample Interval')
ax2.plot(df_1t['hotUse_energy_disag'], color='green', label = '1min Sample Interval')
ax2.plot(df_5t['hotUse_energy_disag'], color='orange', label = '5min Sample Interval')
ax2.plot(df_30t['hotUse_energy_disag'], color='black', label = '30min Sample Interval')
ax2.plot(df_1h['hotUse_energy_disag'], color='purple', label = '1h Sample Interval')

ax2.legend( fontsize=14)
ax2.grid(True)
ax2.set_xlim("'2019-04-01T00:00:00Z'", "'2019-04-02T00:00:00Z'")
ax2.set_ylim(2000,3100)
ax2.set_ylabel('Cumulative Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax2.set_xlabel('Time', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%T"))

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.savefig('dataDecimation_sampleInterval-window.png')
plt.show()



print('done')