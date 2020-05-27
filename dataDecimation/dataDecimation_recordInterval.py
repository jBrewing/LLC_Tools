import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
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
endDate = "'2019-03-29T00:00:00Z'"

# use function to get all inputs
res = ['30S','1T,', '5T', '30T', '1H']


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
df.set_index('time', inplace=True)

print('Data retrieved! \n')


# resample data
print('Decimating Data...')
df_raw = df.copy()
df_30s = df.resample('30S', label='right').sum() # take first obs to simulate a lower sampling frequency
df_1t = df.resample('1T', label='right').sum()   # repeat for all sample freq: 30sec, 1min, 5min, 30min, 1h
df_5t = df.resample('5T', label='right').sum()
df_30t = df.resample('30T', label='right').sum()
df_1h = df.resample('1h', label='right').sum()

#df_30s = df_30s * 30 # multiply first obs by period value represents
#df_1t = df_1t * 60 # i.e. a 1 sec obs representing 1 min = obs * 60s
#df_5t = df_5t* 300
#df_30t = df_30t * 1800
#df_1h = df_1h * 3600

# cumulatively sum water related energy use
print('Summing results...\n')
#df_sum = df.cumsum() # .cumsum() to cumulatively sum water-related energy use
#df_30s = df_30s.cumsum()
##df_1t = df_1t.cumsum()
#df_5t = df_5t.cumsum()
#df_30t = df_30t.cumsum()
#df_1h = df_1h.cumsum()


#  calculate stats for results
dfs = [df, df_30s, df_1t, df_30t,df_1h] # create list of dataframes to iterate through at top level
columns = list()
names = ['pulse', '30s', '1min', '30min', '1h'] # create list of df columns to iterate through for stats
total=[]
for df in dfs:  # loop through query dfs
    total.append(df['hotUse_energy'].sum())

results = pd.DataFrame(list(zip(names, total)))

# print final data
print('Plotting data...')
# Full dataset viz
fig,ax = plt.subplots(figsize=(14, 10))

fig.autofmt_xdate()

ax.plot(df_raw['hotUse_energy'], color='red', label='Pulse Aggregated Data')
ax.plot(df_30s['hotUse_energy'], color='blue', label = '30sec Sample')
ax.plot(df_1t['hotUse_energy'], color='green', label = '1min Sample')
ax.plot(df_5t['hotUse_energy'], color='orange', label = '5min Sample')
ax.plot(df_30t['hotUse_energy'], color='black', label = '30min Sample')
ax.plot(df_1h['hotUse_energy'], color='purple', label = '1h Sample')

ax.legend( fontsize=14)
ax.grid(True)

ax.set_xlim(beginDate, endDate)
ax.set_ylim(0,)
ax.set_ylabel('MJ', fontsize = 18,fontweight='bold')
ax.set_xlabel('Date', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)


plt.savefig('dataDecimation_recordInterval_week.png')

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

print('Saving fig...')



# Window datset viz
fig2, ax2 = plt.subplots(figsize=(14, 8))
fig2.autofmt_xdate()

ax2.plot(df_raw['hotUse_energy'], color='red', label='Pulse Aggregated Data')
ax2.plot(df_30s['hotUse_energy'], color='blue', label = '30sec Sample')
ax2.plot(df_1t['hotUse_energy'], color='green', label = '1min Sample')
ax2.plot(df_5t['hotUse_energy'], color='orange', label = '5min Sample')
ax2.plot(df_30t['hotUse_energy'], color='black', label = '30min Sample')
ax2.plot(df_1h['hotUse_energy'], color='purple', label = '1h Sample')

ax2.legend( fontsize=14)
ax2.grid(True)
ax2.set_xlim("'2019-03-28T04:50:00Z'", "'2019-03-28T06:10:00Z'")
ax2.set_ylim(0,10)
ax2.set_ylabel('MJ', fontsize = 18, fontweight='bold')
ax2.set_xlabel('Date', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.savefig('dataDecimation_recordInterval_window.png')
plt.show()



print('done')