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
endDate = "'2019-03-29T00:00:00Z'"

# use function to get all inputs
res = ['30S','1T,', '5T', '30T', '1H']


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final') # Set database.

query = """SELECT *
      FROM "LLC"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='LLC')))
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True)

print('Data retrieved! \n')
# Convert gallons to m3
df[['hotInFlowRate','hotOutFlowRate']] = df[['hotInFlowRate','hotOutFlowRate']] *  0.00378541


# include rules for each column as dict to pass to resample
agg = {'coldInFlowRate':'sum',
          'hotInFlowRate':'sum',# take first obs to simulate a lower sampling frequency
          'hotOutFlowRate':'sum', # take sum of obs to simulate looking at a physical meter
          'hotInTemp': 'mean',
          'hotOutTemp': 'mean',
          'coldInTemp' : 'mean'}

# resample data
print('Decimating Data...')
df_raw = df.copy()
# resample data
df_30s = df.resample("30s").agg(agg)
df_1t = df.resample("1T").agg(agg)
df_5t = df.resample("5T").agg(agg)
df_30t = df.resample("30T").agg(agg)
df_1h = df.resample("1h").agg(agg)

#perform Water/Water-Energy balance eq
print('Calculating water use and water-related energy use...')
# assign constants
rho = 997 # (kg/m^3) # density of water
c = 4.186 # (J/g*C) # specific heat of water

# calculate hot water use
df_raw['hotWaterUse'] = df_raw['hotInFlowRate'] - df_raw['hotOutFlowRate']# hot water use = hotIn - hotOut
df_30s['hotWaterUse'] = df_30s['hotInFlowRate'] - df_30s['hotOutFlowRate']
df_1t['hotWaterUse'] = df_1t['hotInFlowRate'] - df_1t['hotOutFlowRate']
df_5t['hotWaterUse'] = df_5t['hotInFlowRate'] - df_5t['hotOutFlowRate']
df_30t['hotWaterUse'] = df_30t['hotInFlowRate'] - df_30t['hotOutFlowRate']
df_1h['hotWaterUse'] = df_1h['hotInFlowRate'] - df_1h['hotOutFlowRate']

# HotUse ENERGY  rho * heatCap * HotUseVol * (HotSupplyTemp - ColdSupplyTemp)
df_raw['hotUse_energy'] = (rho*c*1000*df_raw['hotWaterUse']*(df_raw['hotOutTemp'] - df_raw['coldInTemp']))/1000000
df_30s['hotUse_energy'] = (rho*c*1000*df_30s['hotWaterUse']*(df_30s['hotOutTemp'] - df_30s['coldInTemp']))/1000000
df_1t['hotUse_energy'] = (rho*c*1000*df_1t['hotWaterUse']*(df_1t['hotOutTemp'] - df_1t['coldInTemp']))/1000000
df_5t['hotUse_energy'] = (rho*c*1000*df_5t['hotWaterUse']*(df_5t['hotOutTemp'] - df_5t['coldInTemp']))/1000000
df_30t['hotUse_energy'] = (rho*c*1000*df_30t['hotWaterUse']*(df_30t['hotOutTemp'] - df_30t['coldInTemp']))/1000000
df_1h['hotUse_energy'] = (rho*c*1000*df_1h['hotWaterUse']*(df_1h['hotOutTemp'] - df_1h['coldInTemp']))/1000000


# cumulatively sum water related energy use
print('Summing results...\n')
df_raw_sum = df_raw.cumsum() # .cumsum() to cumulatively sum water-related energy use
df_30s_sum = df_30s.cumsum()
df_1t_sum = df_1t.cumsum()
df_5t_sum = df_5t.cumsum()
df_30t_sum = df_30t.cumsum()
df_1h_sum = df_1h.cumsum()

"""
#  calculate stats for results
dfs = [df, df_30s, df_1t, df_30t,df_1h] # create list of dataframes to iterate through at top level
columns = list()
names = ['pulse', '30s', '1min', '30min', '1h'] # create list of df columns to iterate through for stats
total=[]
for df in dfs:  # loop through query dfs
    total.append(df['hotUse_energy'].sum())

results = pd.DataFrame(list(zip(names, total)))
"""
# print final data
print('Plotting data...')
# Full dataset viz
# print final data
print('Plotting data...')
# Initialize figures and subplots
gridsize = (2, 1)
fig = plt.figure(1, figsize=(14, 10))
fig.autofmt_xdate()

# 1st row - hot use
ax = plt.subplot2grid(gridsize, (0, 0))

ax.plot(df_raw['hotUse_energy'], color='red', label='Pulse Aggregated Timestep')
#ax.plot(df_30s['hotUse_energy'], color='blue', label = '30sec Recording Interval')
ax.plot(df_1t['hotUse_energy'], color='green', label = '1min Recording Interval')
ax.plot(df_5t['hotUse_energy'], color='orange', label = '5min Recording Interval')
ax.plot(df_30t['hotUse_energy'], color='black', label = '30min Recording Interval')
ax.plot(df_1h['hotUse_energy'], color='purple', label = '1h Recording Interval')

ax.legend( fontsize=13)
ax.grid(True)

#ax.set_xlim(beginDate, endDate)
ax.set_ylim(0,)
ax.set_ylabel('Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
#ax.set_xlabel('Time', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)
ax.tick_params(labelbottom=False)


# 2nd row - cold in
ax2 = plt.subplot2grid(gridsize, (1, 0))

ax2.plot(df_raw_sum['hotUse_energy'], color='red', label='Pulse Aggregated Data')
#ax2.plot(df_30s_sum['hotUse_energy'], color='blue', label = '30sec Recording Interval')
ax2.plot(df_1t_sum['hotUse_energy'], color='green', label = '1min Recording Interval')
ax2.plot(df_5t_sum['hotUse_energy'], color='orange', label = '5min Recording Interval')
ax2.plot(df_30t_sum['hotUse_energy'], color='black', label = '30min Recording Interval')
ax2.plot(df_1h_sum['hotUse_energy'], color='purple', label = '1h Recording Interval')

ax2.legend( fontsize=13, loc =4)
ax2.grid(True)
#ax2.set_xlim("'2019-03-28T04:50:00Z'", "'2019-03-28T06:10:00Z'")
#ax2.set_ylim(0,10)
ax2.set_ylabel('Cumulative Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax2.set_xlabel('Time', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%T"))

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.savefig('dataDecimation_recordInterval.png')
plt.show()



fig,ax = plt.subplots(figsize=(14, 10))

fig.autofmt_xdate()

ax.plot(df_raw['hotUse_energy'], '-o', color='red', label='Pulse Aggregated Timestep')
#ax.plot(df_30s['hotUse_energy'], '-o',color='blue', label = '30sec Recording Interval')
ax.plot(df_1t['hotUse_energy'], '-o',color='green', label = '1min Recording Interval')
ax.plot(df_5t['hotUse_energy'], '-o',color='orange', label = '5min Recording Interval')

ax.legend( fontsize=14)
ax.grid(True)


ax.set_ylim(0,3.5)
ax.set_xlim("'2019-03-28T05:00:00Z'", "'2019-03-28T05:30:00Z'")
ax.set_ylabel('Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax.set_xlabel('Time', fontsize=18,fontweight='bold')
ax.xaxis.set_major_formatter(mdates.DateFormatter("%T"))
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)


plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.savefig('dataDecimation_recordInterval_window.png')


plt.show()

print('Saving fig...')





print('done')