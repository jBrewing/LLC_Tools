import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from tempQC.calibrationFunctions import calibration_temp
import numpy as np
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/dataDecimation/')

def adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, threshold):
    filteredSignal = []
    window = []
    windowSize = minWindowSize
    headLength = 0
    for j in range(0, windowSize):
        window.append(signal[j])
    for i in range(0, len(signal)):
        delta = signal[i] - signal[i - 1]
        if np.absolute(delta) > threshold:
            windowSize = minWindowSize
            window.clear()
            if windowSize % 2 == 0:
                headLength = windowSize // 2
            else:
                headLength = (windowSize - 1) // 2
            if (i >= headLength) and (i < (len(signal) - headLength)):
                for j in range(0, windowSize):
                    window.append(signal[i - (headLength - j)])
            elif (i < headLength):
                for j in range(0, windowSize):
                    window.append(signal[i + j])
            else:
                for j in range(0, windowSize):
                    window.append(signal[len(signal) - j])
        else:
            if (windowSize + 2) <= maxWindowSize:
                windowSize += 2
                window.append(0)
                window.append(0)

            if windowSize % 2 == 0:
                headLength = windowSize // 2
            else:
                headLength = (windowSize - 1) // 2
            if (i >= headLength) and (i < (len(signal) - headLength)):
                for j in range(0, windowSize):
                    window[j] = signal[i - (headLength - j)]
            elif (i < headLength):
                for j in range(0, windowSize):
                    window[j] = signal[i + j]
            else:
                for j in range(0, windowSize):
                    window[j] = signal[len(signal) - j - 1]
        median = np.median(window)
        filteredSignal.append(median)
        percent = i / len(signal) * 100
        print(" %2.2f %%, window size = %3d" % (percent, windowSize), end="\r", flush=True)
    print("100.00 %%, window size = %3d" % windowSize)
    return filteredSignal



# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldg = input("Input building ID: ").upper()
bldg='D'
bldgID = "'" + bldg + "'"
#    dates
beginDate = "'2019-03-24T00:00:00Z'"
endDate = "'2019-03-31T00:00:00Z'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final') # Set database.

query = """SELECT *
      FROM "DD"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='DD')))
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True) # Set time as index
print('Data retrieved! \n')

df.drop(columns='buildingID', inplace=True)


query = """SELECT *
      FROM "LLC"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df_raw = pd.DataFrame(list(results.get_points(measurement='LLC')))
df_raw['time'] = pd.to_datetime(df_raw['time']) # Convert time to pandas datetime format
df_raw.set_index('time', inplace=True) # Set time as index
print('Data retrieved! \n')



# include rules for each column as dict to pass to resample
agg = {'coldInFlowRate':'sum',
          'hotInFlowRate':'sum',# take first obs to simulate a lower sampling frequency
          'hotOutFlowRate':'sum', # take sum of obs to simulate looking at a physical meter
          'hotInTemp': 'mean',
          'hotOutTemp': 'mean',
          'coldInTemp' : 'mean'}


# resample data
df_30s = df.resample("30s", label='right').agg(agg)
df_1t = df.resample("1T", label='right').agg(agg)
df_5t = df.resample("5T", label='right').agg(agg)
df_30t = df.resample("30T", label='right').agg(agg)
df_1h = df.resample("1h", label='right').agg(agg)


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

df_raw['hotWaterUse'] = df_raw['hotWaterUse'] * 0.00378541

#df_raw.loc[df_raw.hotWaterUse < 0, 'hotWaterUse'] = 0
#df_raw.loc[df_raw.hotWaterUse < 0.009, 'hotWaterUse'] = 0
df_30s.loc[df_30s.hotWaterUse < 0, 'hotWaterUse'] = 0
df_1t.loc[df_1t.hotWaterUse < 0, 'hotWaterUse'] = 0
df_5t.loc[df_5t.hotWaterUse < 0, 'hotWaterUse'] = 0
df_30t.loc[df_30t.hotWaterUse < 0, 'hotWaterUse'] = 0
df_1h.loc[df_1h.hotWaterUse < 0, 'hotWaterUse'] = 0

x = df_30s.loc['2019-03-24T00:52:30']


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

# print final data
print('Plotting data...')
# Full dataset viz
fig,ax = plt.subplots(figsize=(14, 10))
fig.suptitle('RECORD INTERVAL', fontsize = 18)

fig.autofmt_xdate()

ax.plot(df_raw_sum['hotUse_energy'], color='red', label='Pulse Aggregated Data')
ax.plot(df_30s_sum['hotUse_energy'], color='blue', label = '30sec Record Interval')
ax.plot(df_1t_sum['hotUse_energy'], color='green', label = '1min Record Interval')
ax.plot(df_5t_sum['hotUse_energy'], color='orange', label = '5min Record Interval')
ax.plot(df_30t_sum['hotUse_energy'], color='black', label = '30min Record Interval')
ax.plot(df_1h_sum['hotUse_energy'], color='purple', label = '1h Record Interval')

ax.legend(fontsize=14)
ax.grid(True)
ax.set_ylim(0, )
ax.set_xlim(beginDate, endDate)
ax.set_ylim(0,)
ax.set_ylabel('Cumulative Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax.set_xlabel('Date', fontsize=18,fontweight='bold')
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.savefig('dataDecimation_recordInterval-week.png')

plt.show()

print('Saving fig...')



fig2,ax2 = plt.subplots(figsize=(14, 10))

fig.autofmt_xdate()

ax2.plot(df_raw['hotUse_energy'], '-o', color='red', label='Pulse Aggregated Timestep')
ax2.plot(df_30s['hotUse_energy'], '-o',color='blue', label = '30sec Recording Interval')
ax2.plot(df_1t['hotUse_energy'], '-o',color='green', label = '1min Recording Interval')
ax2.plot(df_5t['hotUse_energy'], '-o',color='orange', label = '5min Recording Interval')

ax2.legend( fontsize=14)
ax2.grid(True)


ax2.set_ylim(0,3.5)
ax2.set_xlim("'2019-03-28T05:00:00Z'", "'2019-03-28T05:30:00Z'")
ax2.set_ylabel('Water-Related\nEnergy Use (MJ)', fontsize = 18,fontweight='bold')
ax2.set_xlabel('Time', fontsize=18,fontweight='bold')
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%T"))
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation =25)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.savefig('dataDecimation_recordInterval_window.png')


plt.show()



print('don')