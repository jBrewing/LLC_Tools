import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/wWRE/')

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-19T12:00:00Z'"

bldgs = ['B','C','D','E','F']



# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')

#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
#bldgID = "'" + bldg + "'"

query = """
SELECT "buildingID", "hotUse_energy"
FROM "WaWRE" 
WHERE time >= """+beginDate+""" AND time <= """+endDate+""""""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query)
df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))

# Set dataframe index as datetime.
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)

print('Data retrieved! \n')

print('Resampling data...')

B = df[df['buildingID'] == 'B'] # Divide dataframe by building
C = df[df['buildingID'] == 'C']
D = df[df['buildingID'] == 'D']
E = df[df['buildingID'] == 'E']
F = df[df['buildingID'] == 'F']

B = B.resample('1H').sum() # resample data to one hour interval
C = C.resample('1H').sum()
D = D.resample('1H').sum()
E = E.resample('1H').sum()
F = F.resample('1H').sum()

B = B.groupby(B.index.hour).mean() # Group by timestamp index into weekday
C = C.groupby(C.index.hour).mean() # to get average hourl values
D = D.groupby(D.index.hour).mean()
E = E.groupby(E.index.hour).mean()
F = F.groupby(F.index.hour).mean()


# create x axis time.  Groupby function stores as [1,2,3,4,..etc]
time_hour = ['1am', '2am', '3am', '4am', '5am', '6am', '7am', '8am', '9am', '10am', '11am', '12pm',
             '1pm', '2pm', '3pm', '4pm', '5pm', '6pm', '7pm', '8pm', '9pm', '10pm', '11pm', '12am']


# print final data
print('Plotting final flowrates...')

fig1, ax1 = plt.subplots(figsize=(14,8))

ax1.plot(time_hour, B['hotUse_energy'], color='red', label = 'Bldg B', linewidth=2)
ax1.plot(time_hour, C['hotUse_energy'], color='blue', label = 'Bldg C', linewidth=2)
ax1.plot(time_hour, D['hotUse_energy'], color='green', label = 'Bldg D', linewidth=2)
ax1.plot(time_hour, E['hotUse_energy'], color='darkorange', label = 'Bldg E', linewidth=2)
ax1.plot(time_hour, F['hotUse_energy'], color='black', label = 'Bldg F', linewidth=2)

ax1.set_ylabel('Water-Related\n  Energy Use (MJ)', fontsize=18)
ax1.set_xlim('1am','12am')
#ax1.set_ylim(0,0.4)
ax1.set_xlim('1am', '12am')
ax1.grid(which='both', axis='x', color='grey', linewidth='1', alpha=0.5)
ax1.set_xlabel('Time', fontsize=18)
plt.tick_params(labelsize='large')
plt.xticks(fontsize=14, rotation=35)
plt.yticks(fontsize=14)
ax1.legend()

fig1.tight_layout()

print('Saving figure...')

plt.savefig('WRE_hourlyAvgUse_ALL.png')

plt.show()






print('done')
