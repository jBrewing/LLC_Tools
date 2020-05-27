import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np
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


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')

#for bldg in bldgs:
#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.

query = """
SELECT * 
FROM "WaWRE" 
WHERE time >= """+beginDate+""" AND time <= """+endDate+"""
"""

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

df['diff'] = df['hotUse_energy'] + df['pipeLoss_energy']

print('Resampling data...')
B = df[df['buildingID'] == 'B'] # Divide dataframe by building
C = df[df['buildingID'] == 'C']
D = df[df['buildingID'] == 'D']
E = df[df['buildingID'] == 'E']
F = df[df['buildingID'] == 'F']

B = B.resample('1D').sum() # Have to resample to 1D so groupby mean function can work
C = C.resample('1D').sum()
D = D.resample('1D').sum()
E = E.resample('1D').sum()
F = F.resample('1D').sum()

B_avg = B.groupby(B.index.weekday).mean() # Group by timestamp index into weekday
C_avg = C.groupby(C.index.weekday).mean() # sum to get average daily values
D_avg = D.groupby(D.index.weekday).mean()
E_avg = E.groupby(E.index.weekday).mean()
F_avg = F.groupby(F.index.weekday).mean()

B_err = B.groupby(B.index.weekday).std() # Group by timestamp index into day
C_err = C.groupby(C.index.weekday).std() # std to get standard deviation over 4 week periods
D_err = D.groupby(D.index.weekday).std()
E_err = E.groupby(E.index.weekday).std()
F_err = F.groupby(F.index.weekday).std()


time_day = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun']


# plot each bldgs avg daily enery use as a barplot
gridsize = (1,1)
fig = plt.figure(1, figsize=(14,6), tight_layout=True)

x = np.arange(len(time_day))
width = 0.15

# all bldgs
ax1 = plt.subplot2grid(gridsize, (0,0))
p1 = ax1.bar(x=x-2*width, height=B_avg['hotUse_energy'],
            width=width, capsize=5, label = 'Bldg B', color='firebrick',yerr = B_err['hotUse_energy'])
p2 = ax1.bar(x=x-width, height=C_avg['hotUse_energy'],
            width=width, capsize=5, label = 'Bldg C', color='darkorange',yerr = C_err['hotUse_energy'])
p3 = ax1.bar(x=x, height=D_avg['hotUse_energy'],
            width=width, capsize=5, label = 'Bldg D', color='dimgrey',yerr = D_err['hotUse_energy'])
p4 = ax1.bar(x=x+width, height=E_avg['hotUse_energy'],
            width=width, capsize=5, label = 'Bldg E', color='royalblue',yerr = E_err['hotUse_energy'])
p5 = ax1.bar(x=x+2*width, height=F_avg['hotUse_energy'],
            width=width, capsize=5, label = 'Bldg F', color='seagreen',yerr = F_err['hotUse_energy'])
ax1.set_ylabel("Energy Use \n (MJ)",fontsize = 18, weight='bold')
ax1.set_xlabel("Day",fontsize = 18, weight='bold')
ax1.set_ylim(0, 900)
plt.legend(loc=1, ncol=5, fontsize=14)
plt.yticks(fontsize=14)
plt.grid(False, axis = 'x')
plt.xticks(np.arange(7),('Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun'),fontsize=14)
plt.yticks(fontsize=14)
plt.tight_layout()

print('Saving fig...')
plt.savefig('wAWRE_avgDailyEnergyUse.png')

plt.show()





print('done')
