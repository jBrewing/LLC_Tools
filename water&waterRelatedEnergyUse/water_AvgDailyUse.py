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

B_sum = B.groupby(B.index.weekday).mean() # Group by timestamp index into day
C_sum = C.groupby(C.index.weekday).mean()
D_sum = D.groupby(D.index.weekday).mean()
E_sum = E.groupby(E.index.weekday).mean()
F_sum = F.groupby(F.index.weekday).mean()

B_err = B.groupby(B.index.weekday).std() # Group by timestamp index into day
C_err = C.groupby(C.index.weekday).std()
D_err = D.groupby(D.index.weekday).std()
E_err = E.groupby(E.index.weekday).std()
F_err = F.groupby(F.index.weekday).std()

B_tot = B_sum.sum()
C_tot = C_sum.sum()
D_tot = D_sum.sum()
E_tot = E_sum.sum()
F_tot = F_sum.sum()




time_day = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun']


# plot each bldgs daily water use as a barplot
gridsize = (5,1)
fig = plt.figure(1, figsize=(8,10), tight_layout=True)

x = np.arange(len(time_day))
width = 0.2

# building B
ax1 = plt.subplot2grid(gridsize, (0,0))
p1 = ax1.bar(x=x-0.5*width, height=B_sum['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red',yerr = B_err['hotWaterUse'])
p2 = ax1.bar(x=x+0.5*width, height=B_sum['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue',yerr = B_err['coldWaterUse'])
ax1.set_ylabel("Bldg B \n Volume ($m^3$)",fontsize = 14)
#ax1.set_title('Hot and Cold Water Use', fontsize =14)
ax1.set_ylim (0, 6)
#ax1.legend(loc=1, ncol = 2)
plt.xticks([])
plt.yticks(fontsize=12)

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower right',
           ncol=2,  borderaxespad=0.)


# building C
ax2 = plt.subplot2grid(gridsize, (1,0))
p1 = ax2.bar(x=x-0.5*width, height=C_sum['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red',yerr = C_err['hotWaterUse'])
p2 = ax2.bar(x=x+0.5*width, height=C_sum['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue',yerr = C_err['coldWaterUse'])
ax2.set_ylabel("Bldg C \n Volume ($m^3$)",fontsize = 14)
ax2.set_ylim (0,6)
plt.xticks([])
plt.yticks(fontsize=12)

# building D
ax3 = plt.subplot2grid(gridsize, (2,0))
p1 = ax3.bar(x=x-0.5*width, height=D_sum['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red',yerr = D_err['hotWaterUse'])
p2 = ax3.bar(x=x+0.5*width, height=D_sum['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue',yerr = D_err['coldWaterUse'])
ax3.set_ylabel("Bldg D \n Volume ($m^3$)",fontsize = 14)
ax3.set_ylim (0, 6)
plt.xticks([])
plt.yticks(fontsize=12)

# building E
ax4 = plt.subplot2grid(gridsize, (3,0))
p1 = ax4.bar(x=x-0.5*width, height=E_sum['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use',color='red',yerr = E_err['hotWaterUse'])
p2 = ax4.bar(x=x+0.5*width, height=E_sum['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue',yerr = E_err['coldWaterUse'])
ax4.set_ylabel("Bldg E \n Volume ($m^3$)", fontsize = 14)
ax4.set_ylim (0, 6)
plt.xticks([])
plt.yticks(fontsize=12)

# building F
ax5 = plt.subplot2grid(gridsize, (4,0))
p1 = ax5.bar(x=x-0.5*width, height=F_sum['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use',color='red',yerr = F_err['hotWaterUse'])
p2 = ax5.bar(x=x+0.5*width, height=F_sum['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue', yerr=F_err['coldWaterUse'])
ax5.set_xlabel("Day", fontsize = 18, weight ='bold')
ax5.set_ylabel("Bldg F \n Volume ($m^3$)", fontsize = 14)
ax5.set_ylim (0, 6)
plt.grid(False, axis = 'x')
plt.xticks(np.arange(7),('Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun'),fontsize=14)
plt.yticks(fontsize=12)
plt.tight_layout()

print('Saving fig...')
plt.savefig('wAWRE_avgDailyWaterUse.png')



plt.show()







print('done')
