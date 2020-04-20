# Name:  Daily Water Use Calculator - LLC
# Created by:  Joseph Brewer
# Last Modified: 3.31.20
# Function:  Create simple visualizations of water use from LLC water use data.
#            This script visualizes daily water use for three buildings
#            by querying water use data for three buildings from the
#            InfluxDB database, aggregating them to the day, and plotting them
#            on a bar chart.

# import libraries
import pandas as pd
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient
import numpy as np
import os


print('\nRetreiving data...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')  # Set database.

# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
bldgID = "'D'"
beginDate = "'2019-03-31T00:00:00Z'"
endDate = "'2019-04-07T00:00:00Z'"
query = """SELECT "coldInFlowRate", "hotInFlowRate", "hotOutFlowRate", "buildingID"
FROM "flow" 
WHERE "buildingID" ='E' OR "buildingID" ='B' OR "buildingID" = 'D'
 AND time >= '2019-04-01T00:00:00Z' AND time <= '2019-04-08T00:00:00Z'"""
data = client.query(query)
data = pd.DataFrame(list(data.get_points(measurement='flow'))) # Convert returned ResultSet to Pandas dataframe with list
data['time'] = pd.to_datetime(data['time']) # Set dataframe index as datetime.
data.set_index('time', inplace=True)


print('Subsetting data...')
hot = data[['buildingID', 'hotInFlowRate', 'hotOutFlowRate']] # Create dataframe of just hot flowrates
E = hot[hot['buildingID'] == 'E'] # Divide hot dataframe by building
B = hot[hot['buildingID'] == 'B']
D = hot[hot['buildingID'] == 'D']


print('Resampling data...')
E = E.groupby(E.index.day).sum()/60 # Group by timestamp index into day
B = B.groupby(B.index.day).sum()/60 # Sum values in bins and divide by 60 to convert gpm to gps
D = D.groupby(D.index.day).sum()/60 # 1s of 1 gps = actual flowrate

E['hotWaterUse'] = E['hotInFlowRate'] - E['hotOutFlowRate'] # Calculate hot water use by subtracting agg hotInFlowRate
B['hotWaterUse'] = B['hotInFlowRate'] - B['hotOutFlowRate'] # from agg hotOutFlowRate
D['hotWaterUse'] = D['hotInFlowRate'] - D['hotOutFlowRate']
E.drop(8, axis=0, inplace=True)
B.drop(8, axis=0, inplace=True)
D.drop(8, axis=0, inplace=True)

results = pd.DataFrame() # Create results dataframe to plot
results['E'] = E['hotWaterUse']
results['D'] = D['hotWaterUse']
results['B'] = B['hotWaterUse']


print('Plotting results...')
# Create x-axis ticks to plot
time = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun']

fig, ax = plt.subplots(figsize=(14,6))
x = np.arange(len(time))
width = 0.2

p1 = ax.bar(x=x-width, height=results['B'],
            width=width, capsize=5, label = 'Bldg B')
p2 = ax.bar(x=x, height=results['D'],
            width=width, capsize=5, label = 'Bldg D')
p3 = ax.bar(x=x+width, height=results['E'],
            width=width, capsize=5, label = 'Bldg E')


ax.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
ax.set_xlabel('Day', fontsize=16)
ax.set_ylabel('Water Use (gal)', fontsize=16)
ax.set_xticks(x)
ax.set_xticklabels(time)
ax.legend()
ax.set_title('Daily Hot Water Use', fontsize =18)
ax.tick_params(labelsize='large')



# Save fig
os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dataPresentation/figures/')
plt.savefig('DailyWaterUse.png')


plt.show()

print('done')
