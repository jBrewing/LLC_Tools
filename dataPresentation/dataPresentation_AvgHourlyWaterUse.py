# Name:  Average Hourly Water Use Calculator - LLC
# Created by:  Joseph Brewer
# Last Modified: 3.31.20
# Function:  Create simple visualizations of water use from LLC water use data.
#            This script visualizes hourly water use for three buildings
#            by querying water use data for three buildings from the
#            InfluxDB database, aggregating them to the hour, and plotting them
#            on a line chart.


# import libraries
import pandas as pd
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient
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
 AND time >= '2019-03-31T00:00:00Z' AND time <= '2019-04-14T00:00:00Z'"""
data = client.query(query)
data = pd.DataFrame(list(data.get_points(measurement='flow'))) # Convert returned ResultSet to Pandas dataframe with list
data['time'] = pd.to_datetime(data['time']) # Set dataframe index as datetime.
data.set_index('time', inplace=True)


print('Subsetting data...')
cold = data[['buildingID', 'coldInFlowRate']] # Create dataframe of just cold data
E = cold[cold['buildingID'] == 'E'] # Divide cold dataframe by building
B = cold[cold['buildingID'] == 'B']
D = cold[cold['buildingID'] == 'D']


print('Resampling data...')
E = E.groupby(E.index.hour).sum()/60 # Group by timestamp index into day
B = B.groupby(B.index.hour).sum()/60 # Sum values in bins and divide by 60 to convert gpm to gps
D = D.groupby(D.index.hour).sum()/60 # 1s of 1 gps = actual flowrate


print('Plotting results...')
# Create x-axis ticks to plot
time = ['1am','2am', '3am', '4am', '5am', '6am', '7am', '8am', '9am', '10am', '11am', '12pm',
        '1pm', '2pm', '3pm', '4pm', '5pm', '6pm', '7pm', '8pm', '9pm', '10pm', '11pm', '12am']

plt.figure(1, figsize=(14, 6), tight_layout=True)
plt.plot(time, E, '-', label = 'Bldg E', color='black')
plt.plot(time, B, '--', label = 'Bldg B', color='black')
plt.plot(time, D,'-.',label = 'Bldg D', color='black')
plt.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
plt.xticks(rotation=35)
plt.legend()
plt.title("Average Hourly Cold Water Use", fontsize=18)
plt.ylabel('Water Use (gal)', fontsize=16)
plt.xlabel('Time', fontsize=16)
plt.tick_params(labelsize='large')

# Save fig
os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dataPresentation/figures/')
plt.savefig('AvgHourlyWaterUse.png')

plt.show()

print('done')




