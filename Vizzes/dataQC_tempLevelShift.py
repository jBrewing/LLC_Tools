import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np
import os

filepath = '/Users/augustus/Desktop/GRA/Thesis/Figures/data/'
file = 'BLDG_C_03-23_02-24_RAWflow.csv'



print('Receiving inputs...\n')
# Input parameters.
beginDate = "'2019-04-19T00:00:00Z'"
endDate = "'2019-04-19T11:59:59Z'"
bldgID = input("Input building ID: ").upper()
bldgID = "'" + bldgID + "'"


df = pd.read_csv(filepath+file)
df.drop(columns=['buildingID', 'coldInFlowRate', 'hotInFlowRate', 'hotOutFlowRate', 'coldInTemp'])
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True)  # Set dataframe index as datetime.

"""
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')
"""

print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "hotInFlowRate", "coldInFlowRate" FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

"""
print('Retrieving data...')
df = client.query(query)
df = pd.DataFrame(list(df.get_points(measurement='flow'))) # Convert returned ResultSet to Pandas dataframe with list and get_points.
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True)  # Set dataframe index as datetime.
"""

#df = df.truncate(before=pd.Timestamp('2019-03-23T08:00:00Z'),after=pd.Timestamp('2019-03-23T09:00:00Z'))

df['new_hotInTemp'] = df['hotInTemp'] + 6.5
df['new_hotOutTemp'] = df['hotOutTemp'] + 4.06



print('Plotting final flowrates...')
os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/')

fig = plt.figure(1, figsize=(14, 10))
gridsize = (2, 1)
fig.autofmt_xdate()


# 1st row - hot in
axTempRaw = plt.subplot2grid(gridsize, (0, 0))
axTempRaw.plot(df['hotInTemp'], color='red', label='Raw Data')
axTempRaw.plot(df['hotOutTemp'], color='maroon', label='Raw Data')
#axTempRaw.set_title('Raw Hot Water Supply Flow Signal', fontsize=18)
axTempRaw.set_ylabel('Temp (oC)', fontsize = 16)
axTempRaw.set_xlabel('')
#axTempRaw.set_ylim(50,55)
plt.xticks([])
plt.yticks(fontsize=12)

# 2nd row - cold in
axTempNew = plt.subplot2grid(gridsize, (1, 0))

axTempNew.plot(df['new_hotInTemp'], color='red', label='Filtered Data')
axTempNew.plot(df['new_hotOutTemp'], color='maroon', label='Filtered Data')
#axTempNew.set_title('Filtered Hot Water Supply Flow Signal', fontsize=18)
axTempNew.set_ylabel('Temp (oC)', fontsize=16)
axTempNew.set_xlabel('Time', fontsize=16)
#axTempNew.set_ylim(3,5.25)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

fig.show()

print('saving fig...')
#plt.savefig('dataQC_tempLevelShift-hot.png')



plt.show()



print('done')