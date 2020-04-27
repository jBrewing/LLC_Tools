import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
from tempQC.calibrationFunctions import data_chunk

# accept inputs
print('Receiving inputs...\n')
#    building ID
bldg = input("Input building ID: ").upper()
bldgID = "'" + bldg + "'"
#    dates
beginDate = "'2019-03-23T08:00:00Z'"
endDate = "'2019-03-23T12:00:00Z'"

# Retrieve data
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final') # Set database.

#  write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "LLC" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='LLC')))

df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True) # Set time as index

print('Data retrieved! \n')


# print final data
print('Plotting data...')
# Initialize figures and subplots
gridsize = (1, 1)
fig = plt.figure(1, figsize=(12, 8))
fig.autofmt_xdate()
#fig.suptitle('Water Use for BLDG ' + bldgID, fontsize=14, weight='bold')

# 1st row - hot use
axHotUse = plt.subplot2grid(gridsize, (0, 0))
plt.xticks(fontsize=8, rotation=35)
axHotUse.plot(df['hotInTemp'], color='red')
axHotUse.plot(df['hotOutTemp'], color='maroon')
axHotUse.set_title('hot water supply', fontsize=10, weight='bold')
axHotUse.set_ylabel('C')
axHotUse.set_ylim(49.5, 53)
axHotUse.grid(True)

# 2nd row - cold in
#axColdUse = plt.subplot2grid(gridsize, (1, 0))
#plt.xticks(fontsize=8, rotation=35)
#axColdUse.plot(df['hotOutTemp'], color='maroon')
#axColdUse.set_title('hot water return', fontsize=10, weight='bold')
#axColdUse.set_ylabel('C')
#axColdUse.set_ylim(49.5, 53)
#axColdUse.grid(True)

fig.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

print('done')