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
beginDate = "'2019-03-29T00:00:00Z'"
endDate = "'2019-04-05T00:00:00Z'"

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
query = """SELECT * FROM "WaWRE" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))

df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True) # Set time as index

print('Data retrieved! \n')


# print final data
print('Plotting data...')
# Initialize figures and subplots
gridsize = (2, 1)
fig = plt.figure(1, figsize=(12, 8))
fig.autofmt_xdate()
#fig.suptitle('Water Use for BLDG ' + bldgID, fontsize=14, weight='bold')

# 1st row - hot use
axEnergyLost = plt.subplot2grid(gridsize, (0, 0))
plt.xticks(fontsize=8, rotation=35)
axEnergyLost.plot(df['hotUse_energy'], color='darkOrange')
axEnergyLost.plot(df['pipeLoss_energy'], color='black', linewidth=0.5)
axEnergyLost.set_title('Energy to WW and Pipe', fontsize=10, weight='bold')
axEnergyLost.set_ylabel('C')
axEnergyLost.set_ylim(48, 56)
axEnergyLost.grid(True)

# 2nd row - cold in
axEnergySupRet = plt.subplot2grid(gridsize, (1, 0))
plt.xticks(fontsize=8, rotation=35)
axEnergySupRet.plot(df['hotSupply_energy'], color='navy')
axEnergySupRet.plot(df['hotReturn_energy'], color='maroon')
axEnergySupRet.set_title('hotSupply & hotReturn energy', fontsize=10, weight='bold')
axEnergySupRet.set_ylabel('C')
axEnergySupRet.set_ylim(14, 23)
axEnergySupRet.grid(True)


fig.show()

fig2 = plt.figure(2, figsize=(12,8))
gridsize = (2, 1)
# 1st row - hot in
axHotUse = plt.subplot2grid(gridsize, (0, 0))
plt.xticks(fontsize=8, rotation=35)
axHotUse.plot(df['hotWaterUse'], color='red')
axHotUse.set_title('hot water use', fontsize=10, weight='bold')
axHotUse.set_ylabel('Gal/Pulse')
axHotUse.grid(True)

# 2nd row - cold in
axColdUse = plt.subplot2grid(gridsize, (1, 0))
plt.xticks(fontsize=8, rotation=35)
axColdUse.plot(df['coldWaterUse'], color='navy')
axColdUse.set_title('cold water use', fontsize=10, weight='bold')
axColdUse.set_ylabel('Gal/Pulse')
axColdUse.grid(True)


fig2.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

print('done')