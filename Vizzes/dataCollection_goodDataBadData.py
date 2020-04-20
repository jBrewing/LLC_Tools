import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import os

print('\nConnecting to database...')
# Create client object with InfluxDBClient librarye
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws') # set database


print('Retrieving data for B...')
beginDate = "'2019-02-01T00:00:00Z'"
endDate = "'2019-02-11T00:00:00Z'"
query = """SELECT * FROM "flow" WHERE "buildingID" ='B' AND time >= """+beginDate+""" AND time <= """+endDate+""""""

# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query)
data = pd.DataFrame(list(results.get_points(measurement='flow')))
data['time'] = pd.to_datetime(data['time'])
data.set_index('time', inplace=True) # Set dataframe index as datetime.

badHot = data.loc['2019-02-01 00:00:00': '2019-02-02 00:00:00']
goodHot = data.loc['2019-02-09 00:00:00': '2019-02-10 00:00:00']


print('Retrieving data for E...')
beginDate = "'2019-03-01T00:00:00Z'"
endDate = "'2019-03-10T00:00:00Z'"
query = """SELECT * FROM "flow" WHERE "buildingID" ='E' AND time >= """+beginDate+""" AND time <= """+endDate+""""""

# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query)
data = pd.DataFrame(list(results.get_points(measurement='flow')))
data['time'] = pd.to_datetime(data['time'])
data.set_index('time', inplace=True) # Set dataframe index as datetime.

badReturn = data.loc['2019-03-01 00:00:00': '2019-03-02 00:00:00']
goodReturn = data.loc['2019-03-05 00:00:00': '2019-03-06 00:00:00']

print('Plotting results...')
# Initialize figures and subplots

gridsize=(2,2)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()


 # 1st row - faulty hot in
axHotBad = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotBad.plot(badHot['hotInFlowRate'], color='red')
axHotBad.set_title('Faulty Hot Supply', fontsize=16, weight ='bold')
axHotBad.set_ylabel('GPM',fontsize = 14)
axHotBad.grid(True)
plt.tick_params(labelsize='medium')

# 2nd row - faulty hot return
axReturnBad= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axReturnBad.plot(badReturn['hotOutFlowRate'], color='maroon')
axReturnBad.set_title('Faulty Hot Return', fontsize=16, weight ='bold')
axReturnBad.set_ylabel('GPM',fontsize = 14)
axReturnBad.grid(True)
plt.tick_params(labelsize='medium')

# 1st row - correct hot in
axHotGood = plt.subplot2grid(gridsize, (0,1))
plt.xticks(fontsize=8, rotation=35)
axHotGood.plot(goodHot['hotInFlowRate'], color='red')
axHotGood.set_title('Correct Hot Suply', fontsize=16, weight ='bold')
axHotGood.set_ylabel('GPM',fontsize = 14)
axHotGood.grid(True)
plt.tick_params(labelsize='medium')

# 2nd row - correct hot out
axReturnGood = plt.subplot2grid(gridsize, (1,1))
plt.xticks(fontsize=8, rotation=35)
axReturnGood.plot(goodReturn['hotOutFlowRate'], color='maroon')
axReturnGood.set_title('Correct Hot Return', fontsize=16, weight ='bold')
axReturnGood.set_ylabel('GPM' ,fontsize = 14)
axReturnGood.grid(True)
plt.tick_params(labelsize='medium')

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)



os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/')
plt.savefig('goodData_vs_badData.png')

plt.show()


print('done!')