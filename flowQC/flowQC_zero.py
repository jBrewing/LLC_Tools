import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np

bldg = 'C'
bldgIDQ1 = "'" + bldg + "'"
beginDate = "'2019-03-27T00:00:00Z'"
endDate = "'2019-03-28T00:00:00Z'"

print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')

print ('Fetching data...')
# query 2nd correlation dataset and convert to dataframe
query = """SELECT "hotInFlowRate" FROM "flow" WHERE "buildingID" =""" + bldgIDQ1 + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""
results = client.query(query)
df = pd.DataFrame(list(results.get_points(measurement='flow')))
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)
print('Adjustment data retrieved.\n')

df_zero = df.copy()
for i, row in df.iterrows():
    if row['hotInFlowRate'] <= 3.38:
        df.at[i, 'hotInFlowRate'] = 3.31

raw = df_zero.sum()
new = df.sum()
percent = (new - raw)/raw * 100

print('raw data: ' + str(raw))
print('new data: ' + str(new))
print('%diff: ' + str(percent))
print('Plotting final flowrates...')

gridsize = (2, 1)
fig = plt.figure(1, figsize=(14, 10))
fig.autofmt_xdate()

# 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0, 0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(df_zero['hotInFlowRate'], color='red', label='raw hotIn')
axHotFlow.set_title('RAW hot water flowrate', fontsize=10, weight='bold')
axHotFlow.set_ylabel('GPM')
#axHotFlow.set_xlim(beginDate, endDate)
#axHotFlow.set_ylim(4.2,4.8)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow = plt.subplot2grid(gridsize, (1, 0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(df['hotInFlowRate'], color='maroon', label='New hotIn')
axColdFlow.set_title('NEW hot water flowrate', fontsize=10, weight='bold')
axColdFlow.set_ylabel('GPM')
#axColdFlow.set_ylim(3.65,3.8)
axColdFlow.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.show()


print('done!')