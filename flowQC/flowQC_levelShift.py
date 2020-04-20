import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np

bldg = 'B'
bldgIDQ1 = "'" + bldg + "'"
beginDate = "'2019-04-13T00:00:00Z'"
endDate = "'2019-04-19T00:00:00Z'"

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


df_2 = df.truncate(before=pd.Timestamp('2019-04-16T19:40:00Z')).copy()


print('Calculating new flow values...')
df_2['hotInFlowRate'] = df_2['hotInFlowRate'] - 0.032

#   update original dataframe, df
df['hotInFlowRate'].update(df_2['hotInFlowRate'])
print('New hotInTemp values for BLDG B calculated!\n')

fig, ax = plt.subplots(figsize=(14,6))
p1 = ax.plot(df, color = 'marooon')
ax.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
ax.set_xlabel('time', fontsize=16)
ax.set_ylabel('Water Use (gal)', fontsize=16)
ax.tick_params(labelsize='large')

print('done!')