import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime


def fetch(date1, date2):
    print('Assembling query...')
    query = """SELECT "hotOutFlowRate" FROM "flow" WHERE "buildingID" ='E' AND time >= """ + date1 + """ AND time <= """ + date2 + """"""

    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list
    # and get_points.
    # Set dataframe index as datetime.
    results = client.query(query)
    df = pd.DataFrame(list(results.get_points(measurement='flow')))
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    return df


print('Receiving inputs...\n')
# Input parameters.

dates = ["'2019-03-22T12:00:00Z'","'2019-03-22T16:00:00Z'",
         "'2019-03-29T12:00:00Z'","'2019-03-29T16:00:00Z'",
         "'2019-04-05T12:00:00Z'","'2019-04-05T16:00:00Z'",
         "'2019-04-12T12:00:00Z'","'2019-04-12T16:00:00Z'"]

print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


df1 = fetch(dates[0], dates[1])
df2 = fetch(dates[2], dates[3])
df3 = fetch(dates[4], dates[5])
df4 = fetch(dates[6], dates[7])

df_fix = df1.copy()

dfs = [df1,df2,df3,df4]
counter = 0
print('QCing Data...')
for df in dfs:
    for i, row in df.iterrows():
        if row['hotOutFlowRate'] ==0:
            counter+= 1
        elif row['hotOutFlowRate'] != 0:
            counter += 1
            df.at[i, 'hotOutFlowRate'] = counter
            counter = 0


df1 = df1[(df1['hotOutFlowRate'] != 0)]
df2 = df2[(df2['hotOutFlowRate'] != 0)]
df3 = df3[(df3['hotOutFlowRate'] != 0)]
df4 = df4[(df4['hotOutFlowRate'] != 0)]

df1_pulses = df1.mean()
df2_pulses = df2.mean()
df3_pulses = df3.mean()
df4_pulses = df4.mean()

df_trunc = df_fix.truncate(after=pd.Timestamp('2019-03-22T14:20:23Z')).copy()
counter=0
x = 1

for i, row in df_trunc.iterrows():
    if x < 4:
        if counter < 16:
            df_trunc.at[i, 'hotOutFlowRate'] = 0
            counter += 1
        else:
            df_trunc.at[i, 'hotOutFlowRate'] = 1
            counter = 0
            x+= 1
    elif x == 4 :
        if counter < 17:
            df_trunc.at[i, 'hotOutFlowRate'] = 0
            counter += 1
        else:
            df_trunc.at[i, 'hotOutFlowRate'] = 1
            x = 1
            counter = 0




df_fix['hotOutFlowRate'].update(df_trunc['hotOutFlowRate'])

for i, row in df_fix.iterrows():
    if row['hotOutFlowRate'] == 0:
        counter+= 1
    elif row['hotOutFlowRate'] != 0:
        counter += 1
        df_fix.at[i, 'hotOutFlowRate'] = counter
        counter = 0



df_fix = df_fix[(df_fix['hotOutFlowRate'] != 0)]

dffix_pulses = df_fix.mean()



print('Plotting results...')

# Initialize figures and subplots

gridsize=(4,1)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Bldg E Return Flow', fontsize=14, weight='bold')


 # 1st row - hot in
axWeek1 = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axWeek1.plot(df1['hotOutFlowRate'], color='maroon')
axWeek1.set_title('week 1', fontsize=10, weight ='bold')
axWeek1.set_ylim(16, 19)
axWeek1.set_ylabel('GPM')
axWeek1.grid(True)

# 2nd row - cold in
axWeek2= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axWeek2.plot(df2['hotOutFlowRate'], color='maroon')
axWeek2.set_title('week 2', fontsize=10, weight ='bold')
axWeek2.set_ylabel('GPM')
axWeek2.set_ylim(16, 19)
axWeek2.grid(True)

# 3rd row - hot return
axWeek3 = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axWeek3.plot(df3['hotOutFlowRate'], color='maroon')
axWeek3.set_title('week 3', fontsize=10, weight ='bold')
axWeek3.set_ylabel('GPM')
axWeek3.set_ylim(16, 19)
axWeek3.grid(True)

# 3rd row - hot return
axWeek4 = plt.subplot2grid(gridsize, (3,0))
plt.xticks(fontsize=8, rotation=35)
axWeek4.plot(df4['hotOutFlowRate'], color='maroon')
axWeek4.set_title('week 4', fontsize=10, weight ='bold')
axWeek4.set_ylabel('GPM')
axWeek4.set_ylim(16, 19)
axWeek4.grid(True)

fig.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()



print('done!')