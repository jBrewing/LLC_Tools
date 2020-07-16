import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/dataQC/')


print('Receiving inputs...\n')
# Input parameters.
beginDate = "'2019-04-01T00:00:00Z'"
endDate = "'2019-04-08T00:00:00Z'"
bldgID = input("Input building ID: ").upper()
bldgID = "'" + bldgID + "'"



print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')


print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "hotInTemp", "hotOutTemp" FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""


print('Retrieving data...')
df = client.query(query)
df = pd.DataFrame(list(df.get_points(measurement='flow'))) # Convert returned ResultSet to Pandas dataframe with list and get_points.
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True)  # Set dataframe index as datetime.


#df = df.truncate(before=pd.Timestamp('2019-03-23T08:00:00Z'),after=pd.Timestamp('2019-03-23T09:00:00Z'))

df['new_hotInTemp'] = df['hotInTemp'] + 4.47
df['new_hotOutTemp'] = df['hotOutTemp'] + 4.26



print('Plotting final flowrates...')
fig,ax = plt.subplots(figsize=(14, 10))

fig.autofmt_xdate()

# 1st row - hot in
ax.plot(df['new_hotInTemp'], color='red', label='Level Shifted Supply')
ax.plot(df['new_hotOutTemp'], color='maroon', label='Level Shifted Return')
ax.plot(df['hotInTemp'], '--', color='red', label='Raw Supply')
ax.plot(df['hotOutTemp'], '--', color='maroon', label='Raw Return')
#ax.set_title('Raw Hot Water Supply Flow Signal', fontsize=18)
ax.set_ylabel('Temp (oC)', fontsize = 18, weight = 'bold')
ax.set_xlabel('Date', fontsize=18, weight='bold')
ax.set_ylim(44,56)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%T"))
ax.legend(fontsize=14, ncol=2)
plt.yticks(fontsize=14)
plt.xticks(fontsize=14, rotation = 20)
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


print('saving fig...')
plt.savefig('dataQC_tempLevelShift-hot.png')

plt.show()








print('done')