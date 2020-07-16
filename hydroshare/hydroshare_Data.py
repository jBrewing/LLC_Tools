import pandas as pd
from influxdb import InfluxDBClient
import os


os.chdir('/Users/augustus/Desktop/GRA/Thesis/hydroshare/')


# accept inputs
print('Receiving inputs...\n')
#    dates
beginDate = "'2019-03-24T00:00:00Z'"
endDate = "'2019-03-31T00:00:00Z'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws') # Set database.

query = """SELECT *
      FROM "flow"
      WHERE time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='flow')))
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True) # Set time as index
print('Data retrieved! \n')



df.to_csv('raw_data.csv')

print('done')