#  name: tempQC_correlationEQ.py
#  desc: Generates a correlation EQ for adjusting temperature data.
#  inputs: 2 temp datasets
#  author: joseph brewer
#  last updated: 5/18/2020

# import libraries
import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
beginDate = "'2019-03-23T12:00:00Z'" # Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
endDate = "'2019-03-29T08:00:00Z'"

bldg1 = input('Input bldg id for CORRECT temp data: ').upper()
bldg2 = input('Input bldg id for temp data to ADJUST: ').upper()
bldgID1= "'" + bldg1 + "'" # convert to "'char"' format:  required influxdb format
bldgID2 = "'" + bldg2 + "'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')


print('Assembling data query...')
# Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query BOTH building temp datasets
query = """SELECT "buildingID", "hotInTemp"
FROM "LLC" 
WHERE "buildingID" ="""+bldgID1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
results = client.query(query) # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
df = pd.DataFrame(list(results.get_points(measurement='LLC'))) # set measurement = 'LLC'
df['time'] = pd.to_datetime(df['time'])# Set dataframe index as datetime.
df.set_index('time', inplace=True)

correct = df[df['buildingID'] == bldg1]

# Query raw data to adjust
client.switch_database('ciws_final')
query = """SELECT "buildingID", "hotInTemp"
FROM "flow" 
WHERE "buildingID" ="""+bldgID1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
results = client.query(query) # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
df = pd.DataFrame(list(results.get_points(measurement='LLC'))) # set measurement = 'LLC'
df['time'] = pd.to_datetime(df['time'])# Set dataframe index as datetime.
df.set_index('time', inplace=True)

adjust = df[df['buildingID'] == bldg2]

df = pd.merge(correct, adjust, on='time')


print('Performing linear regression...\n')
X = df.iloc[:,1].values.reshape(-1,1)  # correct temp data serves as independent variable
Y = df.iloc[:,3].values.reshape(-1,1)  # temp data to adjust is dependent variable
model = LinearRegression().fit(X,Y)    # fit data to X
Y_pred = model.predict(X)              # predict new Y values fit to model

print('lr equation for temp data: %fx + %f' %(model.coef_, model.intercept_))

plt.scatter(X,Y)
plt.plot(X, Y_pred, color='red')
plt.xlabel('Correct temp data (C)')
plt.ylabel('Adjusted temp data (C)')
plt.show()



print('done!')