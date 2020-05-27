#  name: tempQC_meanShift.py
#  desc: Determines a level shift for bldg E & F temp data based on mean of temp data from bldg D
#  inputs: 2 temp datasets | 1. bldg D temp dataset 2. faulty temp dataset
#  author: joseph brewer
#  last updated: 5/18/2020

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

bldg1 = 'D'
bldg2 = input('Input bldg id for temp data to ADJUST: ').upper()
bldgID1= "'" + bldg1 + "'" # convert to "'char"' format:  required influxdb format
bldgID2 = "'" + bldg2 + "'"


temp = 'hotOutTemp'


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')


print('Assembling data query...')
# Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query BOTH building temp datasets
query = """SELECT "buildingID", "hotOutTemp"
FROM "LLC" 
WHERE "buildingID" ="""+bldgID1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving first dataset...')
results = client.query(query) # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
df = pd.DataFrame(list(results.get_points(measurement='LLC'))) # set measurement = 'LLC'
df['time'] = pd.to_datetime(df['time'])# Set dataframe index as datetime.
df.set_index('time', inplace=True)

correct = df[df['buildingID'] == bldg1]

# Query raw data to adjust
client.switch_database('ciws')
query = """SELECT "buildingID", "hotOutTemp"
FROM "flow" 
WHERE "buildingID" ="""+bldgID2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving second dataset...')
results = client.query(query) # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
df = pd.DataFrame(list(results.get_points(measurement='flow'))) # set measurement = 'LLC'
df['time'] = pd.to_datetime(df['time'])# Set dataframe index as datetime.
df.set_index('time', inplace=True)

adjust = df[df['buildingID'] == bldg2]

print('Calculating temp adjustment value...')
# goalseek code
factor = 0 # create level shift value
x=0
goal = correct[temp].mean()
while True: # iterate through, increasing 0.01 each iteration until difference between correct mean and adjust mean < 0.001
    factor = factor + 0.001
    adjust[temp] = adjust[temp]+factor
   # test = adjust['hotInTemp'].mean()
    diff = goal - adjust[temp].mean()
    #print('Factor val: %f   | Diff val: %f' % (factor, diff))
    x+=1
    if abs(diff)<0.001:
        print('\n Final adjustment factor for bldg '+bldg2+': %f' % factor)
        print('# of iterations: %f' %  x)
        break
    else:
        adjust = df[df['buildingID'] == bldg2] # reset adjust df to original values





print('done!')