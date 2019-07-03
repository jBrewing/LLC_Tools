# Single data variable vizualizer
#
# Developed by: Joseph Brewer
# Last Modified: 6/17/19
# For: CIWS Research
#
# Description:  This script is used to simultaneously visualize water & water temperature
#               data from 2 buildings in LLC on Utah State University's Campus.
#               The queries share the same date range so what is visualized is a side-by-side
#               comparison of the the specified variable over the a timeframe.
#               This data was collected as part of the Master's Research conducted
#               by Joseph Brewer for Dr. Jeff Horsburgh.
#
#       Inputs: Date range: divided intp 'begin date' and 'end date'
#       BLDGID: The building to query.
#                    Options: (B, C, D, E, F)
#          var: The variable to be visualized, case sensitive
#                    Options: flowrate (hotInFlowRate, coldInFlowRate, hotOutFlowRate)
#                                 temp (hotInTemp, coldInTemp, hotOutTemp)



import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime


print('Receiving inputs...\n')
# Input parameters.
var = input("Input variable (case sensitive): ")
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
# First Query
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-03-30T12:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID = input("Input building ID: ").upper()
bldgIDQuery = "'" + bldgID + "'"
bldgIDlabel1 = bldgID

# 2nd Query
bldgID2 = input("Input building ID: ").upper()
bldgIDQuery2 = "'" + bldgID2 + "'"
bldgIDlabel2 = bldgID2

testDateBegin ="'2019-01-29T00:00:00Z'"
testDateEnd = "'2019-02-05T23:59:59Z'"
#beginDate = testDateBegin
#endDate = testDateEnd

print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgIDQuery+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

print('Data retrieved \n')



print('Assembling second query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query2 = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgIDQuery2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving second data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query2 = client.query(query2)
main_ls2 = list(main_Query2.get_points(measurement='flow'))
main2 = pd.DataFrame(main_ls2)
main2['time'] = pd.to_datetime(main2['time'])
main2.set_index('time', inplace=True)

print('Second Data retrieved.\n')
print('plotting results')
# Initialize figures and subplots
#gridsize=(3,2)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main[var], color='red', label='BLDG '+bldgIDlabel1)
axHotTemp.plot(main2[var], color='blue', label='BLDG '+bldgIDlabel2)
axHotTemp.set_title(bldgIDlabel1+' vs. '+bldgIDlabel2+" "+var, fontsize=10, weight ='bold')
axHotTemp.legend(loc='lower right')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.set_ylim(min(main[var])-5,max(main[var]+5))
axHotTemp.grid(True)



plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()





print('done!')