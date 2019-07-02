# ---------------------------------------------------------------------------
# SemesterProject_source.py
#
# Author(s):  Joseph Brewer / Jaxon White
# Creation Date: 11/14/2018
#
# Development Environment:
#   Pycharm
#   Python 3.7
#
# This script is a general visualization tool used in conjunction with
# the ongoing water-related-energy study, under the CUASHI research
# umbrella, focusing on the Living & Learning Community at Utah State.
# Based on input, it queries an InfluxDB database, where
# water consumption data is stored.  Based on user input,
# the script performs a variety of tasks, including:
#   - visualizing raw hot water and cold water use data
#   - resamples data to the desired resolution
#   - calculates period volume based on resampled data
#
# ----------------------------------------------------------------------------

import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from datetime import datetime


print('Receiving inputs...')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-02-08T00:00:00Z'"
endDate = "'2019-04-22T00:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID2 = input("Input building ID: ").upper()
bldgID = "'" + bldgID2 + "'"



print('Connecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "coldInFlowRate" FROM "flow" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""



print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_query = client.query(query)
main_Ls = list(main_query.get_points(measurement='flow'))
main = pd.DataFrame(main_Ls)
#main_Df.sort_values(by=['source'], inplace=True)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

print('Data retrieved...')


print('Resampling data...')
# Resample tool
# Input resample rule:
#   Options are: Daily, #D.
#                Weekly, #W,
#                Hourly, #H,
#                Minute, #T.
mainCold = main['coldInFlowRate']/60
mainColdDaily = mainCold.resample('D').sum()


# Plot results - raw data, resampled data, resampled with stats.
print('Plotting results...')

# Initialize figures and subplots
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()


# 1st row - raw data


axCold1 = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axCold1.plot(mainColdDaily, color='blue')
axCold1.set_title('Daily cold water flowrate BLDG: '+bldgID2, fontsize=10, weight ='bold')
#axCold1.legend(loc='upper left')
axCold1.set_ylabel('GPM')
axCold1.set_xlim(beginDate, endDate)
axCold1.grid(True)




plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.show()

print('done')

