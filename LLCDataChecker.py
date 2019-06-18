import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-06-01T12:00:00Z'"
#endDate = "'2019-03-27T14:00:00Z'"
endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID2 = input("Input building ID: ").upper()
bldgID = "'" + bldgID2 + "'"

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
query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)
mostRecent = main.last('10S')

maxFlowHot = main['hotInFlowRate'].max()
maxFlowCold = main['coldInFlowRate'].max()
minFlowHot = main['hotInFlowRate'].min()
minFlowCold = main['coldInFlowRate'].min()
meanFlowHot = main['hotInFlowRate'].mean()
meanFlowCold = main['coldInFlowRate'].mean()

maxTempHotIn = main['hotInTemp'].max()
minTempHotIn = main['hotInTemp'].min()
meanTempHotIn = main['hotInTemp'].mean()
maxTempCold = main['coldInTemp'].max()
minTempCold = main['coldInTemp'].min()
meanTempCold = main['coldInTemp'].mean()
maxTempHotOut = main['hotOutTemp'].max()
minTempHotOut = main['hotOutTemp'].min()
meanTempHotOut = main['hotOutTemp'].mean()
print('Plotting results...')

# Initialize figures and subplots
gridsize=(3,2)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Water Use Data Check for Building: '+bldgID+' Most Recent data:', fontsize=14, weight='bold')



# 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(main['hotInFlowRate'], color='red', label='1-Sec HOT Data')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

axHotTemp = plt.subplot2grid(gridsize, (0,1))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main['hotInTemp'], color='red', label='1-Sec HOT Data')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(main['coldInFlowRate'], color='blue', label='1-Sec HOT Data')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,1))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(main['coldInTemp'], color='blue', label='1-Sec HOT Data')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)

# 3rd row - hot return
axHotReturnFlow = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotReturnFlow.plot(main['hotOutFlowRate'], color='maroon', label='1-Sec HOT Data')
axHotReturnFlow.set_title('hot out flowrate', fontsize=10, weight ='bold')
axHotReturnFlow.set_ylabel('GPM')
axHotReturnFlow.set_xlim(beginDate, endDate)
axHotReturnFlow.grid(True)

axHotReturnTemp = plt.subplot2grid(gridsize, (2,1))
plt.xticks(fontsize=8, rotation=35)
axHotReturnTemp.plot(main['hotOutTemp'], color='maroon', label='1-Sec HOT Data')
axHotReturnTemp.set_title('hot out temp', fontsize=10, weight ='bold')
axHotReturnTemp.set_ylabel('Temp (C)')
axHotReturnTemp.set_xlim(beginDate, endDate)
axHotReturnTemp.grid(True)

pd.set_option('display.max_columns', 10)
print(mostRecent, '\n')


source = ["type", "HOT", "COLD", "RETURN"]
A = [('MaxFlow', maxFlowHot, maxFlowCold , ''),
     ('MinFlow', minFlowHot, minFlowCold, ''),
     ('MeanFlow', meanFlowHot, meanTempCold, '',),
     ('MaxTemp', maxTempHotIn, maxTempCold, maxTempHotOut),
     ('MinTemp', minTempHotIn, minTempCold, minTempHotOut),
     ('MeanTemp', meanTempHotIn, meanTempCold, meanTempHotOut)]
print(tabulate(A, headers=source))


plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()



print('done!')