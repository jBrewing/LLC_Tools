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
#PORStart = "'2019-03-22T15:00:00Z'"
#POREnd = "'2019-04-19T15:00:00Z'"
#beginDate = PORStart
#endDate = POREnd
beginDate = "'2019-03-23T00:00:00Z'"
endDate = "'2019-03-24T00:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID2 = input("Input building ID: ").upper()
bldgID = "'" + bldgID2 + "'"

days = int(input("input number of days:  "))

#testDateBegin ="'2019-01-29T00:00:00Z'"
#testDateEnd = "'2019-02-05T23:59:59Z'"
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
#query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgID+""""""

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
mainRaw = main.copy()
print('QCing Data...')


coldInFlow_Sum =0
hotInFlow_Sum = 0
hotInTemp_Sum = 0
coldInTemp_Sum = 0
hotOutTemp_Sum = 0
counter = 0
#for i, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
#    coldInFlow = row['coldInFlowRate']
#    coldInTemp = row['coldInTemp']
#    hotInFlow = row['hotInFlowRate']
#    hotInTemp = row['hotInTemp']
#    hotOutFlow = row['hotOutFlowRate']
#    hotOutTemp = row['hotOutTemp']
#    if hotOutFlow == 0:
#          coldInFlow_Sum = coldInFlow_Sum + coldInFlow
#          coldInTemp_Sum = coldInTemp_Sum + coldInTemp
#          hotInFlow_Sum = hotInFlow_Sum + hotInFlow
#          hotInTemp_Sum = hotInTemp_Sum + hotInTemp
#          hotOutTemp_Sum = hotOutTemp_Sum + hotOutTemp
#          counter = counter + 1
#    elif hotOutFlow != 0:
#         counter = counter + 1
#          coldInFlow_Sum = coldInFlow_Sum + coldInFlow
#          coldInTemp_Sum = (coldInTemp_Sum + coldInTemp) / counter
#          hotInFlow_Sum = hotInFlow_Sum + hotInFlow
#          hotInTemp_Sum = (hotInTemp_Sum + hotInTemp) / counter
#          hotOutTemp_Sum = (hotOutTemp_Sum + hotOutTemp) / counter

#          main.at[i, 'coldInFlowRate'] = coldInFlow_Sum
#          main.at[i, 'coldInTemp'] = coldInTemp_Sum
#          main.at[i, 'hotInFlowRate'] = hotInFlow_Sum
#          main.at[i, 'hotInTemp'] = hotInTemp_Sum
#          main.at[i, 'hotOutTemp'] = hotOutTemp_Sum

#          coldInFlow_Sum = 0
#          coldInTemp_Sum = 0
#          hotInFlow_Sum = 0
#          hotInTemp_Sum = 0
#          hotOutTemp_Sum = 0
#          counter = 0


#mainFinal = main[(main['hotOutFlowRate'] != 0)]
#mainFinal['coldInFlowRate'] = mainFinal['coldInFlowRate']/60
#mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate']/60
#mainFinal['hotOutFlowRate'] = 1
#mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate']
#mainFinal['hotWaterUse'] = mainFinal['hotInFlowRate'] - mainFinal['hotOutFlowRate']

#baseline = mainFinal.loc['2019-10-08T00:00:00Z': '2019-10-15T00:00:00Z']
#week1 = mainFinal.loc['2019-11-05T00:00:00Z':'2019-11-12T00:00:00Z']

#baselineSum = (baseline['hotWaterUse'] + baseline['coldInFlowRate']).sum()
#baselineSum_perDay = baselineSum/7
#baselineSum_perCap = baselineSum/95

#week1Sum = week1['hotWaterUse'] + week1['coldInFlowRate'].sum()
#week1Sum_perDay=week1Sum/7
#week1Sum_perCap = week1Sum/95



maxFlowHot = main['hotInFlowRate'].max()
maxFlowCold = main['coldInFlowRate'].max()
minFlowHot = main['hotInFlowRate'].min()
minFlowCold = main['coldInFlowRate'].min()
meanFlowHot = main['hotInFlowRate'].mean()
meanFlowCold = main['coldInFlowRate'].mean()

#rawHotUse = ((mainRaw['hotInFlowRate'] - mainRaw['hotOutFlowRate'])/60).sum()
#rawColdUse = (mainRaw['coldInFlowRate']/60).sum()

maxTempHotIn = main['hotInTemp'].max()
minTempHotIn = main['hotInTemp'].min()
meanTempHotIn = main['hotInTemp'].mean()
maxTempCold = main['coldInTemp'].max()
minTempCold = main['coldInTemp'].min()
meanTempCold = main['coldInTemp'].mean()
maxTempHotOut = main['hotOutTemp'].max()
minTempHotOut = main['hotOutTemp'].min()
meanTempHotOut = main['hotOutTemp'].mean()

sumHotIn = main['hotInFlowRate'].sum()/60
sumHotOut = main['hotOutFlowRate'].sum()/60
sumColdIn = main['coldInFlowRate'].sum()/60

hotUse = sumHotIn-sumHotOut

print('Plotting results...')

"""

# 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(mainRaw['hotInFlowRate'], color='red', label='1-Sec HOT Data')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

axHotTemp = plt.subplot2grid(gridsize, (0,1))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(mainRaw['hotInTemp'], color='red', label='1-Sec HOT Data')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(mainRaw['coldInFlowRate'], color='blue', label='1-Sec HOT Data')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,1))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(mainRaw['coldInTemp'], color='blue', label='1-Sec HOT Data')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)

# 3rd row - hot return
axHotReturnFlow = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotReturnFlow.plot(mainRaw['hotOutFlowRate'], color='maroon', label='1-Sec HOT Data')
axHotReturnFlow.set_title('hot out flowrate', fontsize=10, weight ='bold')
axHotReturnFlow.set_ylabel('GPM')
axHotReturnFlow.set_xlim(beginDate, endDate)
axHotReturnFlow.grid(True)

axHotReturnTemp = plt.subplot2grid(gridsize, (2,1))
plt.xticks(fontsize=8, rotation=35)
axHotReturnTemp.plot(mainRaw['hotOutTemp'], color='maroon', label='1-Sec HOT Data')
axHotReturnTemp.set_title('hot out temp', fontsize=10, weight ='bold')
axHotReturnTemp.set_ylabel('Temp (C)')
axHotReturnTemp.set_xlim(beginDate, endDate)
axHotReturnTemp.grid(True)
fig.show()

"""
source = ["type", "HOT", "COLD", "RETURN"]
A = [('WaterUse', hotUse, sumColdIn, ''),
     ('WaterUse/day', hotUse/days, sumColdIn/days, ''),
     ('MeanFlow', meanFlowHot, meanTempCold, '',),
     ('MaxTemp', maxTempHotIn, maxTempCold, maxTempHotOut),
     ('MinTemp', minTempHotIn, minTempCold, minTempHotOut),
     ('MeanTemp', meanTempHotIn, meanTempCold, meanTempHotOut)]
print(tabulate(A, headers=source))


# Initialize figures and subplots

gridsize=(3,2)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Water Use Data Check for Building: '+bldgID+' Most Recent data:', fontsize=14, weight='bold')


print('Plotting final flowrates...')
gridsize=(3,1)
fig2=plt.figure(1,figsize=(12,8))
fig2.autofmt_xdate()
fig2.suptitle('Flowrate for BLDG '+bldgID, fontsize=14, weight='bold')

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(main['hotInFlowRate'], color='red', label='hotIn_flow')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(main['coldInFlowRate'], color='blue', label='coldIn_flow')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

# 3rd row - hot return
axHotWaterUse = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotWaterUse.plot(main['hotOutFlowRate'], color='maroon', label='hotOut_flow')
axHotWaterUse.set_title('hotOut pulses', fontsize=10, weight ='bold')
axHotWaterUse.set_ylabel('GPM')
axHotWaterUse.set_xlim(beginDate, endDate)
#axHotWaterUse.set_ylim(-0.01, 0.05)
axHotWaterUse.grid(True)

#fig2.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


print('Plotting final temperatures...')
fig3=plt.figure(2,figsize=(12,8))
fig3.suptitle('Temps for BLDG '+bldgID, fontsize=14, weight='bold')

gridsize = (2,1)
axHotTemp = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main['hotInTemp'], color='red', label='hotIn_temp')
axHotTemp.plot(main['hotOutTemp'], color='maroon', label='hotOut_temp')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(main['coldInTemp'], color='blue', label='coldIn_temp')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
fig3.show()
plt.show()


pd.set_option('display.max_columns', 10)
print(mostRecent, '\n')





#plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
#plt.show()



print('done!')