import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime
import numpy as np


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
#PORStart = "'2019-03-22T12:00:00Z'"
#POREnd = "'2019-04-19T12:00:00Z'"
#beginDate = PORStart
#endDate = POREnd
beginDate = "'2019-03-24T00:00:00Z'"
endDate = "'2019-03-25T00:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
#bldgID2 = input("Input building ID: ").upper()
#bldgID = "'" + bldgID2 + "'"
bldgID = "'D'"

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
main = client.query(query)
main_ls = list(main.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

print('Resampling values to pulse interval...')
main['hotInSum']=0
main['coldInSum'] = 0
main['hotInTemp_avg']=0
main['coldInTemp_avg'] = 0
main['hotOutTemp_avg'] = 0

coldSum =0
hotSum = 0
hotInTemp = 0
coldTemp = 0
hotOutTemp = 0
counter = 0
for i, row in main.iterrows():
    a = row['coldInFlowRate']
    b = row['coldInTemp']
    c = row['hotInFlowRate']
    d = row['hotInTemp']
    e = row['hotOutFlowRate']
    f = row ['hotOutTemp']
    if e == 0:
        coldSum = coldSum + a
        coldTemp = coldTemp + b
        hotSum = hotSum + c
        hotInTemp = hotInTemp + d
        hotOutTemp = hotOutTemp + f
        counter = counter + 1
    elif e != 0:
        counter = counter + 1
        coldSum = coldSum + a
        coldTemp = (coldTemp + b) / counter
        hotSum = hotSum + c
        hotInTemp = (hotInTemp + d) / counter
        hotOutTemp = (hotOutTemp + f) / counter


        main.at[i,'coldInSum'] = hotSum
        main.at[i,'coldInTemp_avg'] = coldTemp
        main.at[i, 'hotInSum'] = hotSum
        main.at[i, 'hotInTemp_avg'] = hotInTemp
        main.at[i, 'hotOutTemp_avg'] = hotOutTemp


        coldSum = 0
        hotSum = 0
        hotInTemp = 0
        coldTemp = 0
        hotOutTemp = 0
        counter = 0


returnTime = main[(main['hotOutFlowRate'] != 0)]
returnTime = returnTime/60
returnTime['hotOutFlowRate'] = 1
returnTime['hotWaterUse'] = returnTime['hotInSum'] - returnTime['hotOutFlowRate']


#resampleRule = input('Input Resample Rule: ')
#mainReturn  = main['hotOutFlowRate'].copy()
#mainReturn = pd.DataFrame(mainReturn)

#main = main.drop('hotOutFlowRate', axis=1)

#main = main.resample(resampleRule).sum()/60

#mainReturn = mainReturn.replace({0 : np.nan}).groupby(pd.Grouper(freq=resampleRule)).count()

#main = pd.merge(main, mainReturn, on='time')

#main['hotWaterUse'] = main['hotInFlowRate'] - main['hotOutFlowRate']


totalHotIn_fact = main['hotInFlowRate'].sum()/60
totalHotOut_fact = main['hotOutFlowRate'].sum()/60
totalColdIn_fact = main['coldInFlowRate'].sum()/60
totalHotUse_fact = totalHotIn_fact - totalHotOut_fact

totalHotIn = returnTime['hotInSum'].sum()
totalHotOut = returnTime['hotOutFlowRate'].sum()
totalColdIn = returnTime['coldInSum'].sum()
totalHotUse = totalHotIn - totalHotOut


main2 = returnTime.copy()
for i, row in main2.iterrows():
    x = row['hotWaterUse']
    if x < 0:
        main2.at[i,'hotWaterUse'] = 0

totalHotUse_zero = main2['hotWaterUse'].sum()


source = ["type", "HOT In (gal)", "COLD In (gal)", "HotOut (gal)", "Use (gal)"]
A = [('TotalUse(gal)_fact', totalHotIn_fact, totalColdIn_fact , totalHotOut_fact, totalHotUse_fact),
     ('TotalUse(gal)_resam', totalHotIn, totalColdIn, totalHotOut, totalHotUse),
     ('TotalUse(gal)_Zero', "-", "-", "-", totalHotUse_zero)]
print(tabulate(A, headers=source))



# Initialize figures and subplots
gridsize=(3,1)
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Water Use Data Check for Building: '+bldgID+' Most Recent data:', fontsize=14, weight='bold')



# 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(returnTime['hotInSum'], color='red', label='supply')
axHotFlow.set_title('hot water supply', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('Water Use (gal)')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

axHotTemp = plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(returnTime['hotOutFlowRate'], color='maroon', label='return')
axHotTemp.set_title('hot water return', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Water Use (gal)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

axHotTemp = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(returnTime['hotWaterUse'], color='black', label='return')
axHotTemp.set_title('hot water use', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Water Use (gal)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

plt.show()




print('done!')