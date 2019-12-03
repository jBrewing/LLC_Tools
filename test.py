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
endDate = "'2019-03-30T00:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID = input("Input building ID: ").upper()
bldgID = "'" + bldgID + "'"


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

pi = 3.1415926
dt = 1 #second
fc = 0.007

y = 1
main['NEWhotInFlowRate'] = float("NaN")

print('reducing noise...\n')
a = (2 * pi * dt * fc) / (2 * pi * dt * fc + 1)
for i, row in main.iterrows():
    y = a * row['hotInFlowRate'] + (1-a) * y
    main.at[i, 'NEWhotInFlowRate'] = y

coldInFlow_Sum = 0
hotInFlow_Sum = 0
hotInFlow_Sum_NEW = 0
counter = 0
for y, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
    coldInFlow = row['coldInFlowRate']
    hotInFlow = row['hotInFlowRate']
    hotInNEWFlow = row['NEWhotInFlowRate']
    hotOutFlow = row['hotOutFlowRate']
    if hotOutFlow == 0:
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
        hotInFlow_Sum_NEW = hotInFlow_Sum_NEW + hotInNEWFlow
        counter = counter + 1

    elif hotOutFlow != 0:
        counter = counter + 1
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
        hotInFlow_Sum_NEW = hotInFlow_Sum_NEW + hotInNEWFlow

        main.at[y, 'coldInFlowRate'] = coldInFlow_Sum
        main.at[y, 'hotInFlowRate'] = hotInFlow_Sum
        main.at[y, 'NEWhotInFlowRate'] = hotInFlow_Sum_NEW

        hotInFlow_Sum_NEW = 0
        coldInFlow_Sum = 0
        hotInFlow_Sum = 0
        counter = 0

# convert flowrates to gps, each second obs = amount of flow for that second

# replace pulse counts with 1, 1 pulse = 1 gal
mainFinal = main[(main['hotOutFlowRate'] != 0)]
mainFinal['hotOutFlowRate'] = 1



main['hotInFlowRate'] = main['hotInFlowRate']/60
main['NEWhotInFlowRate'] = main['NEWhotInFlowRate']/60

#main['NEWhotInFlowRate'] = main['NEWhotInFlowRate']+0.02
#main.loc[main['NEWhotInFlowRate'] < 1.005] =1


main['hotWaterUse'] = main['hotInFlowRate'] - main['hotOutFlowRate']
main['hotWaterUse_NEW'] = main['NEWhotInFlowRate'] - main['hotOutFlowRate']

hotUse_Sum = main['hotInFlowRate'].sum()
hotUse_Sum_NEW = main['NEWhotInFlowRate'].sum()
diff = (hotUse_Sum_NEW-hotUse_Sum)/hotUse_Sum *100

print(hotUse_Sum)
print(hotUse_Sum_NEW)
print(diff)

print('Plotting final flowrates...')
gridsize=(2,1)
fig=plt.figure(1,figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle("bldg: "+bldgID, fontsize=14, weight='bold')

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(main['hotInFlowRate'], color='red', label='hotIn')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
#    axHotFlow.set_ylim(3,4)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(main['NEWhotInFlowRate'], color='maroon', label='New_hotIN')
axColdFlow.set_title('NEW hot water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.set_ylim(0.5,)
axColdFlow.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)



plt.show()










print('done')
