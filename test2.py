import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
from calibrationFactor import caliFact
from tabulate import tabulate


# accept inputs
print('Receiving inputs...\n')
#    building ID
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T15:00:00Z'"
endDate = "'2019-03-29T15:00:00Z'"


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
#   send query
print('Retrieving data...')
main_Query = client.query(query)
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
# Set dataframe index as datetime.
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)


print('Data retrieved! \n')
hotInTOLD = main['hotInTemp'].iloc[0]
hotOutTOLD = main['hotOutTemp'].iloc[0]
coldInFOLD = main['coldInFlowRate'].iloc[0]
coldInTOLD = main['coldInTemp'].iloc[0]
hotInFOLD = main['hotInFlowRate'].iloc[0]
hotOutFOLD = main['hotOutFlowRate'].iloc[0]
std = main['hotInTemp'].std()
var = main['hotInTemp'].var()
mean = main['hotInTemp'].mean()

calibration = caliFact(bldgIDInput1)

print('QCing rest of data...')
# giant for loop
#   handles: Temp level shift, extreme values (3000+), and return flow
#   define values
coldInFlow_Sum =0
hotInFlow_Sum = 0
hotInTemp_Sum = 0
coldInTemp_Sum = 0
hotOutTemp_Sum = 0
counter = 0
for i, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
    coldInFlow = row['coldInFlowRate']
    coldInTemp = row['coldInTemp']
    hotInFlow = row['hotInFlowRate']
    hotInTemp = row['hotInTemp']
    hotOutFlow = row['hotOutFlowRate']
    hotOutTemp = row['hotOutTemp']


# tempQC_levelshift
#  add calibration
    hotInTemp = hotInTemp + calibration[0]
    hotOutTemp = hotOutTemp + calibration[1]
    if hotInTemp > hotOutTemp:
        main.at[i, 'hotInTemp'] = hotInTemp
        main.at[i, 'hotOutTemp'] = hotOutTemp
    else:
        main.at[i, 'hotInTemp'] = hotInTemp
        main.at[i, 'hotOutTemp'] = hotInTemp


#  Not QCing cold flow.  Using raw measurements.
#    coldInTemp = coldInTemp - calibration[2]
#    main.at[i, 'coldInTemp'] = coldInTemp


#   eliminate_extreme_vals
    if (hotInTOLD-hotInTemp) > std:
        hotInTemp = hotInTOLD
        main.at[i, 'hotInTemp']= hotInTemp

# reacquire hotInTemp / hotOutTemp after calibrating raw data and eliminating extreme values
#    hotInTemp = row['hotInTemp']
#    hotOutTemp = row['hotOutTemp']
#   Return flow fixed
    if hotOutFlow == 0:
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        coldInTemp_Sum = coldInTemp_Sum + coldInTemp
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
        hotInTemp_Sum = hotInTemp_Sum + hotInTemp
        hotOutTemp_Sum = hotOutTemp_Sum + hotOutTemp
        counter = counter + 1
    elif hotOutFlow != 0:
        counter = counter + 1
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        coldInTemp_Sum = (coldInTemp_Sum + coldInTemp) / counter
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
        hotInTemp_Sum = (hotInTemp_Sum + hotInTemp) / counter
        hotOutTemp_Sum = (hotOutTemp_Sum + hotOutTemp) / counter


        main.at[i,'coldInFlowRate'] = coldInFlow_Sum
        main.at[i,'coldInTemp'] = coldInTemp_Sum
        main.at[i, 'hotInFlowRate'] = hotInFlow_Sum
        main.at[i, 'hotInTemp'] = hotInTemp_Sum
        main.at[i, 'hotOutTemp'] = hotOutTemp_Sum

        coldInFlow_Sum = 0
        coldInTemp_Sum = 0
        hotInFlow_Sum = 0
        hotInTemp_Sum = 0
        hotOutTemp_Sum = 0
        counter = 0

# store current loop values for next iteration
    hotInTOLD = hotInTemp
 #   hotOutTOLD = hotOutTemp
 #   coldInTOLD = coldInTemp
 #   hotInFOLD = hotInFlow
 #   hotOutFOLD = hotOutFlow
 #   coldInFOLD = coldInFlow
# end of giant for loop



mainFinal = main[(main['hotOutFlowRate'] != 0)]
mainFinal['coldInFlowRate'] = mainFinal['coldInFlowRate']/60
mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate']/60
mainFinal['hotOutFlowRate'] = 1
mainFinal['hotWaterUse'] = mainFinal['hotInFlowRate'] - mainFinal['hotOutFlowRate']

mainFinal['hotWaterUse_fixed'] = mainFinal['hotWaterUse']

x = 0
print('Fixing hot water use...')
for i, row in mainFinal.iterrows():
    if row['hotWaterUse_fixed'] <0:
        mainFinal.at[i,'hotWaterUse_fixed'] = 0
        x = x+1


print('QC Completed!\n')

totalHotIn = mainFinal['hotInFlowRate'].sum()
totalHotOut = mainFinal['hotOutFlowRate'].sum()
totalColdIn = mainFinal['coldInFlowRate'].sum()
totalHotUse = mainFinal['hotWaterUse'].sum()
totalHotIn_perDay = totalHotIn /28
totalColdIn_perDay = totalColdIn /28
totalHotOut_perDay = totalHotOut /28
totalHotUse_perDay = totalHotUse /28


source = ["type", "HOT In (gal)", "HotOut (gal)" ,  "Hot Use (gal)", "COLD In (gal)"]
A = [('TotalUse(gal)_fact', totalHotIn, totalHotOut,totalHotUse , totalColdIn),
     ('TotalUse/day', totalHotIn_perDay, totalHotOut_perDay,totalHotUse_perDay,totalColdIn_perDay)]
print(tabulate(A, headers=source))


print('done')