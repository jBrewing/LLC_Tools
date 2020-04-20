import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
from tempQC.calibrationFunctions import caliFact, calibration_flow

# accept inputs
print('Receiving inputs...\n')
#    building ID
bldg = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldg + "'"
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
query = """SELECT "coldInFlowRate","coldInTemp", "hotInFlowRate", "hotInTemp", "hotOutFlowRate", "hotOutTemp"
  FROM "flow" 
  WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
#   send query
print('Retrieving data...')
results = client.query(query)
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
df = pd.DataFrame(list(results.get_points(measurement='flow')))
# Set dataframe index as datetime.
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)

print('Data retrieved!')


# Begin QC ##################

# Temperature QC ############
print('Level shifting temp values...')
columns = ['hotInTemp', 'hotOutTemp', 'coldInTemp']
for (column, cal) in zip(columns, caliFact(bldg)):
    df[column] = df[column] + cal
print('Level shifting temp valus complete! \n')

if bldg == 'B':
# tempQC: correlation, BLDG B
    # Adjust inaccurate BldgB temp data with correlation from Bldg D temp data
    print ('Fetching data to adjust BLDG B temp data...')
    bldgIDQ2 = "'D'"
    endDate2 = "'2019-03-27T16:00:00Z'"
    # query 2nd correlation dataset and convert to dataframe
    query = """SELECT "hotInTemp" 
    FROM "flow" 
    WHERE "buildingID" =""" + bldgIDQ2 + """ AND time >= """ + beginDate + """ AND time <= """ + endDate2 + """"""

    results = client.query(query)
    df_2 =pd.DataFrame(list(results.get_points(measurement='flow')))
    df_2['time'] = pd.to_datetime(df_2['time'])
    df_2.set_index('time', inplace=True)
    print('Adjustment data retrieved.\n')
    df_2.rename(columns={'hotInTemp': 'hotInTemp_D'}, inplace=True)

    mainHot = df['hotInTemp'].truncate(after=pd.Timestamp('2019-03-27T16:00:00Z')).copy()
    mainHot = mainHot.to_frame()
    mainHotQC = pd.merge(mainHot, df_2, on='time')
    #   while loop coupled with for loop using date?
    print('Calculating new temp values...')
    mainHotQC['hotInTemp'] = 8.25122766 + 0.8218035674 * mainHotQC['hotInTemp_D']

    #   update original dataframe, df
    df['hotInTemp'].update(mainHotQC['hotInTemp'])
    print('New hotInTemp values for BLDG B calculated!\n')


# tempQC: level shift
# Add calibration factor to each value
print('Level shifting temp values...')
columns = ['hotInTemp', 'hotOutTemp','coldInTemp']
for (column, cal) in zip(columns, caliFact(bldg)):
    df[column] = df[column] + cal
print('Level shifting temp values complete!\n')

# flowQC: level shift
print('Level shifting hotInFlowRate values...')
if bldg == 'B':
    df_2 = df.truncate(before=pd.Timestamp('2019-04-16T19:40:00Z')).copy()
    df_2['hotInFlowRate'] = df_2['hotInFlowRate'] - 0.032

elif bldg == 'E':
    df_2 = df.truncate(before=pd.Timestamp('2019-04-04T02:22:55Z')).copy()
    df_2['hotInFlowRate'] = df_2['hotInFlowRate'] - 0.031
print('Level shifting flow complete! \n')


# flowQC: zero flows
print('Zeroing hotInFlowRate values...')
calibration = calibration_flow(bldg)
for i, row in df.iterrows():
    if row['hotInFlowRate'] <= calibration[0]:
        df.at[i, 'hotInFlowRate'] = calibration[1]
print('Zeroing flows complete!\n')


# flowQC: Pulse Aggregation
print('Aggregating pulses...')
df_new = df.copy()
coldInFlow_Sum =0 # Initalize variables to aggregate temp/flows in between pulses
hotInFlow_Sum = 0
hotInTemp_Sum = 0
coldInTemp_Sum = 0
hotOutTemp_Sum = 0
counter = 0       # counter will count seconds between pulses
for i, row in df.iterrows():
    if row['hotOutFlowRate'] == 0:
        coldInFlow_Sum = coldInFlow_Sum + row['coldInFlowRate']
        coldInTemp_Sum = coldInTemp_Sum +row['coldInTemp']
        hotInFlow_Sum = hotInFlow_Sum + row['hotInFlowRate']
        hotInTemp_Sum = hotInTemp_Sum + row['hotInTemp']
        hotOutTemp_Sum = hotOutTemp_Sum + row['hotOutTemp']
        counter += 1
    elif row['hotOutFlowRate'] != 0:
        counter = counter + 1
        coldInFlow_Sum = coldInFlow_Sum + row['coldInFlowRate']
        coldInTemp_Sum = (coldInTemp_Sum + row['coldInTemp']) / counter
        hotInFlow_Sum = hotInFlow_Sum + row['hotInFlowRate']
        hotInTemp_Sum = (hotInTemp_Sum + row['hotInTemp']) / counter
        hotOutTemp_Sum = (hotOutTemp_Sum + row['hotOutTemp']) / counter


        df_new.at[i,'coldInFlowRate'] = coldInFlow_Sum
        df_new.at[i,'coldInTemp'] = coldInTemp_Sum
        df_new.at[i, 'hotInFlowRate'] = hotInFlow_Sum
        df_new.at[i, 'hotInTemp'] = hotInTemp_Sum
        df_new.at[i, 'hotOutTemp'] = hotOutTemp_Sum

        coldInFlow_Sum = 0 # Zero running sums after calculating aggregated values
        coldInTemp_Sum = 0
        hotInFlow_Sum = 0
        hotInTemp_Sum = 0
        hotOutTemp_Sum = 0
        counter = 0

print('Pulse aggregation complete!')


#hotInTOLD = main['hotInTemp'].iloc[0]
#hotOutTOLD = main['hotOutTemp'].iloc[0]
#coldInFOLD = main['coldInFlowRate'].iloc[0]
#coldInTOLD = main['coldInTemp'].iloc[0]
#hotInFOLD = main['hotInFlowRate'].iloc[0]
#hotOutFOLD = main['hotOutFlowRate'].iloc[0]
#std = main['hotInTemp'].std()
#var = main['hotInTemp'].var()
#mean = main['hotInTemp'].mean()

"""

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
    hotInFOLD = hotInFlow
 #   hotOutFOLD = hotOutFlow
 #   coldInFOLD = coldInFlow
# end of giant for loop

"""


df_final = df_new[(df_new['hotOutFlowRate'] != 0)]
df_final['coldInFlowRate'] = df_final['coldInFlowRate']/60
df_final['hotInFlowRate'] = df_final['hotInFlowRate']/60
df_final['hotOutFlowRate'] = 1
df_final['hotWaterUse'] = df_final['hotInFlowRate'] - df_final['hotOutFlowRate']

#mainFinal['hotWaterUse_fixed'] = mainFinal['hotWaterUse']


#print('Fixing hot water use...')
#for i, row in mainFinal.iterrows():
#    if row['hotWaterUse_fixed'] <0:
#        mainFinal.at[i,'hotWaterUse_fixed'] = 0

#hotInFOLD = main['hotInFlowRate'].iloc[0]
#for i, row in mainFinal.iterrows():
#    if row['hotInFlowRate'] <0.5:
#        mainFinal.at[i, 'hotInFlowRate'] = hotInFold
#    else:
#        hotInFold = row['hotInFlowRate']



print('QC Completed!\n')

"""
totalHotIn = mainFinal['hotInFlowRate'].sum()
totalHotOut = mainFinal['hotOutFlowRate'].sum()
totalColdIn = mainFinal['coldInFlowRate'].sum()
totalHotUse = mainFinal['hotWaterUse'].sum()
totalHotIn_perDay = totalHotIn /28
totalColdIn_perDay = totalColdIn /28
totalHotOut_perDay = totalHotOut /28
totalHotUse_perDay = totalHotUse /28


nonzeros = mainFinal['hotWaterUse'].astype(bool).sum(axis=0)
totalVals = mainFinal['hotWaterUse'].count()
percent = nonzeros/totalVals



source = ["type", "HOT In (gal)", "HotOut (gal)" ,  "Hot Use (gal)", "COLD In (gal)"]
A = [('TotalUse(gal)_fact', totalHotIn, totalHotOut,totalHotUse , totalColdIn),
     ('TotalUse/day', totalHotIn_perDay, totalHotOut_perDay,totalHotUse_perDay,totalColdIn_perDay)]
print(tabulate(A, headers=source))
print(nonzeros)
print(totalVals)
print(percent)

"""

print('Plotting final flowrates...')
# print final data
#Initialize figures and subplots
gridsize=(3,1)
fig=plt.figure(1,figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Final Flowrates for BLDG '+bldg, fontsize=14, weight='bold')

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(df_final['hotInFlowRate'], color='red', label='hotIn_final')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(df_final['coldInFlowRate'], color='blue', label='coldWaterUse_final')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

# 3rd row - hot return
axHotWaterUse = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotWaterUse.plot(df_final['hotWaterUse'], color='maroon', label='hotWaterUse_final')
axHotWaterUse.set_title('hotWaterUse', fontsize=10, weight ='bold')
axHotWaterUse.set_ylabel('GPM')
axHotWaterUse.set_xlim(beginDate, endDate)
#axHotWaterUse.set_ylim(-0.01, 0.05)
axHotWaterUse.grid(True)

fig.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


print('Plotting final temperatures...')
fig2=plt.figure(2,figsize=(12,8))
fig2.suptitle('Final Temps for BLDG '+bldg, fontsize=14, weight='bold')

gridsize = (2,1)
axHotTemp = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(df_final['hotInTemp'], color='red', label='hotIn_final')
axHotTemp.plot(df_final['hotOutTemp'], color='maroon', label='hotOut_final')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(df_final['coldInTemp'], color='blue', label='1-Sec HOT Data')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
fig2.show()
plt.show()

x = input('Do you want to write to database? (y/n): ').upper()

if x == 'Y':
# WritePoints
    print('Connecting to database...')
    clientdf = DataFrameClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
    clientdf.switch_database('ciws_final')
    print('Writing points...')
    clientdf.write_points(dataframe=df_final, measurement='LLC',
                            field_columns={'hotInFlowRate':df_final[['hotInFlowRate']],
                                           'coldInFlowRate': df_final[['coldInFlowRate']],
                                           'hotOutFlowRate': df_final[['hotOutFlowRate']],
                                           'hotInTemp': df_final[['hotInTemp']],
                                           'coldInTemp': df_final[['coldInTemp']],
                                           'hotOutTemp': df_final[['hotOutTemp']]
                                          # 'hotWaterUse':mainFinal[['hotWaterUse']],
                                          # 'hotWaterUse_fixed':mainFinal[['hotWaterUse_fixed']]
                                           },
                            tag_columns={'buildingID': bldg},
                            protocol='line', numeric_precision=10, batch_size=2000)

else:
    print('Better luck next time...')




print('DONE!!')

