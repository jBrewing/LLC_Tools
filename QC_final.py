import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
from progressbar import ProgressBar
pbar = ProgressBar()

# accept inputs
print('Receiving inputs...\n')
#    building ID
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-03-29T12:00:00Z'"
#    temp calibration factors
hotInCal = input("Input hotIn level shift val: ")
hotOutCal = input("Input hotOut level shift val: ")
coldInCal = input("Input coldIn level shift val: ")
hotInCal = float(hotInCal)
hotOutCal = float(hotOutCal)
coldInCal = float(coldInCal)


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


# tempQC_correlation: BLDG B
if bldgIDInput1 == 'B':
    print ('Fetching data to adjust BLDG B temp data...')
    bldgIDQ2 = "'D'"
    endDate2 = "'2019-03-27T16:00:00Z'"
    # query 2nd correlation dataset and convert to dataframe
    query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" =""" + bldgIDQ2 + """ AND time >= """ + beginDate + """ AND time <= """ + endDate2 + """"""

    main_Query = client.query(query)
    main_ls = list(main_Query.get_points(measurement='flow'))
    main2 = pd.DataFrame(main_ls)
    main2['time'] = pd.to_datetime(main2['time'])
    main2.set_index('time', inplace=True)
    print('Adjustment data retrieved.\n')
    main2.rename(columns={'hotInTemp': 'hotInTemp_D'}, inplace=True)

    mainHot = main['hotInTemp'].truncate(after=pd.Timestamp('2019-03-27T16:00:00Z')).copy()
    mainHot = mainHot.to_frame()
    mainHotQC = pd.merge(mainHot, main2, on='time')
    #   while loop coupled with for loop using date?
    print('Calculating new values...')
    for i, row in mainHotQC.iterrows():
        x = row['hotInTemp']
        y = row['hotInTemp_D']
        z = 8.25122766 + 0.8218035674 * y
        mainHotQC.at[i, 'hotInTemp'] = z

    #   update original dataframe
    main['hotInTemp'].update(mainHotQC['hotInTemp'])
    print('New values for BLDG B calculated!\n')

hotInTOLD = main['hotInTemp'].iloc[0]
hotOutTOLD = main['hotOutTemp'].iloc[0]
coldInTOLD = main['coldInTemp'].iloc[0]
hotInFOLD = main['hotInFlowRate'].iloc[0]
hotOutFOLD = main['hotOutFlowRate'].iloc[0]

testmean = main['hotInTemp'].mean()
teststd = main['hotInTemp'].std()
print('QCing rest of data...')
# giant for loop
for i, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
    coldInTemp = row['coldInTemp']
    hotInFlow = row['hotInFlowRate']
    hotInTemp = row['hotInTemp']
    hotOutFlow = row['hotOutFlowRate']
    hotOutTemp = row['hotOutTemp']



#    coldInFlow = row ['coldInFlow']

# tempQC_levelshift
#  add calibration
    hotInTemp = hotInTemp + hotInCal
    hotOutTemp = hotOutTemp + hotOutCal
    coldInTemp = coldInTemp - coldInCal
    main.at[i, 'hotInTemp'] = hotInTemp
    main.at[i, 'hotOutTemp'] = hotOutTemp
    main.at[i, 'coldInTemp'] = coldInTemp

#   eliminate_extreme_vals
    if (hotInTOLD-hotInTemp) > 5:
        hotInTemp = hotInTOLD
        main.at[i, 'hotInTemp']= hotInTemp

#   eliminate_0s_hotFlow
  #  if hotInFlow < hotOutFlow:
  #      hotInFlow = hotOutFlow
  #      main.at[i, 'hotInFlowRate'] = hotInFlow
# store current loop values for next iteration
    hotInTOLD = hotInTemp
 #   hotOutTOLD = hotOutTemp
 #   coldInTOLD = coldInTemp
 #   hotInFOLD = hotInFlow
 #   hotOutFOLD = hotOutFlow
 #   coldInFOLD = coldInFlow

# end of giant for loop

print('QC Completed!\n')



print('Plotting final flowrates...')
# print final data
#Initialize figures and subplots
gridsize=(3,1)
fig=plt.figure(1,figsize=(12,8))
fig.autofmt_xdate()
fig.suptitle('Final Flowrate for BLDG '+bldgIDInput1, fontsize=14, weight='bold')

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(main['hotInFlowRate'], color='red', label='1-Sec HOT Data')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(main['coldInFlowRate'], color='blue', label='1-Sec HOT Data')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

# 3rd row - hot return
axHotReturnFlow = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotReturnFlow.plot(main['hotOutFlowRate'], color='maroon', label='1-Sec HOT Data')
axHotReturnFlow.set_title('hot out flowrate', fontsize=10, weight ='bold')
axHotReturnFlow.set_ylabel('GPM')
axHotReturnFlow.set_xlim(beginDate, endDate)
axHotReturnFlow.grid(True)

fig.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


print('Plotting final temperatures...')
fig2=plt.figure(2,figsize=(12,8))
fig2.suptitle('Final Temps for BLDG '+bldgIDInput1, fontsize=14, weight='bold')

axHotTemp = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main['hotInTemp'], color='red', label='1-Sec HOT Data')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(main['coldInTemp'], color='blue', label='1-Sec HOT Data')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)


axHotReturnTemp = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotReturnTemp.plot(main['hotOutTemp'], color='maroon', label='1-Sec HOT Data')
axHotReturnTemp.set_title('hot out temp', fontsize=10, weight ='bold')
axHotReturnTemp.set_ylabel('Temp (C)')
axHotReturnTemp.set_xlim(beginDate, endDate)
axHotReturnTemp.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
fig2.show()

x = input('Do you want to write to database? (y/n):  ')

if x == 'Y':
# WritePoints
    clientdf = DataFrameClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
    clientdf.switch_database('ciws_final')
    clientdf.write_points(dataframe=main, measurment='LLC',
                            field_columns={'hotInFlowRate':main[['hotInFlowRate']],
                                           'coldInFlowRate': main[['coldInFlowRate']],
                                           'hotOutFlowRate': main[['hotOutFlowRate']],
                                           'hotInTemp': main[['hotInTemp']],
                                           'coldInTemp': main[['coldInTemp']],
                                           'hotOutTemp': main[['hotOutTemp']]},
                            tag_columns={'buildingID': main[['buildingID']]},
                            protocol='line', numeric_precision=10, batch_size=2000)

else:
    print('Better luck next time...')




print('DONE!!')

