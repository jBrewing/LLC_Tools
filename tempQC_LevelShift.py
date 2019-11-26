import pandas as pd
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient
from calibrationFactor import caliFact


print('Receiving inputs...\n')
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-19T12:00:00Z'"

#beginDate2 = "'2019-04-15T12:00:00Z'"
#endDate2 = "'2019-04-19T12:00:00Z'"

#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
bldgIDInput2 = input("Input building ID: ").upper()
bldgIDQ2 = "'" + bldgIDInput2 + "'"

#hotInCal = input("Input hotIn level shift val: ")
#hotOutCal = input("Input hotOut level shift val: ")
#coldInCal = input("Input coldIn level shift val: ")
#hotInCal = float(hotInCal)
#hotOutCal = float(hotOutCal)
#coldInCal = float(coldInCal)


print('\nConnecting to database...')
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling data query...')
query = """SELECT "hotInTemp", "hotOutTemp", "coldInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data for query 1...')
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
#main['time'] = pd.DatetimeIndex(main['time']).time
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

#query = """SELECT "hotInTemp", "hotOutTemp", "coldInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

#print('Retrieving data for query 2...')
#main_Query = client.query(query)
#main_ls = list(main_Query.get_points(measurement='flow'))
#main2 = pd.DataFrame(main_ls)
#main2['time'] = pd.DatetimeIndex(main2['time']).time
#main2['time'] = pd.to_datetime(main2['time'])
#main2.set_index('time', inplace=True)





#csvpath = '/Users/joseph/Desktop/GRA/ResearchComponents/DATA/QC/Temp/tempLevelShift_allBLDGS/LLC '+bldgIDInput1+'.csv'

#mainCsv=pd.read_csv(csvpath, header=0)
#mainCsv = mainCsv[['time','LLC-Bldg'+bldgIDInput1+'-DHW-SWT','LLC-Bldg'+bldgIDInput1+'-DHW-RWT','LLC-Bldg'+bldgIDInput1+'-CHW-SWT']].copy()
#mainCsv['time'] = pd.DatetimeIndex(mainCsv['time']).time
#mainCsv['time'] = pd.to_datetime(mainCsv['time'])
#mainCsv.set_index('time', inplace=True)
#mainCsv = mainCsv.sort_index()
#mainCsv = mainCsv.convert_objects(convert_numeric=True)

#for i, row in mainCsv.iterrows():
#    a = row['LLC-Bldg'+bldgIDInput1+'-DHW-SWT']
#    b = row['LLC-Bldg'+bldgIDInput1+'-DHW-RWT']
#    c = row['LLC-Bldg'+bldgIDInput1+'-CHW-SWT']
#    x = (a - 32) * 5/9
#    y = (b - 32) * 5/9
#    z = (c - 32)* 5/9
#    mainCsv.at[i, 'LLC-Bldg'+bldgIDInput1+'-DHW-SWT'] = x
#    mainCsv.at[i, 'LLC-Bldg'+bldgIDInput1+'-DHW-RWT'] = y
#    mainCsv.at[i, 'LLC-Bldg'+bldgIDInput1+'-CHW-SWT'] = z

#mainBAS_June = mainCsv.truncate(before=pd.Timestamp('2019-06-26T03:45:00'))
#mainBAS_Oct = mainCsv.truncate(after=pd.Timestamp('2018-10-12T16:00:00'))



main2 = main.copy()

print('calculating adjusted values for bldg '+bldgIDInput1)
calibration = caliFact(bldgIDInput1)
for i, row in main.iterrows():
    a = row['hotInTemp'] + calibration[0]
    b = row['hotOutTemp'] + calibration[1]
    c = row['coldInTemp'] + calibration[2]
    main.at[i, 'hotInTemp'] = a
    main.at[i,'hotOutTemp'] = b
    main.at[i,'coldInTemp'] = c

#print('calculating adjusted values for bldg '+bldgIDInput2)
#calibration = caliFact(bldgIDInput2)
#for i, row in main2.iterrows():
#    a = row['hotInTemp'] + calibration[0]
#    b = row['hotOutTemp'] + calibration[1]
#    c = row['coldInTemp'] + calibration[2]
#    main2.at[i, 'hotInTemp'] = a
#    main2.at[i,'hotOutTemp'] = b
#    main2.at[i,'coldInTemp'] = c


#print('test')
#mainTest = pd.merge(main, mainC, on='time')

#hotIn = main['hotInTemp'].mean()
#hotOut = main['hotOutTemp'].mean()
#coldIn = main['coldInTemp'].mean()

#hotInCSV = mainC['HotTemp'].mean()
#hotOutCSV = main2['hotOutTemp'].mean()
#coldInCSV = mainC['coldTemp'].mean()

#print(hotIn, hotOut, coldIn)
#print(hotInCSV, hotOutCSV, coldInCSV)

fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main['hotInTemp'], color='red', label='hotIn_'+bldgIDInput1)
axHotTemp.plot(main2['hotInTemp'], color='maroon', label='hotIn_'+bldgIDInput2, linestyle='dashed')
#axHotTemp.plot(mainBAS_June['LLC-Bldg'+bldgIDInput1+'-DHW-SWT'], color='red', label = 'hotIn_BAS', linestyle = 'dashed')
#axHotTemp.plot(mainBAS_June['LLC-Bldg'+bldgIDInput1+'-DHW-RWT'], color='maroon', label = 'hotOut_BAS', linestyle='dashed')
axHotTemp.set_xlim(beginDate, endDate)
#axHotTemp.set_ylim(hotIn - 3, hotIn + 3)
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlabel('Date')
axHotTemp.legend(loc='lower right')
#axHotTemp.set_title('HOT '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')
axHotTemp.set_title('HOT IN '+bldgIDInput1+'  vs. '+bldgIDInput2, fontsize=10, weight ='bold')

fig2 = plt.figure(figsize=(12,8))
fig2.autofmt_xdate()
axHotOut = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotOut.plot(main['hotOutTemp'], color = 'red', label = 'hotOut'+bldgIDInput1)
axHotOut.plot(main2['hotOutTemp'], color='maroon', label = 'hotOut_'+bldgIDInput2, linestyle='dashed')
axHotOut.set_xlim(beginDate, endDate)
axHotOut.set_ylabel('Temp (C)')
axHotOut.legend(loc='lower right')
#axHotOut.set_title('COLD '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')
axHotOut.set_title('HOT OUT '+bldgIDInput1+'  vs. '+bldgIDInput2, fontsize=10, weight ='bold')

fig3 = plt.figure(figsize=(12,8))
fig3.autofmt_xdate()
axColdTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(main['coldInTemp'], color = 'blue', label = 'coldIn_'+bldgIDInput1)
axColdTemp.plot(main2['coldInTemp'], color = 'navy', label = 'coldIn_'+bldgIDInput2, linestyle='dashed')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.set_ylim(5, 25)
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.legend(loc='lower right')
#axColdTemp.set_title('COLD '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')
axColdTemp.set_title('COLD IN '+bldgIDInput1+' vs. '+bldgIDInput2, fontsize=10, weight ='bold')

fig4 = plt.figure(figsize=(12,8))
fig4.autofmt_xdate()
finalPlot = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
finalPlot.plot(main['hotInTemp'], color = 'red', label = 'hotIn_final')
finalPlot.plot(main['hotOutTemp'], color = 'maroon', label = 'hotOut_final', linestyle='dashed')
finalPlot.set_xlim(beginDate, endDate)
finalPlot.set_ylabel('Temp (C)')
finalPlot.legend(loc='lower right')
#axColdTemp.set_title('COLD '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')
finalPlot.set_title('COLD IN '+bldgIDInput1+' vs. '+bldgIDInput2, fontsize=10, weight ='bold')


#fig4 = plt.figure(figsize=(12,8))
#fig4.autofmt_xdate()
#axColdTemp_OctDL = plt.subplot()
#plt.xticks(fontsize=8, rotation=35)
#axColdTemp_OctDL.plot(mainBAS_Oct['LLC-Bldg'+bldgIDInput1+'-CHW-SWT'], color='navy', label = 'coldIn_BAS', linestyle='dashed')
##axColdTemp_OctDL.set_xlim(beginDate, endDate)
#axColdTemp_OctDL.set_ylim(5, 25)
#axColdTemp_OctDL.set_ylabel('Temp (C)')
#axColdTemp_OctDL.legend(loc='lower right')
#axColdTemp_OctDL.set_title('COLD '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

#print(hotIn)
#print(hotInCSV)


print('done')

