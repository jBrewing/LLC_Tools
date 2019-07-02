import pandas as pd
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient


print('Receiving inputs...\n')
beginDate = "'2019-06-26T03:45:00Z'"
endDate = "'2019-06-27T11:30:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"

hotInCal = input("Input hotIn level shift val: ")
hotOutCal = input("Input hotOut level shift val: ")
coldInCal = input("Input coldIn level shift val: ")
hotInCal = float(hotInCal)
hotOutCal = float(hotOutCal)
coldInCal = float(coldInCal)


print('\nConnecting to database...')
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling data query...')
query = """SELECT "hotInTemp", "hotOutTemp", "coldInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)





csvpath = '/Users/joseph/Desktop/GRA/DATA/QC/Temp/LLC '+bldgIDInput1+'.csv'

mainCsv=pd.read_csv(csvpath, header=0)
mainC = mainCsv[['time','LLC-Bldg'+bldgIDInput1+'-DHW-SWT','LLC-Bldg'+bldgIDInput1+'-DHW-RWT','LLC-Bldg'+bldgIDInput1+'-CHW-SWT']].copy()
mainC['time'] = pd.to_datetime(mainC['time'])
mainC.set_index('time', inplace=True)
mainC = mainC.sort_index()
mainC = mainC.convert_objects(convert_numeric=True)

mainC = mainC.truncate(before=pd.Timestamp('2019-06-26T03:45:00'))


for i, row in main.iterrows():
    a = row['hotInTemp']
    b = row['hotOutTemp']
    c = row['coldInTemp']
    x = (a * 9/5)+32+hotInCal
    y = (b * 9/5)+32+hotOutCal

    z = (c * 9 / 5) + 32- coldInCal
    main.at[i, 'hotInTemp'] = x
    main.at[i,'hotOutTemp'] = y
    main.at[i,'coldInTemp'] = z

print('test')
#mainTest = pd.merge(main, mainC, on='time')

hotIn = main['hotInTemp'].mean()
hotOut = main['hotOutTemp'].mean()

hotInCSV = mainC['LLC-Bldg'+bldgIDInput1+'-DHW-SWT'].mean()
hotOutCSV = mainC['LLC-Bldg'+bldgIDInput1+'-DHW-RWT'].mean()

print(hotIn, hotOut)
print(hotInCSV, hotOutCSV)

fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(main['hotInTemp'], color='red', label='DL - supply')
axHotTemp.plot(main['hotOutTemp'], color='maroon', label='DL - return')
axHotTemp.plot(mainC['LLC-Bldg'+bldgIDInput1+'-DHW-SWT'], color='red', label = 'BAS, Supply', linestyle='dashed')
axHotTemp.plot(mainC['LLC-Bldg'+bldgIDInput1+'-DHW-RWT'], color='maroon', label = 'BAS, Return', linestyle='dashed')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.set_ylabel('Temp (F)')
axHotTemp.set_xlabel('Date')
axHotTemp.legend(loc='lower right')
axHotTemp.set_title('HOT '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')

fig2 = plt.figure(figsize=(12,8))
fig2.autofmt_xdate()
axColdTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(main['coldInTemp'], color = 'blue', label = 'DL-supply')
axColdTemp.plot(mainC['LLC-Bldg'+bldgIDInput1+'-CHW-SWT'], color='blue', label = 'BAS, supply', linestyle='dashed')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.set_ylabel('Temp (F)')
axColdTemp.legend(loc='lower right')
axColdTemp.set_title('COLD '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

print(hotIn)
print(hotInCSV)


print('done')

