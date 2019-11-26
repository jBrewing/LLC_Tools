import pandas
import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T15:00:00Z'"
endDate = "'2019-04-19T15:00:00Z'"


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


bldgID = ['B','C','D','E']

# giant for loop to get all data in seperate dataframes
for x in bldgID:
    bldgIDQ1 = "'" + x + "'"
    #   write query
    print('Assembling data query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    query = """SELECT "hotInTemp", "hotOutTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
    #   send query
    print('Retrieving data...')
    main_Query = client.query(query)
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    main_Query = client.query(query)
    main_ls = list(main_Query.get_points(measurement='flow'))
    main = pd.DataFrame(main_ls)
    main['time'] = pd.to_datetime(main['time'])
    main.set_index('time', inplace=True)

    if x == 'B':
        # Adjust inaccurate BldgB temp data with correlation from Bldg D temp data
        print('Fetching data to adjust BLDG B temp data...')
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
        print('Calculating new temp values...')
        for i, row in mainHotQC.iterrows():
            x = row['hotInTemp']
            y = row['hotInTemp_D']
            z = 8.25122766 + 0.8218035674 * y
            mainHotQC.at[i, 'hotInTemp'] = z

        #   update original dataframe
        main['hotInTemp'].update(mainHotQC['hotInTemp'])
        print('New values for BLDG B calculated!\n')
        mainB = main.copy()
        mainB['hotInTemp'] = mainB['hotInTemp'] + 4.47
        mainB['hotOutTemp'] = mainB['hotOutTemp'] + 4.26

    elif x=='C':
        mainC = main.copy()
        mainC['hotInTemp'] = mainC['hotInTemp'] + 6.5
        mainC['hotOutTemp'] = mainC['hotOutTemp'] + 4.06
    elif x=='D':
        mainD = main.copy()
        mainD['hotInTemp'] = mainD['hotInTemp'] + 3.5
        mainD['hotOutTemp'] = mainD['hotOutTemp'] + 1.99
    else:
        mainE = main.copy()
        mainE['hotInTemp'] = mainE['hotInTemp'] + 5.79



#calculate means
xB = (mainB['hotInTemp'] - mainC['hotOutTemp']).mean()
xC = (mainC['hotInTemp'] - mainC['hotOutTemp']).mean()
xD = (mainD['hotInTemp'] - mainD['hotOutTemp']).mean()
xBCD = (xB +xC +xD)/3

# goalseek code
factor = 0
while True:
    factor = factor + 0.01
    mainE['hotOutTemp_new'] = mainE['hotOutTemp']+factor
    xE = (mainE['hotInTemp'] - mainE['hotOutTemp_new']).mean()
    xDiff = xE - xBCD
    mainE['hotOutTemp_new'] = mainE['hotOutTemp']
    print(factor,xDiff,xE,xBCD)
    if abs(xDiff)<0.011:
        break

factor = str(factor)
print('adjustment factor = ' +factor)

fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(mainE['hotInTemp'], color='red', label='hotIn_E')
axHotTemp.plot(mainE['hotOutTemp_new'], color='maroon', label='hotIn_E', linestyle='dashed')
#axHotTemp.plot(mainBAS_June['LLC-Bldg'+bldgIDInput1+'-DHW-SWT'], color='red', label = 'hotIn_BAS', linestyle = 'dashed')
#axHotTemp.plot(mainBAS_June['LLC-Bldg'+bldgIDInput1+'-DHW-RWT'], color='maroon', label = 'hotOut_BAS', linestyle='dashed')
axHotTemp.set_xlim(beginDate, endDate)
#axHotTemp.set_ylim(hotIn - 3, hotIn + 3)
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlabel('Date')
axHotTemp.legend(loc='lower right')
#axHotTemp.set_title('HOT '+bldgIDInput1+' BAS csv vs. '+bldgIDInput1+' Datalogger', fontsize=10, weight ='bold')
axHotTemp.set_title('HotIn_E  vs. HotOut_E', fontsize=10, weight ='bold')






print('Done! \n')
