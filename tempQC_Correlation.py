import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-03-23T12:00:00Z'"
endDate = "'2019-03-29T08:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDInput2 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
bldgIDQ2 = "'" + bldgIDInput2 + "'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

# Run 2nd query for other buildings data
query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main2 = pd.DataFrame(main_ls)
main2['time'] = pd.to_datetime(main2['time'])
main2.set_index('time', inplace=True)
print('data retreived.')

# merge two dataframes into one, using time as the index.
mainTest = pd.merge(main, main2, on='time')
mainTest = mainTest.truncate(after=pd.Timestamp('2019-03-27T13:56:00'))

# truncate Main dataset for testing.  Trim to 100 records
mainTest2 = mainTest.truncate(after=pd.Timestamp('2019-03-23T12:02:00'))
mainTestOrig = mainTest2

print('calculating new values')
# loop through rows with iterrows(), checking BHotInTemp against some value
#   if BHotInTemp doesnt pass test, change with regression equation and insert into row.
for i, row in mainTest.iterrows():
    x = row['hotInTemp_x']
    y = row['hotInTemp_y']
    z = 8.25122766 + 0.8218035674*y
    mainTest.at[i, 'hotInTemp_x'] = z

#for i, row in mainTest.iterrows():
#    x = row['hotInTemp_x']
#    y = row['hotInTemp_y']
#    if x < 46:
 #       z = 8.25122766 + 0.8218035674*y
#    else:
 #       z = x
 #   mainTest.at[i, 'hotInTemp_x'] = z



# visualize results and regression line to check
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
#axHotTemp.scatter(mainTest['hotInTemp_x'], mainTest['hotInTemp_y'], color='red')
#axHotTemp.plot(mainTest['hotInTemp_x'], mainTest['hotInTemp_y'], color='blue')
axHotTemp.plot(mainTest['hotInTemp_x'], color='red')
axHotTemp.plot(mainTest['hotInTemp_y'], color='blue')
#axHotTemp.set_xlim(min(mainTest['hotInTemp_x'])-2, max(mainTest['hotInTemp_x'])+2)
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.set_ylim(min(mainTest['hotInTemp_y'])-2, max(mainTest['hotInTemp_y'])+2)


plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()

print('done!')