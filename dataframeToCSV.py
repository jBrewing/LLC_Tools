import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-03-29T12:00:00Z'"
endDate = "'2019-04-05T12:00:00Z'"
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


print('Assembling train data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving train data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

mainY = main

query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main2 = pd.DataFrame(main_ls)
main2['time'] = pd.to_datetime(main2['time'])
main2.set_index('time', inplace=True)
print('Train data retreived.')

#mainTrain = pd.merge(main, main2, on='time')
mainX = main2

###############
print('Assembling test query...')
beginDate2 = "'2019-03-23T12:00:00Z'"
endDate2 = "'2019-03-27T08:00:00Z'"
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
#query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate2+""" AND time <= """+endDate2+""""""


print('Retrieving test data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
#main_Query = client.query(query)
#main_ls = list(main_Query.get_points(measurement='flow'))
#main = pd.DataFrame(main_ls)
#main['time'] = pd.to_datetime(main['time'])
#main.set_index('time', inplace=True)


#query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ2+""" AND time >= """+beginDate2+""" AND time <= """+endDate2+""""""
#main_Query = client.query(query)
#main_ls = list(main_Query.get_points(measurement='flow'))
#main2 = pd.DataFrame(main_ls)
#main2['time'] = pd.to_datetime(main2['time'])
#main2.set_index('time', inplace=True)
#print('Test data retreived.')
###############

#mainTest = pd.merge(main, main2, on='time')
#mainTest = main2

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

X_train, X_test, y_train, y_test = train_test_split(mainX, mainY, test_size=1,)

lm = LinearRegression()
train = lm.fit(mainTrain['hotInTemp_y'], mainTrain['hotInTemp_x'])
predictions = lm.predict(mainTest['hotInTemp_y'])
type(train)
type(predictions)


print('plotting results')
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)

axHotTemp.plot(main3['hotInTemp_x'], color = 'grey', alpha=0.7)
axHotTemp.plot(main3['hotInTemp_y'], color = 'blue')
axHotTemp.plot(main3['adjusted_HotInTemp'], color='red')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.set_ylim(min(main3['hotInTemp_x'])-3, max(main3['hotInTemp_x'])+3)
axHotTemp.legend(loc='lower right')


#axHotTemp.plot(main3['hotInTemp_x'], main3['hotInTemp_y'])
#axHotTemp.set_xlim(min(main3['hotInTemp_x'])-2, max(main3['hotInTemp_x'])+2)
#axHotTemp.set_ylim(min(main3['hotInTemp_y'])-2, max(main3['hotInTemp_y'])+2)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()






#print('Export dataframes as CSV')
#filename = '/Users/joseph/Desktop/QC_BLDG'+bldgIDInput1+'+BLDG'+bldgIDInput2+'.csv'
#main3.to_csv(filename, header=True)

print('done!')