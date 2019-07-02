import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-06-10T08:23:00Z'"
endDate = "'2019-06-20T07:05:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDInput2 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
#bldgIDQ2 = "'" + bldgIDInput2 + "'"


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
#query = """SELECT "hotInFlowRate","coldInFlowRate", "hotOutFlowRate"  FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
query = """SELECT *  FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)



print('Export dataframes as CSV')
filename = '/Users/joseph/Desktop/QC_BLDG_'+bldgIDInput1+'.csv'
#filename = '/Users/joseph/Desktop/QC_BLDG'+bldgIDInput1+'+BLDG'+bldgIDInput2+'.csv'
main.to_csv(filename, header=True)

print('done!')