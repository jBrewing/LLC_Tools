import pandas as pd
from influxdb import InfluxDBClient

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-05T12:00:00Z'"

bldgs = ['C', 'D', 'E', 'F']
blgds = 'C'


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')

for bldg in bldgs:
    #   write query
    print('Assembling data query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    bldgID = "'" + bldg + "'"
    query = """SELECT * FROM "WaWRE" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
    #   send query
    print('Retrieving data...')
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    results = client.query(query)
    df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))

    # Set dataframe index as datetime.
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    print('Data retrieved! \n')

    hour = df.groupby(df.index.hour).sum()# Group by timestamp index into day
    day = df.groupby(df.index.weekday).sum()
    week = df.groupby(df.index.week).sum()




    print('done')
