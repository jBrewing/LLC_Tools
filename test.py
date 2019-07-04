import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from progressbar import ProgressBar
import numpy as np

# accept inputs
print('Receiving inputs...\n')
#    building ID
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-01T12:00:00Z'"
#    temp calibration factors
pbar = ProgressBar()


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')

#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "flow" WHERE "buildingID" =""" + bldgIDQ1 + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""
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

    print('Querying data for correlation...')
    bldgIDQ2 = "'D'"
    endDate2 = "'2019-03-27T16:00:00Z'"
    # query 2nd correlation dataset and convert to dataframe
    query = """SELECT "hotInTemp" FROM "flow" WHERE "buildingID" =""" + bldgIDQ2 + """ AND time >= """ + beginDate + """ AND time <= """ + endDate2 + """"""

    main_Query = client.query(query)
    main_ls = list(main_Query.get_points(measurement='flow'))
    main2 = pd.DataFrame(main_ls)
    main2['time'] = pd.to_datetime(main2['time'])
    main2.set_index('time', inplace=True)
    main2.rename(columns ={'hotInTemp':'hotInTemp_D'}, inplace = True)

    mainHot = main['hotInTemp'].truncate(after=pd.Timestamp('2019-03-27T16:00:00Z')).copy()
    mainHot=mainHot.to_frame()
    mainHotQC = pd.merge(mainHot, main2, on='time')
    #   while loop coupled with for loop using date?
    print('calculating new values')
    for i, row in mainHotQC.iterrows():
        x = row['hotInTemp']
        y = row['hotInTemp_D']
        z = 8.25122766 + 0.8218035674 * y
        mainHotQC.at[i, 'hotInTemp'] = z

#   update original dataframe
    main['hotInTemp'].update(mainHotQC['hotInTemp'])



print('done')
