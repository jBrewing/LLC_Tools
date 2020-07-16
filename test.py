import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/wWRE/')

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-19T12:00:00Z'"

bldgs = ['B','C','D','E','F']



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

    query = """
    SELECT "coldInTemp" 
    FROM "WaWRE" 
    WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

    print('Retrieving data...')
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    results = client.query(query)
    df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))

    # Set dataframe index as datetime.
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

