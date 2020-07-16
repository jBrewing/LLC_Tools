# Title: hydroshare_waterEnergyBalance.py
# Author: Joseph Brewer
# Last updates: 07/15/2020
# Desc: This script visualizes water and water-related energy use calculated from QUALITY CONTROLLED
#       high-resolution smart meter data.  Specifically, this script calculates average hourly water use
#       and a breakdown of average hourly water-related energy use.
#
#   External libraries required:
        # Pandas, influxdb, and matplotlib

import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D



# Todo: update creds here
host = 'Your-hostname-here'
port = 8086
username = 'Your-username-here'
password = 'Your-password-here'
measurement = 'Your-final-data-measurement-here'
database = 'Your-final-database-here'
measurement_WRE = 'Your-final-water/water-related-energy-measurement-here'

# accept inputs
print('Receiving inputs...\n')
bldgs = ['B', 'C', 'D', 'E', 'F']  # create list of building ID
begin = "'2019-03-22T12:00:00Z'"  # POR start date
end = "'2019-04-19T12:00:00Z'"  # POR end date


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host=host,
                        port=port,
                        username=username,
                        password=password)
client.switch_database(database)

for bldg in bldgs:
    #   write query
    print('Assembling data query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    bldgID = "'" + bldg + "'"

    query = """SELECT *
               FROM "WaWRE" 
               WHERE "buildingID" ="""+bldgID+""" AND time >= """+begin+""" AND time <= """+end+""""""

    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    results = client.query(query)                                         # send query
    df = pd.DataFrame(list(results.get_points(measurement=measurement)))  # convert to pandas df with list + get points
    df['time'] = pd.to_datetime(df['time'])                               # Convert time to pandas datetime format
    df.set_index('time', inplace=True)                                    # Set time as index
    print('Data retrieved! \n')

    print('Resampling data...')
    hour = df.resample('1H').sum()                  # resample data to one hour interval
    hour = hour.groupby(hour.index.hour).mean()     # Group by timestamp index into hour to form average day

    energy = hour.copy()                                                            # create energy dataframe to simplify plotting
    energy['diff_HS-pipe'] = energy['hotSupply_energy'] - energy['pipeLoss_energy'] # build layered viz by calculating
    energy['diff_HS-WW'] = energy['diff_HS-pipe'] - energy['hotUse_energy']         # tiered WRE use

    # create x axis time.  Groupby function stores as [1,2,3,4,..etc]
    time_hour = ['1am', '2am', '3am', '4am', '5am', '6am', '7am', '8am', '9am', '10am', '11am', '12pm',
                 '1pm', '2pm', '3pm', '4pm', '5pm', '6pm', '7pm', '8pm', '9pm', '10pm', '11pm', '12am']


    # print final data
    print('Plotting final flowrates...')

    fig1, ax1 = plt.subplots(figsize=(14,8))

    ax1.plot(time_hour, hour['hotWaterUse'], color='red', label = 'Hot Water Use', linewidth=3)
    ax1.plot(time_hour, hour['coldWaterUse'], color='blue', label = 'Cold Water Use', linewidth=3)
    ax1.set_ylabel('Water Use ($m^3$)', fontsize=18)
    ax1.set_xlim('1am','12am')
    ax1.set_ylim(0,0.4)
    ax1.set_xlim('1am', '12am')
    ax1.grid(which='both', axis='x', color='grey', linewidth='1', alpha=0.5)
    ax1.set_xlabel('Time', fontsize=18)
    plt.tick_params(labelsize='large')
    plt.xticks(fontsize=14, rotation=35)
    plt.yticks(fontsize=14)

    plt.text('2am',0.35, bldg, fontsize = 30, fontweight='bold')



    fig1.tight_layout()
    plt.show()

    fig2, ax2 = plt.subplots(figsize=(14,8))

    ax2.plot(time_hour, energy['hotSupply_energy'], color='red', linewidth=4)
    ax2.plot(time_hour, energy['diff_HS-pipe'],color='black')
    #ax2.plot(time_hour, energy['diff_HS-WW'], color='darkorange')
    ax2.plot(time_hour, energy['hotReturn_energy'], color='maroon')
    ax2.fill_between(time_hour, energy['diff_HS-pipe'],energy['hotSupply_energy'], color='black', alpha=0.75)
    ax2.fill_between(time_hour, energy['diff_HS-pipe'],energy['hotReturn_energy'],  color='darkorange', alpha=0.75)
    ax2.fill_between(time_hour, 0, energy['hotReturn_energy'], color = 'maroon', alpha=0.75)
    ax2.set_ylabel('Energy Use (MJ)', fontsize=18)
    ax2.set_ylim(0,250)
    ax2.set_xlim('1am', '12am')
    ax2.grid(which='both', axis='x', color='grey', linewidth='1', alpha=0.5)
    ax2.set_xlabel('Time', fontsize=18)
    plt.tick_params(labelsize='large')
    plt.xticks(fontsize=14,rotation=35)
    plt.yticks(fontsize=14)

    plt.text('2am', 225, bldg, fontsize=30, fontweight='bold')

    legend_elements = [Line2D([0], [0], color = 'red', lw=2, label='Energy supplied to building'),
                       Patch(facecolor='black', edgecolor='grey', label='Energy lost to pipe'),
                       Patch(facecolor='darkorange', edgecolor='grey', label = 'Water-related energy use'),
                       Patch(facecolor='maroon', edgecolor='grey', label='Energy returned to boilers')]
    ax2.legend(handles = legend_elements,loc=4,fontsize=14)
    fig2.tight_layout()


    plt.show()






print('done')
