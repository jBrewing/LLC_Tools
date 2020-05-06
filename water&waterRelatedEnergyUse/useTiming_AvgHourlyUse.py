import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-19T12:00:00Z'"

bldgs = ['B','C', 'D', 'E', 'F']



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
    SELECT "buildingID", "coldWaterUse", "hotWaterUse", "hotUse_energy","pipeLoss_energy"  
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

    print('Data retrieved! \n')

    print('Resampling data...')

    df['diff'] = df['hotUse_energy'] + df['pipeLoss_energy']
    hour = df.groupby(df.index.hour).sum() # Group by timestamp index into day
    day = df.groupby(df.index.weekday).sum()
    week = df.groupby(df.index.week).sum()

    time_hour = ['1am', '2am', '3am', '4am', '5am', '6am', '7am', '8am', '9am', '10am', '11am', '12pm',
            '1pm', '2pm', '3pm', '4pm', '5pm', '6pm', '7pm', '8pm', '9pm', '10pm', '11pm', '12am']
    time_day = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun'] # reindex is [3,4,5,6,0,1,2]


    # print final data
    print('Plotting final flowrates...')

    fig1, ax1 = plt.subplots(figsize=(14,8))

    ax1.plot(time_hour, hour['hotUse_energy'], '--', color='darkorange')
    #ax1.plot(time_hour, hour['diff'], '--', color='black')
    #ax1.fill_between(time_hour, hour['hotUse_energy'],hour['diff'], color='black')
    ax1.fill_between(time_hour, 0, hour['hotUse_energy'], color='darkorange', alpha=0.75)
    ax1.set_ylabel('Energy Use (MJ)', fontsize=14)
    ax1.set_xlim('1am','12am')
    ax1.set_ylim(0,)
    ax1.grid(which='both', axis='x', color='grey', linewidth='1', alpha=0.5)
    ax1.set_xlabel('Time', fontsize=14)
    ax1.set_title('Avg. Hourly Water & Water Related Energy Use: Building '+bldg)
    ax1.legend()
    plt.tick_params(labelsize='large')
    plt.xticks(rotation=35)


    ax2 = ax1.twinx()

    ax2.plot(time_hour, hour['hotWaterUse'],'-', color='red', linewidth=3, zorder =1)
    ax2.plot(time_hour, hour['coldWaterUse'],'-', color='blue', linewidth=3, zorder=2)
    ax2.set_ylabel('Water Use (m^3)', fontsize=14)
    ax2.set_ylim(0,)
    plt.tick_params(labelsize='large')
    ax2.legend()


    fig1.tight_layout()
    fig1.show()
    """
    
    fig2, ax1 = plt.subplots(figsize=(14,8))
    
    ax1.plot(time_day, day['hotUse_energy'], '--', color='darkorange')
    ax1.plot(time_day, day['diff'], '--', color='black')
    ax1.fill_between(time_day, day['hotUse_energy'],day['diff'], color='black')
    ax1.fill_between(time_day, 0, day['hotUse_energy'], color='darkorange', alpha=0.75)
    ax1.set_ylabel('Energy Use (MJ)', fontsize=14)
    ax1.set_xlim('Mon','Sun')
    ax1.set_ylim(0,3300)
    ax1.grid(which='both', axis='x', color='grey', linewidth='1', alpha=0.5)
    ax1.set_xlabel('Time', fontsize=14)
    plt.tick_params(labelsize='large')
    plt.xticks(rotation=35)
    
    ax2 = ax1.twinx()
    
    ax2.plot(time_day, day['hotWaterUse'],'-', color='red', linewidth=3, zorder =1)
    ax2.plot(time_day, day['coldWaterUse'],'-', color='blue', linewidth=3, zorder=2)
    ax2.set_ylabel('Water Use (m^3)', fontsize=14)
    ax2.set_ylim(0,)
    plt.tick_params(labelsize='large')
    
    
    
    fig1.tight_layout()
    fig2.show()
    
    """
    plt.show()


print('done')
