# Title: hydroshare_waterEnergyBalance.py
# Author: Joseph Brewer
# Last updates: 07/15/2020
# Desc: This script calculates water and water-related energy use from QUALITY CONTROLLED
#       high-resolution smart meter data.  The justifications for the assumptions and derived equations
#       are detailed in the related text (Brewer et al., 2020)
#
#   External libraries required:
        # Pandas, influxdb, and matplotlib


import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt


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


for bldg in bldgs:

    bldgID ="'"+bldg+"'"

    # Retrieve data
    print('\nConnecting to database...')
    # Create client object with InfluxDBClient library
    client = InfluxDBClient(host=host,
                            port=port,
                            username=username,
                            password=password)
    client.switch_database(database) # Set database.

    #  write query
    print('Assembling data query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
    # Query returns a 'ResultSet" type.

    # Todo: change measurement name
    query = """SELECT * 
            FROM "Your-Measurement-Here" 
            WHERE "buildingID" ="""+bldgID+""" AND time >= """+begin+""" AND time <= """+end+""""""

    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    results = client.query(query)                                         # send query
    df = pd.DataFrame(list(results.get_points(measurement=measurement)))  # convert to pandas df with list + get points
    df['time'] = pd.to_datetime(df['time'])                               # Convert time to pandas datetime format
    df.set_index('time', inplace=True)                                    # Set time as index
    print('Data retrieved! \n')


    #perform eq
    print('Calculating water use and water-related energy use...')
    # assign constants
    rho = 997 # (kg/m^3) # density of water
    c = 4.186 # (J/g*C) # specific heat of water

    # create dataframes
    use = df[['hotInFlowRate', 'coldInFlowRate', 'hotOutFlowRate']].copy()
    use.rename(columns={'hotInFlowRate': 'hotInSupply',
                        'coldInFlowRate':'coldWaterUse',
                        'hotOutFlowRate':'hotOutReturn'}, inplace=True)

    # convert flowrates to m^3/pulse
    columns = ['hotInSupply', 'coldWaterUse', 'hotOutReturn']
    for column in columns:
        use[column] = use[column] * 0.00378541  # 1 gal = 0.00378541 m^3

    # WATER USE
    #   Hot use WATER
    #       HotSupplyFR - HotReturnFR = HotUseFR
    use['hotWaterUse'] = use['hotInSupply'] - use['hotOutReturn']


    # ENERGY:
    # EQ: density * heat capacity of water * volume * change in temperature

    # HotSupply ENERGY  rho * heatCap * HotSupplyVol * (HotSupplyTemp - ColdSupplyTemp)
    use['hotSupply_energy'] = (rho*c*1000*use['hotInSupply']*(df['hotInTemp'] - df['coldInTemp'])) /1000000# use megaJoules

    # HotReturn ENERGY  rho * heatCap * HotReturnVol * (HotReturnTemp - ColdSupplyTemp)
    use['hotReturn_energy'] = (rho*c*1000*use['hotOutReturn']*(df['hotOutTemp'] - df['coldInTemp']))/1000000 # use megaJoules

    # HotUse ENERGY  rho * heatCap * HotUseVol * (HotSupplyTemp - ColdSupplyTemp)
    use['hotUse_energy'] = (rho*c*1000*use['hotWaterUse']*(df['hotOutTemp'] - df['coldInTemp']))/1000000 # use megaJoules

    # Pipe ENERGY loss  HotSupplyE - HotReturnE - HotUseE
    use['pipeLoss_energy'] = (use['hotSupply_energy'] - use['hotReturn_energy'] - use['hotUse_energy'])# use megaJoules

    use['buildingID'] = bldg

    print('Water use and water-related energy use calculated! \n')



    # print final data
    print('Plotting final flowrates...')
    #Initialize figures and subplots
    gridsize=(2,1)
    fig=plt.figure(1,figsize=(12,8))
    fig.autofmt_xdate()
    fig.suptitle('Water Use for BLDG '+bldgID, fontsize=14, weight='bold')

     # 1st row - hot use
    axHotUse = plt.subplot2grid(gridsize, (0,0))
    plt.xticks(fontsize=8, rotation=35)
    axHotUse.plot(use['hotWaterUse'], color='red')
    axHotUse.set_title('Hot Water Use', fontsize=10, weight ='bold')
    axHotUse.set_ylabel('m^3')
    axHotUse.grid(True)

    # 2nd row - cold in
    axColdUse= plt.subplot2grid(gridsize, (1,0))
    plt.xticks(fontsize=8, rotation=35)
    axColdUse.plot(use['coldWaterUse'], color='blue')
    axColdUse.set_title('Cold Water Use', fontsize=10, weight ='bold')
    axColdUse.set_ylabel('m^3')
    axColdUse.grid(True)

    fig.show()
    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


    print('Plotting final temperatures...')
    fig2=plt.figure(2,figsize=(12,8))
    fig2.suptitle('Water Related Energy Use for BLDG '+bldgID, fontsize=14, weight='bold')

    gridsize = (2,1)
    axUseLose= plt.subplot2grid(gridsize, (0,0))
    plt.xticks(fontsize=8, rotation=35)
    axUseLose.plot(use['hotUse_energy'], color='darkorange', label='Energy USED')
    axUseLose.plot(use['pipeLoss_energy'], color = 'black', linewidth=0.5, label = 'Energy LOST')
    axUseLose.set_title('Energy USE/LOST', fontsize=10, weight ='bold')
    axUseLose.set_ylabel('MJ')
    #axUseLose.set_ylim(0, 2.75)
    axUseLose.grid(True)
    axUseLose.legend()

    axSupplyReturn= plt.subplot2grid(gridsize, (1,0))
    plt.xticks(fontsize=8, rotation=35)
    axSupplyReturn.plot(use['hotSupply_energy'], color='red', label = 'HotSupply Energy')
    axSupplyReturn.plot(use['hotReturn_energy'], color='maroon', label = 'HotReturn Energy')
    axSupplyReturn.set_title('hot water SUPPLY/RETURN energy ', fontsize=10, weight ='bold')
    axSupplyReturn.set_ylabel('MJ')
    axSupplyReturn.set_ylim(0,2.75)
    axSupplyReturn.grid(True)
    axSupplyReturn.legend()

    fig2.show()
    plt.show()


    x = input('Do you want to write to database? (y/n): ').upper()

    if x == 'Y':
    # WritePoints
        print('Connecting to database...')
        clientdf = DataFrameClient(host=host,
                                   port=port,
                                   username=username,
                                   password=password)
        clientdf.switch_database(database)
        print('Writing points...')
        clientdf.write_points(dataframe=use, measurement=measurement_WRE,
                                field_columns={'hotWaterUse':use[['hotWaterUse']],
                                               'coldWaterUse': use[['coldWaterUse']],
                                               'hotUse_energy': use[['hotUse_energy']],
                                               'hotSupply_energy': use[['hotSupply_energy']],
                                               'hotReturn_energy': use[['hotReturn_energy']],
                                               'pipeLoss_energy': use[['pipeLoss_energy']]
                                               },
                                tag_columns={'buildingID': use[['buildingID']]},
                                protocol='line', numeric_precision=10, batch_size=2000)

    else:
        print('Better luck next time...')


print('done')