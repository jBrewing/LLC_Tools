import pandas as pd
from influxdb import InfluxDBClient

# accept inputs
print('Receiving inputs...\n')
#    building ID
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T15:00:00Z'"
endDate = "'2019-03-23T15:00:00Z'"


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')

#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "LLC" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
#   send query
print('Retrieving data...')
main_Query = client.query(query)
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='LLC'))
main = pd.DataFrame(main_ls)
# Set dataframe index as datetime.
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

print('Data retrieved! \n')


#perform eq

# assign constants
# density of water
rho = 997 # (kg/m^3)
# specific heat of water
c = 4.186 # (J/g*C)

#convert flowrates to m^3/s


twaterUse = pd.DataFrame(columns=['hotWaterUse', 'coldWaterUse'])
energyUse = pd.DataFrame(columns=['E_HSupply', 'E_HReturn', 'E_HUse', 'E_pipe'])


# WATER USE
#   Hot use WATER
#       HotSupplyFR - HotReturnFR = HotUseFR

waterUse['hotWaterUse'] = main['hotInFlowRate'] - main['hotOutFlowRate']
#main['hotWaterUse_fixed'] = main['hotWaterUse']

print('Fixing hot water use...')
for i, row in waterUse.iterrows():
    if row['hotWaterUse'] <0:
        waterUse.at[i,'hotWaterUse'] = 0

#   Cold Use WATER
#       ColdSupplyFR = ColdUseFR
waterUse['coldWaterUse'] = main['coldInFlowRate']

# ENERGY
#   HotSupply ENERGY
#       rho * heatCap * HotSupplyFR * (HotSupplyTemp - ColdSupplyTemp)
energyUse['E_HSupply'] = rho*c*main['hotInFlowRate'](main['hotInTemp'] - main['coldInTemp'])

#   HotReturn ENERGY
#       rho * heatCap * HotReturnFR * (HotReturnTemp - ColdSupplyTemp)
energyUse['E_HRreturn'] = rho*c*main['hotOutFlowRate']*(main['hotOutTemp'] - main['coldInTemp'])

#   HotUse ENERGY
#       rho * heatCap * HotUseFR * (HotSupplyTemp - ColdSupplyTemp)
energyUse['E_HUse'] = rho*c*waterUse['hotWaterUse']*(main['hotOutTemp'] - main['coldInTemp'])

#   Pipe ENERGY loss
#       HotSupplyE - HotReturnE - HotUseE
energyUse['E_pipe'] = energyUse['E_HSupply'] - energyUse['E_HRreturn'] - energyUse['E_HUse']


#spit out results
    #tablulated
    #graphs
    #insight?

