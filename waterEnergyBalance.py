

# 1. Import statements
import pandas as pd
from influxdb import InfluxDBClient
import numpy as np


# 2. Inputs for range of data
print('Receiving inputs...\n')
bldgID2 = "'E'"
beginDate ="'2019-02-11T00:00:00Z'"
endDate = "'2019-02-11T23:59:59Z'"


# 3. query data
#   - query ALL data
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')

print('Assembling query...')
query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgID2+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

# 4. Get data into dataframe
print('Retrieving data...\n')
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)


# 5. Flow Balance
# convert pulsed return flow output into per second value
dfLength = len(main.index)
nonZeros = main['hotOutFlowRate'].astype(bool).sum(axis=0)




returnFlow = pd.DataFrame(main['hotOutFlowRate'])
#returnFlow = returnFlow[(returnFlow.T != 0).any()]
returnFlowAvg = returnFlow.mean()
returnFlowVar = np.std(returnFlow)

main['hotOutFlowRate'] = returnFlowAvg/60
print(dfLength)
print(nonZeros)
print(returnFlowAvg)
print(returnFlowVar)








print('\ndone!')
