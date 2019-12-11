import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt



filepath='/Users/joseph/Desktop/GRA/ResearchComponents/DATA/QC/Flow/NoiseReduction/AggData/1day_midnight-midnight/'



print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
#PORStart = "'2019-03-22T15:00:00Z'"
#POREnd = "'2019-04-19T15:00:00Z'"
#beginDate = PORStart
#endDate = POREnd
beginDate = "'2019-03-23T00:00:00Z'"
endDate = "'2019-03-24T00:00:00Z'"
#endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
bldgID = input("Input building ID: ").upper()
bldgID = "'" + bldgID + "'"


#testDateBegin ="'2019-01-29T00:00:00Z'"
#testDateEnd = "'2019-02-05T23:59:59Z'"
#beginDate = testDateBegin
#endDate = testDateEnd

print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""
#query = """SELECT * FROM "flow" WHERE "buildingID" ="""+bldgID+""""""

print('Retrieving data...')
# Convert returned ResultSet to Pandas dataframe with list
# and get_points.
# Set dataframe index as datetime.
main_Query = client.query(query)
main_ls = list(main_Query.get_points(measurement='flow'))
main = pd.DataFrame(main_ls)
main['time'] = pd.to_datetime(main['time'])
main.set_index('time', inplace=True)

coldInFlow_Sum = 0
hotInFlow_Sum = 0
#hotInFlow_Sum_NEW = 0
counter = 0
for y, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
    coldInFlow = row['coldInFlowRate']
    hotInFlow = row['hotInFlowRate']
#    hotInNEWFlow = row['NEWhotInFlowRate']
    hotOutFlow = row['hotOutFlowRate']
    if hotOutFlow == 0:
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
#        hotInFlow_Sum_NEW = hotInFlow_Sum_NEW + hotInNEWFlow
        counter = counter + 1

    elif hotOutFlow != 0:
        counter = counter + 1
        coldInFlow_Sum = coldInFlow_Sum + coldInFlow
        hotInFlow_Sum = hotInFlow_Sum + hotInFlow
#        hotInFlow_Sum_NEW = hotInFlow_Sum_NEW + hotInNEWFlow

        main.at[y, 'coldInFlowRate'] = coldInFlow_Sum
        main.at[y, 'hotInFlowRate'] = hotInFlow_Sum
#        main.at[y, 'NEWhotInFlowRate'] = hotInFlow_Sum_NEW

#        hotInFlow_Sum_NEW = 0
        coldInFlow_Sum = 0
        hotInFlow_Sum = 0
        counter = 0

# Copy main where hotOutFlowRate != 0, replace pulse counts with 1 (1 pulse = 1 gal)
mainFinal = main[(main['hotOutFlowRate'] != 0)]
mainFinal['hotOutFlowRate'] = 1

# convert flowrates to gps, each second obs = amount of flow for that second
mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate'] / 60
mainFinal['coldInFlowRate'] = mainFinal['coldInFlowRate'] / 60

pi = 3.1415926
dt = 1 #second
fc = [0.05, 0.04, 0.03, 0.02, 0.01, 0.009, 0.008, 0.007, 0.006, 0.005]

results = pd.DataFrame(columns=['a', 'hotSum', 'NEWhotSum', 'diffHot', 'coldSum', 'NEWcoldSum', 'diffCold'], index = fc)

y = 1
z=1
for x in fc:
    print('reducing noise...\n')
    a = (2 * pi * dt * x) / (2 * pi * dt * x + 1)
    for i, row in mainFinal.iterrows():
        y = a * row['hotInFlowRate'] + (1-a) * y
        z = a * row['coldInFlowRate'] + (1 - a) * z
        mainFinal.at[i, 'NEWhotInFlowRate'] = y
        mainFinal.at[i, 'NEWcoldInFlowRate'] = z


    mainFinal['hotWaterUse'] = mainFinal['hotInFlowRate'] - mainFinal['hotOutFlowRate']
    mainFinal['hotWaterUse_NEW'] = mainFinal['NEWhotInFlowRate'] - mainFinal['hotOutFlowRate']

    # Calculate sum of hotIn flowrate without adjustment for control value
    hotSum = mainFinal['hotInFlowRate'].sum()
    coldSum = mainFinal['coldInFlowRate'].sum()

    #iteratvely calculate NEW hotIn flowrate to compare impact of adjustment with control
    NEWhotSum = mainFinal['NEWhotInFlowRate'].sum()
    NEWcoldSum = mainFinal['NEWcoldInFlowRate'].sum()

    diffHot = str((hotSum-NEWhotSum)/hotSum * 100)
    diffCold = str((coldSum - NEWcoldSum) / coldSum * 100)

    #update results
    results.at[x, 'a'] = a
    results.at[x, 'hotSum'] = hotSum
    results.at[x, 'NEWhotSum'] = NEWhotSum
    results.at[x, 'diffHot'] = diffHot + ' %'
    results.at[x, 'coldSum'] = coldSum
    results.at[x, 'NEWcoldSum'] = NEWcoldSum
    results.at[x, 'diffCold'] = diffCold + ' %'

    x = str(x)
    a = str(a)

    print('Plotting final flowrates...')
    gridsize=(2,1)
    fig=plt.figure(1,figsize=(12,8))
    fig.autofmt_xdate()
    fig.suptitle('a = ' + a+', x = '+x, fontsize=14, weight='bold')

     # 1st row - hot in
    axHotFlow = plt.subplot2grid(gridsize, (0,0))
    plt.xticks(fontsize=8, rotation=35)
    axHotFlow.plot(mainFinal['hotInFlowRate'], color='red', label='hotIn')
    axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
    axHotFlow.set_ylabel('GPM')
    axHotFlow.set_xlim(beginDate, endDate)
    #axHotFlow.set_ylim(0.9,1.1)
    axHotFlow.grid(True)

    # 2nd row - cold in
    axColdFlow= plt.subplot2grid(gridsize, (1,0))
    plt.xticks(fontsize=8, rotation=35)
    axColdFlow.plot(mainFinal['NEWhotInFlowRate'], color='maroon', label='New_hotIN')
    axColdFlow.set_title('NEW hot water flowrate', fontsize=10, weight ='bold')
    axColdFlow.set_ylabel('GPM')
    axColdFlow.set_xlim(beginDate, endDate)
    #axColdFlow.set_ylim(0, 1.1)
    axColdFlow.grid(True)

    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
    print('saving fig...')
    plt.savefig(filepath+'HOTflowQC_noiseReduction_fc=' + x + '.png')

    fig = plt.figure(2, figsize=(12, 8))
    fig.autofmt_xdate()
    fig.suptitle('a = ' + a + ', x = ' + x, fontsize=14, weight='bold')

    # 1st row - hot in
    axHotFlow = plt.subplot2grid(gridsize, (0, 0))
    plt.xticks(fontsize=8, rotation=35)
    axHotFlow.plot(mainFinal['coldInFlowRate'], color='blue', label='hotIn')
    axHotFlow.set_title('RAW cold-water flowrate', fontsize=10, weight='bold')
    axHotFlow.set_ylabel('Gal')
    axHotFlow.set_xlim(beginDate, endDate)
    axHotFlow.set_ylim(0,5)
    axHotFlow.grid(True)

    # 2nd row - cold in
    axColdFlow = plt.subplot2grid(gridsize, (1, 0))
    plt.xticks(fontsize=8, rotation=35)
    axColdFlow.plot(mainFinal['NEWcoldInFlowRate'], color='navy', label='New_hotIN')
    axColdFlow.set_title('NEW cold-water flowrate', fontsize=10, weight='bold')
    axColdFlow.set_ylabel('Gal')
    axColdFlow.set_xlim(beginDate, endDate)
    axColdFlow.set_ylim(0, 5)
    axColdFlow.grid(True)

    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
    print('saving fig...')
    plt.savefig(filepath + 'COLDflowQC_noiseReduction_fc=' + x + '.png')

    plt.show()


print(results)
results.reset_index(inplace=True)
results.to_csv(filepath+'24hr_flowQC_noiseReduction_Agg.csv', encoding='utf-8', index=False)

print(results)






print('done')
