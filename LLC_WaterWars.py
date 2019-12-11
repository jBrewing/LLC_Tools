import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime
from bldgNUM import bldgNUM
import numpy as np


print('Receiving inputs...\n')
# Input parameters.
# Available dates - 2018/10/10 - 2018/11/10
#       Dates - Do not remove 'T' or 'Z' - required influxDB syntax.
#       bldgID - Do not remove " " or ' ' - required influxDB syntax
beginDate = "'2019-10-08T00:00:00Z'"
#endDate = "'2019-10-22T00:00:00Z'"
endDate = str(datetime.now().strftime("'%Y-%m-%dT%H:%M:%SZ'"))
filepath = '/Users/joseph/Desktop/GRA/WaterWars/'



# create results dataframe into which results will load.  Also create present week and last week slices
bldgs = ['A','B', 'C', 'D', 'E', 'F']
results = pd.DataFrame(columns=['totalWaterUse','TWU_perDay', 'TWU_hot', 'TWU_cold', 'TWU_perCap', 'TWU_perCap_perDay',
                                'pw_TWU', 'pw_TWU_perDay', 'pw_TWU_hot', 'pw_TWU_cold', 'pw_TWU_perCap', 'pw_TWU_perCap_perDay']
                       , index=bldgs)


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')


for x in bldgs:
    bldgID = "'"+x+"'"
    print('Assembling query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
    # bracketed with ' '.
    # Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
    query = """SELECT "coldInFlowRate", "hotInFlowRate", "hotOutFlowRate" FROM "flow" WHERE "buildingID" ="""+bldgID+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""


    print('Retrieving data for ' +x+'...')
    # Convert returned ResultSet to Pandas dataframe with list
    # and get_points.
    # Set dataframe index as datetime.
    main_Query = client.query(query)
    main_ls = list(main_Query.get_points(measurement='flow'))
    main = pd.DataFrame(main_ls)
    main['time'] = pd.to_datetime(main['time'])
    main.set_index('time', inplace=True)
    print('Data retrieved!')

    print('QCing data...')
    coldInFlow_Sum = 0
    hotInFlow_Sum = 0
    #hotInTemp_Sum = 0
    #coldInTemp_Sum = 0
    #hotOutTemp_Sum = 0
    counter = 0
    for y, row in main.iterrows():
        # get all values from row.  Values not needed are commented out
        coldInFlow = row['coldInFlowRate']
        #coldInTemp = row['coldInTemp']
        hotInFlow = row['hotInFlowRate']
        #hotInTemp = row['hotInTemp']
        hotOutFlow = row['hotOutFlowRate']
        #hotOutTemp = row['hotOutTemp']
        if hotOutFlow == 0:
            coldInFlow_Sum = coldInFlow_Sum + coldInFlow
            #coldInTemp_Sum = coldInTemp_Sum + coldInTemp
            hotInFlow_Sum = hotInFlow_Sum + hotInFlow
            #hotInTemp_Sum = hotInTemp_Sum + hotInTemp
            #hotOutTemp_Sum = hotOutTemp_Sum + hotOutTemp
            counter = counter + 1
        elif hotOutFlow != 0:
            counter = counter + 1
            coldInFlow_Sum = coldInFlow_Sum + coldInFlow
            #coldInTemp_Sum = (coldInTemp_Sum + coldInTemp) / counter
            hotInFlow_Sum = hotInFlow_Sum + hotInFlow
            #hotInTemp_Sum = (hotInTemp_Sum + hotInTemp) / counter
            #hotOutTemp_Sum = (hotOutTemp_Sum + hotOutTemp) / counter

            main.at[y, 'coldInFlowRate'] = coldInFlow_Sum
            #main.at[i, 'coldInTemp'] = coldInTemp_Sum
            main.at[y, 'hotInFlowRate'] = hotInFlow_Sum
            #main.at[i, 'hotInTemp'] = hotInTemp_Sum
            #main.at[i, 'hotOutTemp'] = hotOutTemp_Sum

            coldInFlow_Sum = 0
            #coldInTemp_Sum = 0
            hotInFlow_Sum = 0
            #hotInTemp_Sum = 0
            #hotOutTemp_Sum = 0
            counter = 0
    print('Data QCed!')

    # convert flowrates to gps, each second obs = amount of flow for that second
    main['coldInFlowRate'] = main['coldInFlowRate'] / 60
    main['hotInFlowRate'] = main['hotInFlowRate'] / 60
    # replace pulse counts with 1, 1 pulse = 1 gal
    mainFinal = main[(main['hotOutFlowRate'] != 0)]
    mainFinal['hotOutFlowRate'] = 1

    if bldgs == 'E':
        fix = mainFinal['hotInFlowRate'].loc['2019-10-08T00:00:00Z':'2019-10-10T12:54:55'] - .15
        mainFinal['hotInFlowRate'].update(fix)

    mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate']
    mainFinal['hotWaterUse'] = mainFinal['hotInFlowRate'] - mainFinal['hotOutFlowRate']
    mainFinal['totalWaterUse'] = mainFinal['hotWaterUse']+mainFinal['coldInFlowRate']

    print('Generating results...')
# baseline
    baseline = mainFinal.loc['2019-10-08T00:00:00Z':'2019-10-15T00:00:00Z']
    bl_TWU = baseline['totalWaterUse'].sum()
    bl_hot = baseline['hotWaterUse'].sum()
    bl_cold = baseline['coldInFlowRate'].sum()
  # generate graph of hourly use throughout day for last wee
    baseline = baseline.resample(rule='1H', base=0).sum()
    baseline = baseline.groupby(baseline.index.hour).mean()

# week 1
    week1 = mainFinal.loc['2019-11-04T00:00:00Z': '2019-11-11T00:00:00Z']
  # savings
    wk1_TWU = week1['totalWaterUse'].sum()
    wk1_hot= week1['hotWaterUse'].sum()
    wk1_cold = week1['coldInFlowRate'].sum()
  # generate graph of hourly use throughout day for last
    week1 = week1.resample(rule='1H', base=0).sum()
    week1 = week1.groupby(week1.index.hour).mean()

# week 2
    if x == 'C':
        week2 = mainFinal.loc['2019-11-12T00:00:00Z':'2019-11-16T00:00:00Z']
    else:
        week2 = mainFinal.loc['2019-11-12T00:00:00Z':'2019-11-19T00:00:00Z']
    # savings
    wk2_TWU = week2['totalWaterUse'].sum()
    wk2_hot = week2['hotWaterUse'].sum()
    wk2_cold = week2['coldInFlowRate'].sum()
    # generate graph of hourly use throughout day for last
    week2 = week2.resample(rule='1H', base=0).sum()
    week2 = week2.groupby(week2.index.hour).mean()

# follow up
    week3 = mainFinal.loc['2019-12-02T00:00:00Z': '2019']
    # savings
    wk3_TWU = week3['totalWaterUse'].sum()
    wk3_hot = week3['hotWaterUse'].sum()
    wk3_cold = week3['coldInFlowRate'].sum()
    # generate graph of hourly use throughout day for last
    week3 = week3.resample(rule='1H', base=0).sum()
    week3 = week3.groupby(week3.index.hour).mean()


    bldgNum = bldgNUM(x)
    days = 7

    print('Adding to results table...')
#    results.at[x, 'buildingID'] = x
    results.at[x, 'totalWaterUse'] = bl_TWU
    results.at[x, 'TWU_perDay'] = bl_TWU/days
    results.at[x, 'TWU_hot'] = bl_hot
    results.at[x, 'TWU_cold'] = bl_cold
    results.at[x, 'TWU_perCap'] = bl_TWU/bldgNum
    results.at[x, 'TWU_perCap_perDay'] = bl_TWU/bldgNum/days
    results.at[x, 'wk1_TWU'] = wk1_TWU
    results.at[x, 'wk1_TWU_perDay'] = wk1_TWU/days
    results.at[x, 'wk1_TWU_hot'] = wk1_hot
    results.at[x, 'wk1_TWU_cold'] = wk1_cold
    results.at[x, 'wk1_TWU_perCap'] = wk1_TWU/bldgNum
    results.at[x, 'wk1_TWU_perCap_perDay'] = wk1_TWU/bldgNum/days
    if x == 'C':
        days = 4
    results.at[x, 'wk2_TWU'] = wk2_TWU
    results.at[x, 'wk2_TWU_perDay'] = wk2_TWU/days
    results.at[x, 'wk2_TWU_hot'] = wk2_hot
    results.at[x, 'wk2_TWU_cold'] = wk2_cold
    results.at[x, 'wk2_TWU_perCap'] = wk2_TWU / bldgNum
    results.at[x, 'wk2_TWU_perCap_perDay'] = wk2_TWU/bldgNum/days
    days = 7
    results.at[x, 'wk3_TWU'] = wk3_TWU
    results.at[x, 'wk3_TWU_perDay'] = wk3_TWU / days
    results.at[x, 'wk3_TWU_hot'] = wk3_hot
    results.at[x, 'wk3_TWU_cold'] = wk3_cold
    results.at[x, 'wk3_TWU_perCap'] = wk3_TWU / bldgNum
    results.at[x, 'wk3_TWU_perCap_perDay'] = wk3_TWU / bldgNum / days



    print('Plotting results...\n')
    # Initialize figures and subplots

    # create hour column to circumvent index/datetime errors in plotting
    hours = ['12:00am', '01:00am','02:00am','03:00am','04:00am','05:00am','06:00am','07:00am','08:00am','09:00am','10:00am','11:00am','12:00pm',
             '01:00pm','02:00pm','03:00pm','04:00pm','05:00pm','06:00pm','07:00pm','08:00pm','09:00pm','10:00pm','11:00pm']
    baseline = baseline.apply(np.roll, shift=1)
    baseline['time'] = hours
    week1['time'] = hours
    week2['time'] =hours
    week3['time']=hours

    plt.figure(figsize=(12,6))
    plt.plot('time', 'totalWaterUse', data=baseline, color='grey', alpha=0.35, label='Baseline')
    plt.plot('time', 'totalWaterUse', data=week1, color='skyblue', linewidth=2, linestyle='--', alpha=0.75,
             label='Nov 4 - Nov 10')
    plt.plot('time', 'totalWaterUse', data=week2, color='navy', linewidth=2, alpha=0.75, label='Nov 11 - Nov 17')
    plt.plot('time', 'totalWaterUse', data=week2, color='navy', linewidth=4, label='DEC 03 - DEC 10')
    plt.title('Average Hourly Water Use: BLDG '+x, fontsize=18)
    plt.xlabel('Time', fontsize=14)
    plt.ylabel('Water Use (gal)', fontsize=14)
    plt.xticks(rotation=45, fontsize = 8)
    plt.legend()
    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


    plt.savefig(filepath+'waterUse_'+x+'.png')


#    plt.show()

print('Saving results to csv file...')
results.reset_index(inplace=True)
results.to_csv(filepath+'waterUseResults.csv', encoding='utf-8', index=False)


print('done!')