# Title: hydroshare_QC.py
# Author: Joseph Brewer
# Last updates: 07/15/2020
# Desc: This is a one-size-fits-all quality control script for 4 weeks of high resolution smart meter
#       data collected from 6 multi-unit residential structures on Utah State University's campus
#       from 03/22/2019 12:00:00 - 04/19/2019 12:00:00.  The data was collected as part of research seeking to
#       quantify and characterize water and water-related energy use in multi-unit residential structures with high-res
#       smart meter data.  Datat quality issues were encountered during data collection.  This script executes a number
#       quality control measures to allow the derived water and energy balance equations to calculate water and
#       water-related energy use.
#
#   External libraries required:
        # Pandas, influxdb, matplotlib, and numpy.




import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
from hydroshare_tempCalibration import calibration_temp
import numpy as np


def adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, threshold):
    filteredSignal = []
    window = []
    windowSize = minWindowSize
    headLength = 0
    for j in range(0, windowSize):
        window.append(signal[j])
    for i in range(0, len(signal)):
        delta = signal[i] - signal[i - 1]
        if np.absolute(delta) > threshold:
            windowSize = minWindowSize
            window.clear()
            if windowSize % 2 == 0:
                headLength = windowSize // 2
            else:
                headLength = (windowSize - 1) // 2
            if (i >= headLength) and (i < (len(signal) - headLength)):
                for j in range(0, windowSize):
                    window.append(signal[i - (headLength - j)])
            elif (i < headLength):
                for j in range(0, windowSize):
                    window.append(signal[i + j])
            else:
                for j in range(0, windowSize):
                    window.append(signal[len(signal) - j])
        else:
            if (windowSize + 2) <= maxWindowSize:
                windowSize += 2
                window.append(0)
                window.append(0)

            if windowSize % 2 == 0:
                headLength = windowSize // 2
            else:
                headLength = (windowSize - 1) // 2
            if (i >= headLength) and (i < (len(signal) - headLength)):
                for j in range(0, windowSize):
                    window[j] = signal[i - (headLength - j)]
            elif (i < headLength):
                for j in range(0, windowSize):
                    window[j] = signal[i + j]
            else:
                for j in range(0, windowSize):
                    window[j] = signal[len(signal) - j - 1]
        median = np.median(window)
        filteredSignal.append(median)
        percent = i / len(signal) * 100
        print(" %2.2f %%, window size = %3d" % (percent, windowSize), end="\r", flush=True)
    print("100.00 %%, window size = %3d" % windowSize)
    return filteredSignal


# Todo: update creds here
host = 'Your-hostname-here'
port = 8086
username = 'Your-username-here'
password = 'Your-password-here'
measurement_raw = 'Your-raw-data-measurement-here'
measurement = 'Your-final-data-measurement-here'
database_raw = 'Your-raw-data-database-here'
database = 'Your-final-database-here'


# Connect to database
print('\nConnecting to database...')
client = InfluxDBClient(host=host,  # Create client object with InfluxDBClient library
                        port=port,
                        username=username,
                        password=password)
client.switch_database(database_raw)  # Set database.

# accept inputs
print('Receiving inputs...\n')
bldgs = ['B', 'C', 'D', 'E', 'F']  # create list of building ID
begin = "'2019-03-22T12:00:00Z'"  # POR start date
end = "'2019-04-19T12:00:00Z'"  # POR end date

for bldg in bldgs:  # loop through list of bldgs to QC all data
    bldgID = "'" + bldg + "'"  # reformat string to match InfluxDB requirements

    # Retrieve data
    print('Assembling data query...')
    # Build query by concatenating inputs into query.  InfluxDB query language has several
    # requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
    # Query returns a 'ResultSet" type.

    # Todo: change measurement name
    query = """SELECT *
      FROM "Your-Raw-Measurment-Here"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + begin + """ AND time <= """ + end + """"""

    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list and get_points.
    results = client.query(query)  # send query
    df = pd.DataFrame(list(results.get_points(measurement=measurement_raw)))  # convert to pandas df with list + get points
    df['time'] = pd.to_datetime(df['time'])  # Convert time to pandas datetime format
    df.set_index('time', inplace=True)  # Set time as index
    print('Data retrieved! \n')

    print('Data retrieved!\n')

    # Begin QC ##################

    # Temperature QC ############

    # tempQC: correlation, BLDG B
    if bldg == 'B':

        # Adjust inaccurate BldgB temp data with correlation from Bldg D temp data
        print('Fetching data to adjust BLDG B temp data...')
        bldgID2 = "'D'"
        begin2 = "'2019-03-22T12:00:00Z'"
        end2 = "'2019-03-27T16:00:00Z'"

        # query correlation dataset and convert to dataframe
        query = """SELECT "hotInTemp" 
        FROM "flow" 
        WHERE "buildingID" =""" + bldgID2 + """ AND time >= """ + begin2 + """ AND time <= """ + end2 + """"""

        results = client.query(query)
        df_2 = pd.DataFrame(list(results.get_points(measurement='flow')))
        df_2['time'] = pd.to_datetime(df_2['time'])
        df_2.set_index('time', inplace=True)
        print('Adjustment data retrieved.\n')

        df_2.rename(columns={'hotInTemp': 'hotInTemp_D'}, inplace=True)
        mainHot = df['hotInTemp'].truncate(after=pd.Timestamp('2019-03-27T16:00:00Z')).copy()
        mainHot = mainHot.to_frame()
        mainHotQC = pd.merge(mainHot, df_2, on='time')

        print('Calculating new temp values...')
        mainHotQC['hotInTemp'] = 8.25122766 + 0.8218035674 * mainHotQC[
            'hotInTemp_D']  # use correlation EQ, determined previously
        df['hotInTemp'].update(mainHotQC['hotInTemp'])  # update original dataframe, df
        print('New hotInTemp values for BLDG B calculated!\n')

        temp_old = df['hotInTemp'].iloc[0]  # Because of differences in missed second observations, have to
        for i, row in df.iterrows():  # parse through QCed dataframe and adjust unadjusted values
            x = row['hotInTemp']  # i.e. B temp data has 12:00:01 while D temp data does not, so
            if x < 40:  # that timestamp is not updated with QC.  Fix by replacing with prev temp value
                df.at[i, 'hotInTemp'] = temp_old
            else:
                temp_old = row['hotInTemp']

    # tempQC: level shift
    df_temp = df.copy()

    print('Level shifting temp values...')
    columns = ['hotInTemp', 'hotOutTemp', 'coldInTemp']
    for (column, cal) in zip(columns, calibration_temp(bldg)):  # Add calibration factor to each value
        df_temp[column] = df_temp[column] + cal  # Calibration factor determined with goal-seek program
    print('Level shifting temp values complete!\n')

    # tempQC:  point fix.  Unless all heat exited the universe in this one second, Temp did not drop to -1333 deg Celsius
    if bldg == 'F':
        df_temp.at[
            '2019-04-15T15:55:33', 'hotInTemp'] = 53.18400  # temp dropped from ~52.4 deg C to -1333 deg C, replace
        # replace with averaged temp value from either side

    # Flow QC ###################

    # flowQC: filter noise
    df_filter = df_temp.copy()

    print('Filtering noise from hotInFlowRate...')
    df_filter['hotInFlowRate'] = adaptiveMedianFilter(df_filter['hotInFlowRate'], 9, 301,
                                                      0.5)  # filter noise in hotInFlowRate
    # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
    print('hotInFlowRate complete!')
    print('Filtering noise from coldInFlowRate...')
    df_filter['coldInFlowRate'] = adaptiveMedianFilter(df_filter['coldInFlowRate'], 1, 301,
                                                       0.5)  # filter noise in coldInFlowRate
    # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
    print('coldInFlowRate complete! \n')

    # flowQC: fix return flow for bldg E
    x = 0
    if bldg == 'E':
        df_2 = df_filter.truncate(after=pd.Timestamp('2019-03-22T14:20:23Z')).copy()
        counter = 0
        for i, row in df_2.iterrows():
            if x < 4:
                if counter < 16:  # average pulse rate for this timeframe from other three weeks
                    df_2.at[i, 'hotOutFlowRate'] = 0  # is 17.26 seconds/pulse.  Code adds 18 second pulse every
                    counter += 1  # every 4th pulse inserted to account for 0.26 second offset
                else:
                    df_2.at[i, 'hotOutFlowRate'] = 1
                    counter = 0
                    x += 1
            elif x == 4:
                if counter < 17:
                    df_2.at[i, 'hotOutFlowRate'] = 0
                    counter += 1
                else:
                    df_2.at[i, 'hotOutFlowRate'] = 1
                    x = 1
                    counter = 0

        df_filter['hotOutFlowRate'].update(df_2['hotOutFlowRate'])  # update with new return pulse values

    df_shift = df_filter.copy()
    # flowQC: level shift
    print('Level shifting hotInFlowRate values...')
    if bldg == 'B':
        df_2 = df_shift.truncate(before=pd.Timestamp('2019-04-16T19:40:00Z')).copy()  # truncate after erroneous vals
        df_2['hotInFlowRate'] = df_2['hotInFlowRate'] - 0.032  # level shift
        df_shift['hotInFlowRate'].update(df_2['hotInFlowRate'])  # update
    elif bldg == 'E':
        df_2 = df_shift.truncate(before=pd.Timestamp('2019-04-04T02:22:55Z')).copy()
        df_2['hotInFlowRate'] = df_2['hotInFlowRate'] - 0.031
        df_shift['hotInFlowRate'].update(df_2['hotInFlowRate'])
    print('Level shifting flow complete! \n')

    # flowQC: Pulse Aggregation
    print('Aggregating pulses...')
    df_agg = df_shift.copy()
    coldInFlow_Sum = 0  # Initalize variables to aggregate temp/flows in between pulses
    hotInFlow_Sum = 0
    hotInTemp_Sum = 0
    coldInTemp_Sum = 0
    hotOutTemp_Sum = 0
    counter = 0  # counter will count seconds between pulses
    for i, row in df_agg.iterrows():
        if row['hotOutFlowRate'] == 0:
            coldInFlow_Sum = coldInFlow_Sum + row['coldInFlowRate']
            coldInTemp_Sum = coldInTemp_Sum + row['coldInTemp']
            hotInFlow_Sum = hotInFlow_Sum + row['hotInFlowRate']
            hotInTemp_Sum = hotInTemp_Sum + row['hotInTemp']
            hotOutTemp_Sum = hotOutTemp_Sum + row['hotOutTemp']
            counter += 1
        elif row['hotOutFlowRate'] != 0:
            counter = counter + 1
            coldInFlow_Sum = coldInFlow_Sum + row['coldInFlowRate']  # sum flows
            coldInTemp_Sum = (coldInTemp_Sum + row['coldInTemp']) / counter  # average temperatures
            hotInFlow_Sum = hotInFlow_Sum + row['hotInFlowRate']
            hotInTemp_Sum = (hotInTemp_Sum + row['hotInTemp']) / counter
            hotOutTemp_Sum = (hotOutTemp_Sum + row['hotOutTemp']) / counter

            df_agg.at[i, 'coldInFlowRate'] = coldInFlow_Sum
            df_agg.at[i, 'coldInTemp'] = coldInTemp_Sum
            df_agg.at[i, 'hotInFlowRate'] = hotInFlow_Sum
            df_agg.at[i, 'hotInTemp'] = hotInTemp_Sum
            df_agg.at[i, 'hotOutTemp'] = hotOutTemp_Sum

            coldInFlow_Sum = 0  # Zero running sums after calculating aggregated values
            coldInTemp_Sum = 0
            hotInFlow_Sum = 0
            hotInTemp_Sum = 0
            hotOutTemp_Sum = 0
            counter = 0

    print('Pulse aggregation complete!')

    df_pulse = df_agg[(df_agg['hotOutFlowRate'] != 0)]  # remove all timestampes where hotReturnFlowRate = 0
    df_pulse['coldInFlowRate'] = df_pulse['coldInFlowRate'] / 60  # convert from gpm to gps
    df_pulse['hotInFlowRate'] = df_pulse['hotInFlowRate'] / 60  # convert from gpm to gps
    df_pulse['hotOutFlowRate'] = 1  # 1 pulse = 1 gal.  SO every pulse indicates 1 gal has
    # passed through the return system

    # flowQC: zero hot water flow
    for i, row in df_pulse.iterrows():  # replace infintessimal negative flow values with 0.
        if row['hotInFlowRate'] < 1:
            df_pulse.at[i, 'hotInFlowRate'] = 1

    df_pulse['hotWaterUse'] = df_pulse['hotInFlowRate'] - df_pulse['hotOutFlowRate']  # Calculate hot water use

    df_final = df_pulse.copy()

    # Add building ID, influx write points requires all data coming from dataframe
    df_final['buildingID'] = bldg

    print('QC Completed!\n')

    print('Plotting final flowrates...')
    # print final data
    # Initialize figures and subplots
    gridsize = (3, 1)
    fig = plt.figure(1, figsize=(12, 8))
    fig.autofmt_xdate()
    fig.suptitle('Final Flowrates for BLDG ' + bldg, fontsize=14, weight='bold')

    # 1st row - hot in
    axHotFlow = plt.subplot2grid(gridsize, (0, 0))
    plt.xticks(fontsize=8, rotation=35)
    axHotFlow.plot(df_final['hotInFlowRate'], color='red', label='hotIn_final')
    axHotFlow.set_title('hot water flowrate', fontsize=10, weight='bold')
    axHotFlow.set_ylabel('GPM')
    axHotFlow.grid(True)

    # 2nd row - cold in
    axColdFlow = plt.subplot2grid(gridsize, (1, 0))
    plt.xticks(fontsize=8, rotation=35)
    axColdFlow.plot(df_final['coldInFlowRate'], color='blue', label='coldWaterUse_final')
    axColdFlow.set_title('cold water flowrate', fontsize=10, weight='bold')
    axColdFlow.set_ylabel('GPM')
    axColdFlow.grid(True)

    # 3rd row - hot return
    axHotWaterUse = plt.subplot2grid(gridsize, (2, 0))
    plt.xticks(fontsize=8, rotation=35)
    axHotWaterUse.plot(df_final['hotWaterUse'], color='maroon', label='hotWaterUse_final')
    axHotWaterUse.set_title('hotWaterUse', fontsize=10, weight='bold')
    axHotWaterUse.set_ylabel('GPM')
    axHotWaterUse.grid(True)

    fig.show()
    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

    print('Plotting final temperatures...')
    fig2 = plt.figure(2, figsize=(12, 8))
    fig2.suptitle('Final Temps for BLDG ' + bldg, fontsize=14, weight='bold')

    gridsize = (2, 1)
    axHotTemp = plt.subplot2grid(gridsize, (0, 0))
    plt.xticks(fontsize=8, rotation=35)
    axHotTemp.plot(df_final['hotInTemp'], color='red', label='hotIn_final')
    axHotTemp.plot(df_final['hotOutTemp'], color='maroon', label='hotOut_final')
    axHotTemp.set_title('hot water temp', fontsize=10, weight='bold')
    axHotTemp.set_ylabel('Temp (C)')
    axHotTemp.grid(True)

    axColdTemp = plt.subplot2grid(gridsize, (1, 0))
    plt.xticks(fontsize=8, rotation=35)
    axColdTemp.plot(df_final['coldInTemp'], color='blue', label='1-Sec HOT Data')
    axColdTemp.set_title('cold water temp', fontsize=10, weight='bold')
    axColdTemp.set_ylabel('Temp (C)')
    axColdTemp.grid(True)

    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
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
        clientdf.write_points(dataframe=df_final, measurement=measurement,
                              field_columns={'hotInFlowRate': df_final[['hotInFlowRate']],
                                             'coldInFlowRate': df_final[['coldInFlowRate']],
                                             'hotOutFlowRate': df_final[['hotOutFlowRate']],
                                             'hotInTemp': df_final[['hotInTemp']],
                                             'coldInTemp': df_final[['coldInTemp']],
                                             'hotOutTemp': df_final[['hotOutTemp']]
                                             },
                              tag_columns={'buildingID': df_final[['buildingID']]},
                              protocol='line', numeric_precision=10, batch_size=2000)

    else:
        print('Better luck next time...')

print('DONE!!')

