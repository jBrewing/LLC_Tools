import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from tempQC.calibrationFunctions import calibration_temp
import numpy as np
import os

os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/dataDecimation/')

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



# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldg = input("Input building ID: ").upper()
bldg='D'
bldgID = "'" + bldg + "'"
#    dates
beginDate = "'2019-03-24T00:00:00Z'"
endDate = "'2019-03-31T00:00:00Z'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws') # Set database.

query = """SELECT *
      FROM "flow"
      WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query) # send query
df = pd.DataFrame(list(results.get_points(measurement='flow')))
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True) # Set time as index
print('Data retrieved! \n')


print('QCing Data...')
# Temperature QC ############

# tempQC: level shift
df_temp = df.copy()
# Add calibration factor to each value
print('Level shifting temp values...')
columns = ['hotInTemp', 'hotOutTemp','coldInTemp']
for (column, cal) in zip(columns, calibration_temp(bldg)):
    df_temp[column] = df_temp[column] + cal
print('Level shifting temp values complete!\n')


# Flow QC ###################

# flowQC: filter noise
df_filter=df_temp.copy()

print('Filtering noise from hotInFlowRate...')
df_filter['hotInFlowRate'] = adaptiveMedianFilter(df_filter['hotInFlowRate'], 9, 301,0.5) # filter noise in hotInFlowRate
                           # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
print('hotInFlowRate complete!')
print('Filtering noise from coldInFlowRate...')
df_filter['coldInFlowRate'] = adaptiveMedianFilter(df_filter['coldInFlowRate'], 1, 301,0.5) # filter noise in coldInFlowRate
                            # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
print('coldInFlowRate complete! \n')

df_filter.loc[df_filter.hotOutFlowRate != 0, 'hotOutFlowRate'] = 1

df_filter[['hotInFlowRate','coldInFlowRate']] = df_filter[['hotInFlowRate','coldInFlowRate']]/60 # convert from gpm to gps
df_filter[['hotInFlowRate','coldInFlowRate','hotOutFlowRate']] = df_filter[['hotInFlowRate','coldInFlowRate','hotOutFlowRate']]*0.00378541 # convert gps to m3/s

df_final = df_filter.copy()

print('Data QC complete!')

x = input('Do you want to write to database? (y/n): ').upper()

if x == 'Y':

# WritePoints
    print('Connecting to database...')
    clientdf = DataFrameClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
    clientdf.switch_database('ciws_final')
    print('Writing points...')
    clientdf.write_points(dataframe=df_final, measurement='DD',
                            field_columns={'hotInFlowRate':df_final[['hotInFlowRate']],
                                           'coldInFlowRate': df_final[['coldInFlowRate']],
                                           'hotOutFlowRate': df_final[['hotOutFlowRate']],
                                           'hotInTemp': df_final[['hotInTemp']],
                                           'coldInTemp': df_final[['coldInTemp']],
                                           'hotOutTemp': df_final[['hotOutTemp']],
                                           },
                            tag_columns={'buildingID': df_final[['buildingID']]},
                            protocol='line', numeric_precision=10, batch_size=2000)

else:
    print('Better luck next time...')