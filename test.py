import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np


filepath='/Users/joseph/Desktop/GRA/ResearchComponents/DATA/QC/Flow/NoiseReduction/AggData/1day_midnight-midnight/'

def adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, threshold):
    filteredSignal = []
    window = []
    windowSize = minWindowSize
    headLength = 0
    for j in range(0, windowSize):
        window.append(signal[j])
    for i in range(0,len(signal)):
        delta = signal[i] - signal[i-1]
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
                    window.append(signal[i+j])
            else:
                for j in range(0, windowSize):
                    window.append(signal[len(signal)-j])
        else:
            if (windowSize + 2) <= maxWindowSize:
                windowSize += 2
                window.append(0)
                window.append(0)

            if windowSize % 2 == 0:
                headLength = windowSize // 2
            else:
                headLength = (windowSize -1) // 2
            if (i >= headLength) and (i < (len(signal) - headLength)):
                for j in range(0, windowSize):
                    window[j] = signal[i - (headLength - j)]
            elif (i < headLength):
                for j in range(0, windowSize):
                    window[j] = signal[i+j]
            else:
                for j in range(0, windowSize):
                    window[j] = signal[len(signal) -j-1]
        median = np.median(window)
        filteredSignal.append(median)
        percent = i/len(signal) * 100
        print( " %2.2f %%, window size = %3d" % (percent, windowSize), end ="\r", flush = True)
    print("100.00 %%, window size = %3d" % windowSize)
    return filteredSignal



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
df= client.query(query)
df_ls = list(df.get_points(measurement='flow'))
df = pd.DataFrame(df_ls)
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)


print('reducing noise...')
print('hot in:')
hotAdaptiveMedian = adaptiveMedianFilter(df['hotInFlowRate'], 9, 301, 0.5) # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
dfHot = pd.DataFrame(hotAdaptiveMedian)
print('hot in DONE')

print ('cold in')
coldAdaptiveMedian = adaptiveMedianFilter(df['coldInFlowRate'], 1, 301, 0.5) # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
print('cold in DONE')


# compare differences in volume
# Calculate sum of hotIn flowrate without adjustment for control value
hotSum = df['hotInFlowRate'].sum()
coldSum = df['coldInFlowRate'].sum()

#iteratvely calculate NEW hotIn flowrate to compare impact of adjustment with control
NEWhotSum = sum(hotAdaptiveMedian)
NEWcoldSum = sum(coldAdaptiveMedian)

diffHot = str((hotSum-NEWhotSum)/hotSum * 100)
diffCold = str((coldSum - NEWcoldSum) / coldSum * 100)

print('hot diff: ' +diffHot+' %')
print('cold diff: '+diffCold+' %')



print('Plotting final flowrates...')

gridsize=(2,1)
fig=plt.figure(1,figsize=(14,10))
fig.autofmt_xdate()

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(df['hotInFlowRate'], color='red', label='hotIn')
axHotFlow.set_title('RAW hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
#axHotFlow.set_xlim(beginDate, endDate)
#axHotFlow.set_ylim(3,5)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(hotAdaptiveMedian, color='maroon', label='New_hotIN')
axColdFlow.set_title('NEW hot water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
#axColdFlow.set_xlim(beginDate, endDate)
#axColdFlow.set_ylim(3,5)
axColdFlow.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
print('saving fig...')
plt.show()
#plt.savefig(filepath+'HOTflowQC_noiseReduction_fc=' + x + '.png')


fig = plt.figure(2, figsize=(14, 10))
fig.autofmt_xdate()


# 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0, 0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(df['coldInFlowRate'], color='blue', label='hotIn')
axHotFlow.set_title('RAW cold-water flowrate', fontsize=10, weight='bold')
axHotFlow.set_ylabel('GPM')
#axHotFlow.set_xlim(beginDate, endDate)
#axHotFlow.set_ylim(0,40)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow = plt.subplot2grid(gridsize, (1, 0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(coldAdaptiveMedian, color='navy', label='New_hotIN')
axColdFlow.set_title('NEW cold-water flowrate', fontsize=10, weight='bold')
axColdFlow.set_ylabel('GPM')
#axColdFlow.set_xlim(beginDate, endDate)
#axColdFlow.set_ylim(0, 40)
axColdFlow.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
#print('saving fig...')
#  plt.savefig(filepath + 'COLDflowQC_noiseReduction_fc=' + x + '.png')
plt.show()





print('done')
