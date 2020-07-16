import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os




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


print('Receiving inputs...\n')
# Input parameters.

beginDate = "'2019-03-23T03:00:00Z'"
endDate = "'2019-03-23T04:00:00Z'"
bldg='C'
bldgID = "'" + bldg + "'"


print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
client.switch_database('ciws')


print('Assembling query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be
# bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
query = """SELECT "hotInFlowRate", "coldInFlowRate" FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + beginDate + """ AND time <= """ + endDate + """"""


print('Retrieving data...')
df = client.query(query)
df = pd.DataFrame(list(df.get_points(measurement='flow'))) # Convert returned ResultSet to Pandas dataframe with list and get_points.
df['time'] = pd.to_datetime(df['time']) # Convert time to pandas datetime format
df.set_index('time', inplace=True)  # Set dataframe index as datetime.


df = df.truncate(before=pd.Timestamp('2019-03-23T03:00:00Z'),after=pd.Timestamp('2019-03-23T04:00:00Z'))


print('reducing noise...')
print('hot in:')
df['new_hotInFlowRate'] = adaptiveMedianFilter(df['hotInFlowRate'], 9, 301,0.5)
                    # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)
print('hot in DONE')

print('cold in:')
df['new_coldInFlowRate'] = adaptiveMedianFilter(df['coldInFlowRate'], 1, 301,0.5)
                     # adaptiveMedianFilter(signal, minWindowSize, maxWindowSize, Threshold)



print('Plotting final flowrates...')
os.chdir('/Users/augustus/Desktop/GRA/Thesis/Figures/dataQC/')

fig = plt.figure(1, figsize=(14, 10))
gridsize = (2, 1)

fig.autofmt_xdate()


# 1st row - hot in
axHotRaw = plt.subplot2grid(gridsize, (0, 0))
axHotRaw.plot(df['hotInFlowRate'], color='red', label='Raw Data')
#axHotFlow.set_title('Raw Hot Water Supply Flow Signal', fontsize=18)
axHotRaw.set_ylabel('GPM', fontsize = 18, weight='bold')
axHotRaw.set_xlabel('')
axHotRaw.set_ylim(3,5.25)
axHotRaw.tick_params(labelbottom=False)
plt.yticks(fontsize=14)

# 2nd row - cold in
axColdRaw = plt.subplot2grid(gridsize, (1, 0))

axColdRaw.plot(df['coldInFlowRate'], color='blue', label='Filtered Data')
#axColdFlow.set_title('Filtered Hot Water Supply Flow Signal', fontsize=18)
axColdRaw.set_ylabel('GPM', fontsize=18, weight = 'bold')
axColdRaw.set_xlabel('Time', fontsize=18, weight='bold')
#axColdRaw.set_ylim(3,5.25)
axColdRaw.xaxis.set_major_formatter(mdates.DateFormatter("%T"))
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

fig.show()

print('saving fig...')
plt.savefig('dataQC_noiseReduction-raw.png')




fig = plt.figure(2, figsize=(14, 10))
fig.autofmt_xdate()

# 1st row - hot in
axHotNew = plt.subplot2grid(gridsize, (0, 0))
axHotNew.plot(df['new_hotInFlowRate'], color='red', label='Raw Data')
#axHotNew.set_title('Raw Hot Water Supply Flow Signal', fontsize=18)
axHotNew.set_ylabel('GPM', fontsize = 18, weight='bold')
axHotNew.set_xlabel('')
axHotNew.tick_params(labelbottom=False)
plt.yticks(fontsize=14)

# 2nd row - cold in
axColdNew = plt.subplot2grid(gridsize, (1, 0))

axColdNew.plot(df['new_coldInFlowRate'], color='blue', label='Filtered Data')
#axColdNew.set_title('Filtered Hot Water Supply Flow Signal', fontsize=18)
axColdNew.set_ylabel('GPM', fontsize=18, weight='bold')
axColdNew.set_xlabel('Time', fontsize=18, weight='bold')
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
axColdNew.xaxis.set_major_formatter(mdates.DateFormatter("%T"))
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
print('saving fig...')
plt.savefig('dataQC_noiseReduction-qa.png')

fig.show()
plt.show()



print('done')