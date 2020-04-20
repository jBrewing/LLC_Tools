import pandas as pd
import matplotlib.pyplot as plt
import os

#change to match your directory
workingDir = "/Users/joseph/Desktop/GRA/ResearchComponents/DATA/QC/Flow/NoiseReduction/toJosh/"
os.chdir(workingDir)


fileInput = "BLDG_C_03-23_02-24_RAWflow.csv"

# import csv file
df = pd.read_csv(fileInput)
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)


pi = 3.1415926
dt = 1 #second
fc = [0.05, 0.04, 0.03, 0.02, 0.01, 0.009, 0.008, 0.007, 0.006, 0.005]


y = 1
z=1
for x in fc:
    print('reducing noise...\n')
    a = (2 * pi * dt * x) / (2 * pi * dt * x + 1)
    for i, row in df.iterrows():
        y = a * row['hotInFlowRate'] + (1-a) * y
        z = a * row['coldInFlowRate'] + (1 - a) * z
        df.at[i, 'NEWhotInFlowRate'] = y
        df.at[i, 'NEWcoldInFlowRate'] = z



# compare differences in volume
    # Calculate sum of hotIn flowrate without adjustment for control value
    hotSum = df['hotInFlowRate'].sum()
    coldSum = df['coldInFlowRate'].sum()

    #iteratvely calculate NEW hotIn flowrate to compare impact of adjustment with control
    NEWhotSum = df['NEWhotInFlowRate'].sum()
    NEWcoldSum = df['NEWcoldInFlowRate'].sum()

    diffHot = str((hotSum-NEWhotSum)/hotSum * 100)
    diffCold = str((coldSum - NEWcoldSum) / coldSum * 100)

    x = str(x)
    a = str(a)

    print('hot diff: ' +diffHot+' %')
    print('cold diff: '+diffCold+' %')


    print('Plotting final flowrates...')
    gridsize=(2,1)
    fig=plt.figure(1,figsize=(12,8))
    fig.autofmt_xdate()
    fig.suptitle('a = ' + a+', x = '+x, fontsize=14, weight='bold')


     # 1st row - hot in
    axHotFlow = plt.subplot2grid(gridsize, (0,0))
    plt.xticks(fontsize=8, rotation=35)
    axHotFlow.plot(df['hotInFlowRate'], color='red', label='hotIn')
    axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
    axHotFlow.set_ylabel('GPM')
    #axHotFlow.set_ylim(0.9,1.1)
    axHotFlow.grid(True)


    # 2nd row - cold in
    axColdFlow= plt.subplot2grid(gridsize, (1,0))
    plt.xticks(fontsize=8, rotation=35)
    axColdFlow.plot(df['NEWhotInFlowRate'], color='maroon', label='New_hotIN')
    axColdFlow.set_title('NEW hot water flowrate', fontsize=10, weight ='bold')
    axColdFlow.set_ylabel('GPM')
    #axColdFlow.set_ylim(0, 1.1)
    axColdFlow.grid(True)

    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


    fig = plt.figure(2, figsize=(12, 8))
    fig.autofmt_xdate()
    fig.suptitle('a = ' + a + ', x = ' + x, fontsize=14, weight='bold')


    # 1st row - hot in
    axHotFlow = plt.subplot2grid(gridsize, (0, 0))
    plt.xticks(fontsize=8, rotation=35)
    axHotFlow.plot(df['coldInFlowRate'], color='blue', label='hotIn')
    axHotFlow.set_title('RAW cold-water flowrate', fontsize=10, weight='bold')
    axHotFlow.set_ylabel('GPM')
#    axHotFlow.set_ylim(0,5)
    axHotFlow.grid(True)


    # 2nd row - cold in
    axColdFlow = plt.subplot2grid(gridsize, (1, 0))
    plt.xticks(fontsize=8, rotation=35)
    axColdFlow.plot(df['NEWcoldInFlowRate'], color='navy', label='New_hotIN')
    axColdFlow.set_title('NEW cold-water flowrate', fontsize=10, weight='bold')
    axColdFlow.set_ylabel('GPM')
#    axColdFlow.set_ylim(0, 5)
    axColdFlow.grid(True)

    plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
    plt.show()


print('done')
