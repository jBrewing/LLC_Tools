

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
from bldgNUM import bldgNUM
from tabulate import tabulate


beginDate = "'2019-10-08T00:00:00'"
endDate = "'2019-10-22T00:00:00'"

# print options for plotting
print('Connecting to directory...')
filepath = "/Users/joseph/Desktop/GRA/WaterWars/"
all_files = glob.iglob(os.path.join(filepath, "bldgA_Data/*.csv"))
x="A"

print('Building dataframe...')
main = pd.concat((pd.read_csv(f, header=1, sep=',', parse_dates=True, low_memory=False)
                     for f in all_files),
                    ignore_index=True)
main['Date'] = pd.to_datetime(main['Date'])
main.set_index('Date', inplace=True)
main.sort_index(inplace=True)
main.drop(['RecordNumber', 'coldInVoltage', 'coldInVolume', 'hotInVoltage',
              'hotInVolume', 'hotOutPulseCount', 'hotOutVolume'], axis=1, inplace=True)

main = main.loc['2019-10-08T00:00:00Z':'2019-10-22T00:00:00']

print('QCing Data...')
coldInFlow_Sum =0
hotInFlow_Sum = 0
hotInTemp_Sum = 0
coldInTemp_Sum = 0
hotOutTemp_Sum = 0
counter = 0
for i, row in main.iterrows():
    # get all values from row.  Values not needed are commented out
    coldInFlow = row['coldInFlowRate']
    coldInTemp = row['coldInTemp']
    hotInFlow = row['hotInFlowRate']
    hotInTemp = row['hotInTemp']
    hotOutFlow = row['hotOutFlowRate']
    hotOutTemp = row['hotOutTemp']
    if hotOutFlow == 0:
          coldInFlow_Sum = coldInFlow_Sum + coldInFlow
          coldInTemp_Sum = coldInTemp_Sum + coldInTemp
          hotInFlow_Sum = hotInFlow_Sum + hotInFlow
          hotInTemp_Sum = hotInTemp_Sum + hotInTemp
          hotOutTemp_Sum = hotOutTemp_Sum + hotOutTemp
          counter = counter + 1
    elif hotOutFlow != 0:
          counter = counter + 1
          coldInFlow_Sum = coldInFlow_Sum + coldInFlow
          coldInTemp_Sum = (coldInTemp_Sum + coldInTemp) / counter
          hotInFlow_Sum = hotInFlow_Sum + hotInFlow
          hotInTemp_Sum = (hotInTemp_Sum + hotInTemp) / counter
          hotOutTemp_Sum = (hotOutTemp_Sum + hotOutTemp) / counter

          main.at[i, 'coldInFlowRate'] = coldInFlow_Sum
          main.at[i, 'coldInTemp'] = coldInTemp_Sum
          main.at[i, 'hotInFlowRate'] = hotInFlow_Sum
          main.at[i, 'hotInTemp'] = hotInTemp_Sum
          main.at[i, 'hotOutTemp'] = hotOutTemp_Sum

          coldInFlow_Sum = 0
          coldInTemp_Sum = 0
          hotInFlow_Sum = 0
          hotInTemp_Sum = 0
          hotOutTemp_Sum = 0
          counter = 0

mainFinal = main[(main['hotOutFlowRate'] != 0)]
mainFinal['coldInFlowRate'] = mainFinal['coldInFlowRate']/60
mainFinal['hotInFlowRate'] = mainFinal['hotInFlowRate']/60
mainFinal['hotOutFlowRate'] = 1
mainFinal['hotWaterUse'] = mainFinal['hotInFlowRate'] - mainFinal['hotOutFlowRate']


sumHotIn = mainFinal['hotInFlowRate'].sum()
sumHotOut = mainFinal['hotOutFlowRate'].sum()
sumColdIn = mainFinal['coldInFlowRate'].sum()

hotUse = sumHotIn-sumHotOut

source = ["type", "HOT", "COLD", "RETURN"]
A = [('WaterUse', hotUse, sumColdIn , ''),
     ('WaterUse/day', hotUse/14, sumColdIn/14, ''),]
print(tabulate(A, headers=source))


print('Plotting final flowrates...')
gridsize=(3,1)
fig2=plt.figure(1,figsize=(12,8))
fig2.autofmt_xdate()
fig2.suptitle('Flowrate for BLDG A', fontsize=14, weight='bold')

 # 1st row - hot in
axHotFlow = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotFlow.plot(mainFinal['hotInFlowRate'], color='red', label='hotIn_final')
axHotFlow.set_title('hot water flowrate', fontsize=10, weight ='bold')
axHotFlow.set_ylabel('GPM')
axHotFlow.set_xlim(beginDate, endDate)
axHotFlow.grid(True)

# 2nd row - cold in
axColdFlow= plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdFlow.plot(mainFinal['coldInFlowRate'], color='blue', label='coldWaterUse_final')
axColdFlow.set_title('cold water flowrate', fontsize=10, weight ='bold')
axColdFlow.set_ylabel('GPM')
axColdFlow.set_xlim(beginDate, endDate)
axColdFlow.grid(True)

# 3rd row - hot return
axHotWaterUse = plt.subplot2grid(gridsize, (2,0))
plt.xticks(fontsize=8, rotation=35)
axHotWaterUse.plot(mainFinal['hotWaterUse'], color='maroon', label='hotWaterUse_final')
axHotWaterUse.set_title('hotWaterUse', fontsize=10, weight ='bold')
axHotWaterUse.set_ylabel('GPM')
axHotWaterUse.set_xlim(beginDate, endDate)
#axHotWaterUse.set_ylim(-0.01, 0.05)
axHotWaterUse.grid(True)

#fig2.show()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)


print('Plotting final temperatures...')
fig3=plt.figure(2,figsize=(12,8))
fig3.suptitle('Temps for BLDG A', fontsize=14, weight='bold')

gridsize = (2,1)
axHotTemp = plt.subplot2grid(gridsize, (0,0))
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(mainFinal['hotInTemp'], color='red', label='hotIn_final')
axHotTemp.plot(mainFinal['hotOutTemp'], color='maroon', label='hotOut_final')
axHotTemp.set_title('hot water temp', fontsize=10, weight ='bold')
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.grid(True)

axColdTemp = plt.subplot2grid(gridsize, (1,0))
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(mainFinal['coldInTemp'], color='blue', label='1-Sec HOT Data')
axColdTemp.set_title('cold water temp', fontsize=10, weight ='bold')
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.grid(True)

plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
fig3.show()
plt.show()

print('done!')
