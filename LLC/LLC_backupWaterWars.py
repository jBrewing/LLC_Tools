#
# Simple dataplotter
#
# Developed by Joseph Brewer
# Date: 11/14/18
#
# Description:  This is a simple script for the purpose of querying LLC Water
# consumption data with inputs and plotting it for basic visualization.
#   Overview:  - import datalogger csv files
#              - plot
#
# Latest edit - 11/14/2018

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

bldgs = ['A']
results = pd.DataFrame(columns=['totalWaterUse','TWU_perDay', 'TWU_hot', 'TWU_cold', 'TWU_perCap', 'TWU_perCap_perDay'], index=bldgs)


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


print('Calculating results...')
# convert flowrates to gps, each second obs = amount of flow for that second
main['coldInFlowRate'] = main['coldInFlowRate'] / 60
main['hotInFlowRate'] = main['hotInFlowRate'] / 60
# replace pulse counts with 1, 1 pulse = 1 gal
main['hotOutFlowRate'] = main['hotOutFlowRate'].astype(bool).astype(int)
# calc hot water use by subtracting hotIn - hotOut.  Will resolve when resampling
main['hotWaterUse'] = main['hotInFlowRate'] - main['hotOutFlowRate']
main['totalWaterUse'] = main['hotWaterUse'] + main['coldInFlowRate']

#    presentWeek = main.loc['2019-10-20T00:00:00':'2019-10-27T00:00:00']
#    lastWeek  = main.loc['2019-10-13T00:00:00':'2019-10-20T00:00:00']

# calculate sum of rolling total water use; hot and cold
# hotWaterUse = main['hotWaterUse'].sum()
# coldInSum = main['coldInFlowRate'].sum()
totalWaterUse = main['totalWaterUse'].sum() / 2
hotUseSum = main['hotWaterUse'].sum() / 2
coldUseSum = main['coldInFlowRate'].sum() / 2

# generate graph of hourly use throughout day for last wee
main = main.resample(rule='1H', base=0).sum()
main = main.groupby(main.index.hour).mean()

# Present week stats
#    hotUseSum_pw = presentWeek['hotWaterUse'].sum()
#    coldInSum_pw = presentWeek['coldInFlowRate'].sum()
#    TotalWaterUse_pw = presentWeek['totalWaterUse'].sum()

# generate graph of hourly use throughout day for present week
#    presentWeek = presentWeek.resample(rule='1H', base=0).sum()
#    presentWeek = presentWeek.groupby(presentWeek.index.hour).mean()
# presentWeek['TotalWaterUse'] = presentWeek['hotWaterUse'] + presentWeek['coldInFlowRate']

# Last week stats
# hotUseSum_lw = lastWeek['hotWaterUse'].sum()
# coldInSum_lw = lastWeek['coldInFlowRate'].sum()
#    TotalWaterUse_lw = lastWeek['totalWaterUse'].sum()

# generate graph of hourly use throughout day for last week
#    lastWeek = lastWeek.resample(rule='1H', base=0).sum()
#    lastWeek = lastWeek.groupby(lastWeek.index.hour).mean()

bldgNum = bldgNUM('A')

#results.at[x, 'buildingID'] = x
results.at['A', 'totalWaterUse'] = totalWaterUse
results.at['A', 'TWU_perDay'] = totalWaterUse / 7
results.at['A', 'TWU_hot'] = hotUseSum
results.at['A', 'TWU_cold'] = coldUseSum
results.at['A', 'TWU_perCap'] = totalWaterUse / bldgNum
results.at['A', 'TWU_perCap_perDay'] = totalWaterUse / bldgNum / 7
# upload present week stats
#results.at[i, 'TWU_hot_pw'] = hotUseSum_pw
#results.at[i, 'TWU_cold_pw'] = coldInSum_pw
#results.at[i, 'TWU_pw'] = TotalWaterUse_pw
#results.at[i, 'TWU_pw_perDay'] = TotalWaterUse_pw/7
#results.at[i, 'TWU_pw_perCap'] = TotalWaterUse_pw/bldgNum
#results.at[i, 'TWU_pw_perCap_perDay'] = TotalWaterUse_pw/bldgNum/7
# upload previous week stats
#results.at[1, 'TWU_hot_lw'] = hotWaterUse_lw
#results.at[1, 'TWU_cold_lw'] = coldInSum_lw
#results.at[i, 'TWU_lw'] = TotalWaterUse_lw
#results.at[i, 'TWU_lw_perDay'] = TotalWaterUse_lw/7
#results.at[i, 'TWU_lw_perCap'] = TotalWaterUse_lw/bldgNum
#results.at[i, 'TWU_lw_perCap_perDay'] = TotalWaterUse_lw/bldgNum/7


#main['totalWaterUse'] = main['totalWaterUse'] / bldgNum

print('Plotting results...\n')
# Initialize figures and subplots

# create hour column to plot outside of index/datetime errors
hours = ['12:00am', '01:00am', '02:00am', '03:00am', '04:00am', '05:00am', '06:00am', '07:00am', '08:00am', '09:00am',
         '10:00am', '11:00am', '12:00pm',
         '01:00pm', '02:00pm', '03:00pm', '04:00pm', '05:00pm', '06:00pm', '07:00pm', '08:00pm', '09:00pm', '10:00pm',
         '11:00pm']
main['time'] = hours

plt.figure(figsize=(12, 6))
plt.plot('time', 'totalWaterUse', data=main, color='blue', linewidth=4, label='Baseline')
#    plt.plot('time', 'totalWaterUse', data=main, color='navy', alpha=0.35, label='Oct 13th - Oct 20th')
plt.title('Average Hourly Water Use: BLDG A', fontsize=18)
plt.xlabel('Time', fontsize=14)
plt.ylabel('Water Use (gal)', fontsize=14)
plt.xticks(rotation=45, fontsize=8)
plt.legend()
plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)

plt.savefig(filepath + 'waterUse_A.png')

plt.show()

print('Saving results to csv file...')
results.reset_index(inplace=True)
results.to_csv(filepath + 'waterUseResults_A.csv', encoding='utf-8', index=False)

print('done!')