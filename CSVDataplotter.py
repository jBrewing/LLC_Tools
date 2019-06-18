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

# take inputs
bldg = input('\nbuilding ID:  ').upper()

# print options for plotting
print('\n')


filepath = "/Users/joseph/Documents/Datalog_backups/LLC_BLDG_"+bldg+"/"


dirs = os.listdir(filepath)
print(dirs)
print('\n')

dateID = input('date:  ')
timeID = input('time: ')

hotin = 'hotInFlowRate'
coldin = 'coldInFlowRate'
hotout = 'hotOutVolume'


file = "multi_meter_datalog_LLC_BLDG_" + bldg+"_"+dateID+"_"+ timeID+".csv"

print('\n fetching file from:\n')
print(filepath+file)
print('\n building data frame, please hold...')

df_main = pd.read_csv(filepath + file, header=1, sep=',', index_col=0, parse_dates=True,
                      infer_datetime_format=True, low_memory=False)


gridsize = (3,2)
fig = plt.figure(figsize=(12,8))
ax1 = plt.subplot2grid(gridsize, (0,0), colspan =2)
ax2 = plt.subplot2grid(gridsize, (1,0), colspan =2)
ax3 = plt.subplot2grid(gridsize, (2,0), colspan =2)

ax1.plot(df_main[hotin], label='HotInFlow')
ax2.plot(df_main[coldin], label='ColdInFlow')
ax3.plot(df_main[hotout], label='hotReturn')

ax1.set_title(file)
plt.show()

print('\n\ndone')