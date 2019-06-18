import pandas as pd
import matplotlib.pyplot as plt

filepath = "/Users/joseph/Desktop/"
file = "multi_meter_datalog_LLC_BLDG_B_2019-1-30_18-47-46.csv"

df_main = pd.read_csv(filepath+file, header=1, index_col=0, parse_dates=True, infer_datetime_format=True)

hotFlow = df_main['hotInFlowRate']
coldFlow = df_main['coldInFlowRate']
returnFlow = df_main['hotOutPulseCount']

gridsize = (3,1)
fig = plt.figure(figsize=(12,8))
fig.suptitle('Water Temp Measurment Dist. BLDG: E')


ax1 = plt.subplot2grid(gridsize, (0,0))
ax1.plot(hotFlow, color='red')
ax1.set_xlabel('Temp (Celsius)')
ax1.set_ylabel('Freq.')
ax1.set_title('Hot water supply')

ax2 = plt.subplot2grid(gridsize, (1,0))
ax2.plot(coldFlow, color = 'blue')
ax2.set_xlabel('Temp (Celsius)')
ax2.set_ylabel('Freq.')
ax2.set_title('Cold water supply')

ax3 = plt.subplot2grid(gridsize,(2,0))
ax3.plot(returnFlow, color = 'black')




plt.show()
print('done')