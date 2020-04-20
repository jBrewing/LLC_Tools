# Import libraries
import pandas as pd
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient
from tempQC.calibrationFunctions import caliFact


# Set date range
print('Receiving inputs...\n')
beginDate = "'2019-03-22T12:00:00Z'" # Beginning of POR
endDate = "'2019-04-19T12:00:00Z'" # End of POR
# Input building ID to viz
bldgIDInput1 = input("Input building ID: ").upper()
bldgIDQ1 = "'" + bldgIDInput1 + "'"

# Connect to InfluxDB
print('\nConnecting to influx database...')
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws') # Switch to CIWS database


print('Assembling query...')
query = """SELECT "hotInTemp", "hotOutTemp", "coldInTemp" FROM "flow" WHERE "buildingID" ="""+bldgIDQ1+""" AND time >= """+beginDate+""" AND time <= """+endDate+""""""

#Client.query returns ResultSet.  Convert to pandas dataframe with list
print('Retrieving temp data to QC...')
results = client.query(query)
df = pd.DataFrame(list(results.get_points(measurement='flow')))
df['time'] = pd.to_datetime(df['time']) # Convert 'time' to pandas datetime format
df.set_index('time', inplace=True) # Set index to 'time'

# Make copy of df to compare new values with old
df2 = df.copy()

# Add calibration factor to each value
print('Calculating adjusted values for bldg '+bldgIDInput1+'...')
columns = ['hotInTemp', 'hotOutTemp','coldInTemp']
for (column, cal) in zip(columns, caliFact(bldgIDInput1)):
    df2[column] = df2[column] + cal

print('Printing results...')
fig=plt.figure(figsize=(12,8))
fig.autofmt_xdate()

axHotTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotTemp.plot(df['hotInTemp'], color='red', label='hotIn_'+bldgIDInput1+'_new')
axHotTemp.plot(df2['hotInTemp'], color='maroon', label='hotIn_'+bldgIDInput1, linestyle='dashed')
axHotTemp.set_xlim(beginDate, endDate)
axHotTemp.set_ylabel('Temp (C)')
axHotTemp.set_xlabel('Date')
axHotTemp.legend(loc='lower right')
axHotTemp.set_title('HOT IN '+bldgIDInput1+' Old  vs. New', fontsize=10, weight ='bold')

fig2 = plt.figure(figsize=(12,8))
fig2.autofmt_xdate()
axHotOut = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axHotOut.plot(df['hotOutTemp'], color = 'red', label = 'hotOut'+bldgIDInput1+'_new')
axHotOut.plot(df2['hotOutTemp'], color='maroon', label = 'hotOut_'+bldgIDInput1, linestyle='dashed')
axHotOut.set_xlim(beginDate, endDate)
axHotOut.set_ylabel('Temp (C)')
axHotOut.legend(loc='lower right')
axHotOut.set_title('HOT OUT '+bldgIDInput1+' Old  vs. New', fontsize=10, weight ='bold')

fig3 = plt.figure(figsize=(12,8))
fig3.autofmt_xdate()
axColdTemp = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
axColdTemp.plot(df['coldInTemp'], color = 'blue', label = 'coldIn_'+bldgIDInput1+'_new')
axColdTemp.plot(df2['coldInTemp'], color = 'navy', label = 'coldIn_'+bldgIDInput1, linestyle='dashed')
axColdTemp.set_xlim(beginDate, endDate)
axColdTemp.set_ylim(5, 25)
axColdTemp.set_ylabel('Temp (C)')
axColdTemp.legend(loc='lower right')
axColdTemp.set_title('COLD IN '+bldgIDInput1+' Old vs. New', fontsize=10, weight ='bold')

fig4 = plt.figure(figsize=(12,8))
fig4.autofmt_xdate()
finalPlot = plt.subplot()
plt.xticks(fontsize=8, rotation=35)
finalPlot.plot(df['hotInTemp'], color = 'red', label = 'hotIn_final')
finalPlot.plot(df['hotOutTemp'], color = 'maroon', label = 'hotOut_final', linestyle='dashed')
finalPlot.set_xlim(beginDate, endDate)
finalPlot.set_ylabel('Temp (C)')
finalPlot.legend(loc='lower right')
finalPlot.set_title('New HotIn_'+bldgIDInput1+' vs. New HotOut_'+bldgIDInput1, fontsize=10, weight ='bold')


plt.tight_layout(pad=5, w_pad=2, h_pad=2.5)
plt.show()


print('done')

