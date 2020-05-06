import pandas as pd
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import numpy as np

# accept inputs
print('Receiving inputs...\n')
#    building ID
#bldgIDInput1 = input("Input building ID: ").upper()
#bldgIDQ1 = "'" + bldgIDInput1 + "'"
#    dates
beginDate = "'2019-03-22T12:00:00Z'"
endDate = "'2019-04-19T12:00:00Z'"


# Retrieve data
#   connect to database
print('\nConnecting to database...')
# Create client object with InfluxDBClient library
# Set database.
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws_final')

#for bldg in bldgs:
#   write query
print('Assembling data query...')
# Build query by concatenating inputs into query.  InfluxDB query language has several
# requirements.  Fields/Tags must be bracketed with " " and the field/tag values must be bracketed with ' '.
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.

query = """
SELECT * 
FROM "WaWRE" 
WHERE time >= """+beginDate+""" AND time <= """+endDate+"""
"""

print('Retrieving data...')
# Query returns a 'ResultSet" type.  Have to convert to pandas dataframe.
# Convert returned ResultSet to Pandas dataframe with list and get_points.
results = client.query(query)
df = pd.DataFrame(list(results.get_points(measurement='WaWRE')))

# Set dataframe index as datetime.
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)

print('Data retrieved! \n')

print('Resampling data...')

df['diff'] = df['hotUse_energy'] + df['pipeLoss_energy']

print('Resampling data...')
B = df[df['buildingID'] == 'B'] # Divide dataframe by building
C = df[df['buildingID'] == 'C']
D = df[df['buildingID'] == 'D']
E = df[df['buildingID'] == 'E']
F = df[df['buildingID'] == 'F']

B = B.groupby(B.index.weekday).sum() # Group by timestamp index into day
C = C.groupby(C.index.weekday).sum()
D = D.groupby(D.index.weekday).sum()
E = E.groupby(E.index.weekday).sum()
F = F.groupby(F.index.weekday).sum()

dfs = [B,C,D,E,F]# Group by timestamp index into day
time_day = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun']


# plot each bldgs daily water use as a barplot
gridsize = (5,1)
fig = plt.figure(1, figsize=(8,10), tight_layout=True)

x = np.arange(len(time_day))
width = 0.2


ax1 = plt.subplot2grid(gridsize, (0,0))
p1 = ax1.bar(x=x-width, height=B['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red')
p2 = ax1.bar(x=x, height=B['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue')
ax1.set_ylabel("Bldg B \n Volume (m^3)",fontsize = 14)
ax1.set_title('Hot and Cold Water Use', fontsize =14 )
ax1.legend()
plt.xticks([])
plt.yticks(fontsize=12)


ax2 = plt.subplot2grid(gridsize, (1,0))
p1 = ax2.bar(x=x-width, height=C['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red')
p2 = ax2.bar(x=x, height=C['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue')
ax2.set_ylabel("Bldg C \n Volume (m^3)",fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)


ax3 = plt.subplot2grid(gridsize, (2,0))
p1 = ax3.bar(x=x-width, height=D['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use', color='red')
p2 = ax3.bar(x=x, height=D['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue')
ax3.set_ylabel("Bldg D \n Volume (m^3)",fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)


ax4 = plt.subplot2grid(gridsize, (3,0))
p1 = ax4.bar(x=x-width, height=E['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use',color='red')
p2 = ax4.bar(x=x, height=E['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue')
ax4.set_ylabel("Bldg E \n Volume (m^3)", fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)

ax5 = plt.subplot2grid(gridsize, (4,0))
p1 = ax5.bar(x=x-width, height=F['hotWaterUse'],
            width=width, capsize=5, label = 'Hot Water Use',color='red')
p2 = ax5.bar(x=x, height=F['coldWaterUse'],
            width=width, capsize=5, label = 'Cold Water Use', color='blue')
ax5.set_xlabel("Day", fontsize = 16, weight ='bold')
ax5.set_ylabel("Bldg F \n Volume (m^3)", fontsize = 14)
plt.grid(False, axis = 'x')
plt.xticks(np.arange(7),('Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun'),fontsize=14)
plt.yticks(fontsize=12)


plt.tight_layout()
plt.show()


for df in dfs:
    print('Plotting results...')
    # Create x-axis ticks to plot
    time = ['Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat','Sun']

    fig, ax = plt.subplots(figsize=(14,6))
    x = np.arange(len(time))
    width = 0.2

    p1 = ax.bar(x=x-width, height=results['B'],
                width=width, capsize=5, label = 'Bldg B')
    p2 = ax.bar(x=x, height=results['D'],
                width=width, capsize=5, label = 'Bldg D')
    p3 = ax.bar(x=x+width, height=results['E'],
                width=width, capsize=5, label = 'Bldg E')


    ax.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
    ax.set_xlabel('Day', fontsize=16)
    ax.set_ylabel('Water Use (gal)', fontsize=16)
    ax.set_xticks(x)
    ax.set_xticklabels(time)
    ax.legend()
    ax.set_title('Daily Hot Water Use', fontsize =18)
    ax.tick_params(labelsize='large')



# Save fig
os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dataPresentation/figures/')
plt.savefig('DailyWaterUse.png')


plt.show()

print('done')
