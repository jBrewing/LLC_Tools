import pandas as pd
from influxdb import DataFrameClient


print('\nlocating file...')
path="/Users/joseph/Desktop/"
file = "multi_meter_datalog_LLC_BLDG_A_2019-11-7_0-0-0.csv"

print('\nfile located, building dataframe...')

csvReader = pd.read_csv(path+file, sep=',', header=1, index_col=0, parse_dates=True, infer_datetime_format=True)
csvReader['buildingID'] = 'A'
print('\ndataframe complete, connecting to influxDB...')



print('\nconnection established, uploading to influxdb...')
# WritePoints
print('Connecting to database...')
clientdf = DataFrameClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
clientdf.switch_database('ciws')
print('Writing points...')
clientdf.write_points(dataframe=csvReader, measurement='flow',
                    field_columns={'hotInFlowRate':csvReader[['hotInFlowRate']],
                                   'coldInFlowRate': csvReader[['coldInFlowRate']],
                                   'hotOutFlowRate': csvReader[['hotOutFlowRate']],
                                   'hotInTemp': csvReader[['hotInTemp']],
                                   'coldInTemp': csvReader[['coldInTemp']],
                                   'hotOutTemp': csvReader[['hotOutTemp']]
                                  # 'hotWaterUse':mainFinal[['hotWaterUse']],
                                  # 'hotWaterUse_fixed':mainFinal[['hotWaterUse_fixed']]
                                   },
                    tag_columns={'buildingID': csvReader[['buildingID']]},
                    protocol='line', numeric_precision=10, batch_size=2000)

print('\n\nDONE!')