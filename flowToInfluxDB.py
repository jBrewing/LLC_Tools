import pandas as pd
from influxdb import DataFrameClient

bldg = input('BLDG ID?: ').upper()
print('\nlocating file...')


# Used for testing connection.
#path="/Users/joseph/Desktop/GRA/InfluxSemesterProject/"
#file = "test.csv"

path="/Users/joseph/Documents/Datalog_Backups/LLC_BLDG_F/"
file = "multi_meter_datalog_LLC_BLDG_F_2018-10-4_10-26-44.csv"

print('\nfile located, building dataframe...')

csvReader = pd.read_csv(path+file, sep=',', header=1, index_col=0, parse_dates=True, infer_datetime_format=True)
csvReader.drop(['RecordNumber','coldInVoltage', 'coldInVolume','hotInVoltage', 'hotInVolume', 'hotOutVolume'],
        axis=1, inplace=True)

print('\ndataframe complete, connecting to influxDB...')

client = DataFrameClient(host='influxdbubuntu.bluezone.usu.edu', port=8086)
client.switch_database('LLCBackup')

print('\nconnection established, uploading to influxdb...')

client.write_points(csvReader, 'LLC', {'buildingID':bldg}, batch_size=2000, protocol='line')

print('\n\nDONE!')