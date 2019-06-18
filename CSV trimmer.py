import pandas as pd

path = "/Users/joseph/Documents/Datalog_backups/LLC_BLDG_F/"
file = "multi_meter_datalog_LLC_BLDG_F_2018-12-14_12-44-12.csv"

main = pd.read_csv(path+file, header=1, infer_datetime_format=True, sep=',', index_col=0,
                   parse_dates=True, low_memory=True)

main.drop(['RecordNumber','coldInVoltage', 'coldInVolume','hotInVoltage', 'hotInVolume', 'hotOutVolume','Unnamed: 14'],
        axis=1, inplace=True)

begin='2018-12-12T00:00:00Z'
end = '2018-12-13T00:00:00Z'

main.between_time(begin, end)

print('done')