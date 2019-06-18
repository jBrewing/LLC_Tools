import paramiko

date = input("Date: ")
time = input("time: ")

dest = 'B'
hostdest = dest.lower()

host = 'rpi-llc-'+hostdest+'-wlan.bluezone.usu.edu'
un = 'pi'
password ='p1w4t3r'
remote ='/home/pi/CampusMeter/multi_meter_datalog_LLC_BLDG_'+dest+'_2018-11-'+date+'_'+time+'.csv'
local ='/Users/joseph/Desktop/GRA/Datalog_Backups/LLC_BLDG_'+dest+'/multi_meter_datalog_LLC_BLDG_'+dest+'_2018-10-'+date+'_'+time+'.csv'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=un, password=password)

ftp = client.open_sftp()
ftp.get(remote, local)
ftp.close()





#host = 'rpi-llc-c-wlan.bluezone.usu.edu'
#port = 22

#transport = paramiko.Transport((host, port))
#transport.connect(username = 'pi', password = 'p1w4t3r')
#sftp = paramiko.SFTPClient.from_transport(transport)

#sftp.get(source,target)
#sftp.close()
#transport.close()

