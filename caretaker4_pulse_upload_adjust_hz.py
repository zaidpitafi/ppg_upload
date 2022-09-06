import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob

from tqdm import tqdm
from influxdb import InfluxDBClient
import operator
import scipy.signal as sg
import scipy as sp
import pytz
import subprocess
from datetime import datetime
from pdb import set_trace as st
import warnings
warnings.filterwarnings("ignore")


# This function write an array of data to influxdb. It assumes the sample interval is 1/fs.
# influx - the InfluxDB info including ip, db, user, pass. Example influx = {'ip': 'https://sensorweb.us', 'db': 'algtest', 'user':'test', 'passw':'sensorweb'}
# dataname - the dataname such as temperature, heartrate, etc
# timestamp - the epoch time (in second) of the first element in the data array, such as datetime.now().timestamp()
# fs - the sampling interval of readings in data
# unit - the unit location name tag
def write_influx(influx, unit, table_name, data_name, data, start_timestamp, fs):
    # print("epoch time:", timestamp)
    max_size = 100
    count = 0
    total = len(data)
    prefix_post  = "curl -s -POST \'"+ influx['ip']+":8086/write?db="+influx['db']+"\' -u "+ influx['user']+":"+ influx['passw']+" --data-binary \' "
    http_post = prefix_post
    for value in tqdm(data):
        count += 1
        http_post += "\n" + table_name +",location=" + unit + " "
        # st()
        http_post += data_name + "=" + str(int(round(value))) + " " + str(int(start_timestamp*10e8))
        start_timestamp +=  1/fs
        if(count >= max_size):
            http_post += "\'  &"
            # print(http_post)
            # print("Write to influx: ", table_name, data_name, count)
            subprocess.call(http_post, shell=True)
            total = total - count
            count = 0
            http_post = prefix_post
    if count != 0:
        http_post += "\'  &"
        # print(http_post)
        # print("Write to influx: ", table_name, data_name, count, data)
        subprocess.call(http_post, shell=True)





# This function converts the time string to epoch time xxx.xxx (second.ms).
# Example: time = "2020-08-13T02:03:00.200", zone = "UTC" or "America/New_York"
# If time = "2020-08-13T02:03:00.200Z" in UTC time, then call timestamp = local_time_epoch(time[:-1], "UTC"), which removes 'Z' in the string end
def local_time_epoch(time, zone):
    local_tz = pytz.timezone(zone)
    try:
        localTime = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
    except:
        localTime = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")

    local_dt = local_tz.localize(localTime, is_dst=None)
    # utc_dt = local_dt.astimezone(pytz.utc)
    epoch = local_dt.timestamp()
    # print("epoch time:", epoch) # this is the epoch time in seconds, times 1000 will become epoch time in milliseconds
    # print(type(epoch)) # float
    return epoch

# This function converts the epoch time xxx.xxx (second.ms) to time string.
# Example: time = "2020-08-13T02:03:00.200", zone = "UTC" or "America/New_York"
# If time = "2020-08-13T02:03:00.200Z" in UTC time, then call timestamp = local_time_epoch(time[:-1], "UTC"), which removes 'Z' in the string end
def epoch_time_local(epoch, zone):
    local_tz = pytz.timezone(zone)
    time = datetime.fromtimestamp(epoch).astimezone(local_tz).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return time


# This function read an array of data from influxdb.
# influx - the InfluxDB info including ip, db, user, pass. Example influx = {'ip': 'https://sensorweb.us', 'db': 'testdb', 'user':'test', 'passw':'sensorweb128'}
# dataname - the dataname such as temperature, heartrate, etc
# start_timestamp, end_timestamp - the epoch time (in second) of the first element in the data array, such as datetime.now().timestamp()
# unit - the unit location name tag
def read_influx(influx, unit, table_name, data_name, start_timestamp, end_timestamp):
    if influx['ip'] == '127.0.0.1' or influx['ip'] == 'localhost':
        client = InfluxDBClient(influx['ip'], '8086', influx['user'], influx['passw'], influx['db'],  ssl=influx['ssl'])
    else:
        client = InfluxDBClient(influx['ip'], '8086', influx['user'], influx['passw'], influx['db'],  ssl=influx['ssl'])

    # client = InfluxDBClient(influx['ip'].split('//')[1], '8086', influx['user'], influx['passw'], influx['db'],  ssl=True)
    query = 'SELECT "' + data_name + '" FROM "' + table_name + '" WHERE "location" = \''+unit+'\' AND time >= '+ str(int(start_timestamp*10e8))+' AND time < '+str(int(end_timestamp*10e8))
    # query = 'SELECT last("H") FROM "labelled" WHERE ("location" = \''+unit+'\')'

    # print(query)
    result = client.query(query)
    # print(result)

    points = list(result.get_points())
    values =  list(map(operator.itemgetter(data_name), points))
    times  =  list(map(operator.itemgetter('time'),  points))
    # print(times)
    # times = [local_time_epoch(item[:-1], "UTC") for item in times] # convert string time to epoch time
    # print(times)

    data = values #np.array(values)
    # print(data, times)
    return data, times


def get_epoch_time_list(date, time, zone):
    assert len(date) == len(time)
    local_tz = pytz.timezone(zone)
    epoch_time_list = []
    last_epoch = 0
    for i in range(date.shape[0]):
        time_str = date[i] + ' ' + time[i]

        localTime = datetime.strptime(time_str, "%m/%d/%y %H:%M:%S.%f")

        local_dt = local_tz.localize(localTime, is_dst=None)
        # utc_dt = local_dt.astimezone(pytz.utc)
        epoch = local_dt.timestamp()

        if i == 0:
            last_epoch = epoch
            count = 1
            epoch_time_list.append(epoch)

        if epoch == last_epoch:
            epoch_time_list.append(epoch)
            count += 1
        else:
            for j in range(len(epoch_time_list)-count+1, len(epoch_time_list)):
                epoch_time_list[j] = round(epoch_time_list[j-1] + (epoch - last_epoch) / count ,3)
            epoch_time_list.append(epoch)
            count = 1
            last_epoch = epoch
            # st()



    epoch_time_list.pop()

        # st()

    return epoch_time_list

def postHTTP(ip, db, user, password, mac, pulse_value, date_tmp):
    http_post = "curl -i -XPOST \'%s/write?db=%s\' -u %s:%s --data-binary \'" % (ip, db, user, password)
    ##only for the first row
    date_tmp = str(int(date_tmp*1000000*1000))
    # add_ms = int(ms)-int(ms_start)
    # date_tmp = str(date_start + add_ms*1000000) # notice six 0s are needed
    http_post += "\nfingerPulse,location=%s pulse=%s" %(mac, pulse_value)
    http_post += " " + date_tmp
    http_post += "\'"

    subprocess.call(http_post, shell=True,stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    # subprocess.call()
    # print(http_post)


def main():

    dest = {'ip':'https://sensorweb.us', 'db':'caretaker4', 'user':'admin', 'passw':'sensorweb128'}


    # mac = 'b8:27:eb:d4:ca:46'

    # now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

    # start_time = now
    # # end_time = '2021-08-04T15:20:00.000'

    # duration = 10
    # fs = 100

    # start_epoch = int(local_time_epoch(now, "America/New_York")) - 1
    # end_epoch = start_epoch + 1

    # influx_raw = {'ip': 'sensorweb.us', 'db': 'shake', 'user':'admin', 'passw':'sensorweb128','ssl':True}
    # # st()
    # # while end_epoch < local_time_epoch(end_time, "America/New_York"):

    # plt.figure()
    # plt.ion()
    # data_ = []
    # while True:
    #     # print(1)
    #     each_data, _ = read_influx(influx_raw, unit=mac, table_name="Z", data_name="value", start_timestamp=start_epoch, end_timestamp=end_epoch)
    #     start_epoch = end_epoch
    #     end_epoch = end_epoch + 1
    #     data_ = data_ + each_data
    #     plt.cla()
    #     if len(data_) < fs*duration:
    #         plt.plot(data_)
    #     else:
    #         plt.plot(data_[-1000:])
    #     plt.pause(0.1)
    #     # st()


    # dd = extract_data_from_influxdb(ip='sensorweb.us',
    #                             port='8086',
    #                             unit='b8:27:eb:d4:ca:46',
    #                             user_id='admin',
    #                             pass_word='sensorweb128',
    #                             db_name='shake',
    #                             stampIni='1628101837',
    #                             stampEnd='1628102837')

    # st()
    # pulse_waveform = pd.read_csv('../data/Log0804/Log-0804_pulseWaveform_2021-08-04_16-22-18.csv')
    # pulse_waveform = pd.read_csv('../data/20210205/02052021_1230-1_pulseWaveform_2021-02-05_12-43-28.csv')

    pulse_waveform_file_list = np.sort(glob('./Pulsewave/*.csv'))
    # st()

    for ind, pulse_waveform_path in enumerate(pulse_waveform_file_list):
        print(f'{ind}/{len(pulse_waveform_file_list)}')
        print(f'Now processing {pulse_waveform_path}')
        pulse_waveform = pd.read_csv(pulse_waveform_path)

        pulse_value = pulse_waveform['Value']
        date_ = pulse_waveform['Date']
        time_ = pulse_waveform['Time']

        epoch_time_list = get_epoch_time_list(date_, time_, "America/New_York")
        # st()

        assert len(pulse_value) == len(epoch_time_list)

        hz = int(len(pulse_value) / (epoch_time_list[-1]-epoch_time_list[0]))
        # st()
        write_influx(dest, 'ca:re:ta:ke:za:id', 'fingerPulse', 'pulse', pulse_value, epoch_time_list[0], fs=31.25)
        #read_influx(dest, 'ca:re:ta:ke:za:id', 'fingerPulse', 'pulse', epoch_time_list[0], epoch_time_list[-1])




        # for i in range(len(epoch_time_list)):
        #     # if i >= 100:
        #     #     break
        # # for i in tqdm(range(len(epoch_time_list))):

        # # for i in range(len(epoch_time_list)):
        #     # st()
        #     # postHTTP(ip='https://sensorweb.us:8086', db='caretaker4', user='admin', password='sensorweb128', mac='ca:re:ta:ke:ct:ru', pulse_value=pulse_value[i], date_tmp=epoch_time_list[i])
            



        #     # postHTTP(ip='http://localhost:8086', db='caretaker4', user='admin', password='sensorweb128', mac='ca:re:ta:ke:ct:ru', pulse_value=pulse_value[i], date_tmp=epoch_time_list[i])
        #     postHTTP(ip='http://localhost:8086', db='caretaker4', user='admin', password='sensorweb128', mac='ca:re:ta:ke:pu:da', pulse_value=pulse_value[i], date_tmp=epoch_time_list[i])

        #     # postHTTP(pulse_value=pulse_value[i], date_tmp=epoch_time_list[i])


        # st()
        # plt.figure()

        # plt.ion()
        # for i in range(8000,pulse_value.shape[0]):
        #     plt.cla()
        #     plt.plot(pulse_value[i:i+300])
        #     plt.ylim(-400,500)
        #     # plt.show()
        #     plt.pause(0.1)



        # st()

if __name__ == '__main__':
    main()
