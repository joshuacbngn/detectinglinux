# import statements
from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
import datetime as dt
import csv


def read_csv(fileName):
    data = []
    with open(fileName, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data.append({'sequence': int(row['sequence']), 'machine': int(row['machine']), 'PID': int(row['PID']),
                         'TRUN': int(row['TRUN']), 'TSLPI': int(row['TSLPI']), 'TSLPU': int(row['sequence']),
                         'POLI': row['POLI'], 'NICE': int(row['NICE']), 'PRI': int(row['PRI']),
                         'RTPR': int(row['RTPR']), 'CPUNR': int(row['CPUNR']), 'Status': row['Status'],
                         'EXC': int(row['EXC']), 'State': row['State'], 'CPU': int(float(row['CPU'])),
                         'CMD': row['CMD']})

    return data


def publish_message(producer_instance, topic_name, data):
    try:
        producer_instance.send(topic_name, data)
        print('Message published successfully. Data: ' + str(data))
    except Exception as ex:
        print('Exception in publishing message.')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x: dumps(x).encode('ascii'),
                                  api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


if __name__ == '__main__':

    topic = 'Streaming_Linux_process7'
    cRows = read_csv('Streaming_Linux_process.csv')

    print('Publishing records..')
    producer = connect_kafka_producer()

    machines_set = set()
    for a in cRows:
        machines_set.add(a['machine'])

    machine_list = list(machines_set)

    g = globals()
    for m_num in machine_list:
        g['machine_{0}'.format(m_num)] = []

    for m_num in machine_list:
        for row in cRows:
            if row['machine'] == m_num:
                g['machine_{0}'.format(m_num)].append(row)

    data_machine = []
    for m_num in machine_list:
        data_machine.append(g['machine_{0}'.format(m_num)])

    start_list = [0] * len(data_machine)
    final_produced_data = []
    i = 0
    while True:
        produced_data = []
        for i in range(len(data_machine)):
            rand_n = random.randint(10, 50)
            if start_list[i] + rand_n < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data.append(a)

            start_list[i] = start_list[i] + rand_n

        publish_message(producer, topic, produced_data)
        sleep(5)

def process_produce():
    topic = 'Streaming_Linux_process7'
    cRows = read_csv('Streaming_Linux_process.csv')

    print('[process]Publishing records..')
    producer = connect_kafka_producer()
    print('Process producer created')
    machines_set = set()
    for a in cRows:
        machines_set.add(a['machine'])

    machine_list = list(machines_set)

    g = globals()
    for m_num in machine_list:
        g['machine_{0}'.format(m_num)] = []

    for m_num in machine_list:
        for row in cRows:
            if row['machine'] == m_num:
                g['machine_{0}'.format(m_num)].append(row)

    data_machine = []
    for m_num in machine_list:
        data_machine.append(g['machine_{0}'.format(m_num)])

    start_list = [0] * len(data_machine)
    final_produced_data = []
    i = 0
    while True:
        produced_data = []
        for i in range(len(data_machine)):
            rand_n = random.randint(10, 50)
            if start_list[i] + rand_n < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data.append(a)

            start_list[i] = start_list[i] + rand_n

        publish_message(producer, topic, produced_data)
        sleep(5)