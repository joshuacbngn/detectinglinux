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
                         'MINFLT': int(row['MINFLT']), 'MAJFLT': int(row['MAJFLT']), 'VSTEXT': int(row['VSTEXT']),
                         'VSIZE': int(float(row['VSIZE'])), 'RSIZE': int(float(row['RSIZE'])),
                         'VGROW': int(float(row['VGROW'])), 'RGROW': int(float(row['RGROW'])),
                         'MEM': int(float(row['MEM'])), 'CMD': row['CMD']})

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
        print('***KAFKA MEMORY PRODUCER SUCCESSFUL***')
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


if __name__ == '__main__':

    topic = 'Streaming_Linux_memory5'
    cRows = read_csv('Streaming_Linux_memory.csv')

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
    iteration = 0
    X = []
    Y = [[], []]
    while True:
        produced_data = []
        produced_data_small = []
        for i in range(len(data_machine)):
            rand_n = random.randint(20, 80)
            if start_list[i] + rand_n < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data.append(a)
            start_list[i] = start_list[i] + rand_n

            rand_n_small = random.randint(0, 5)
            if start_list[i] + rand_n_small < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n_small]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n_small]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data_small.append(a)
            start_list[i] = start_list[i] + rand_n_small

        if iteration == 0:
            X = produced_data
            publish_message(producer, topic, X)
            Y[0] = produced_data_small

        else:
            Y[1] = Y[0] + produced_data_small
            X = produced_data
            X_plus_Y = X + Y[0]
            publish_message(producer, topic, X_plus_Y)
            Y[0] = Y[1]

        iteration = iteration + 1
        sleep(10)

def memory_produce():

    topic = 'Streaming_Linux_memory5'
    cRows = read_csv('Streaming_Linux_memory.csv')

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
    iteration = 0
    X = []
    Y = [[], []]
    while True:
        produced_data = []
        produced_data_small = []
        for i in range(len(data_machine)):
            rand_n = random.randint(20, 80)
            if start_list[i] + rand_n < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data.append(a)
            start_list[i] = start_list[i] + rand_n

            rand_n_small = random.randint(0, 5)
            if start_list[i] + rand_n_small < len(data_machine[i]):
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n_small]
            else:
                start_list[i] = 0
                selected_data = data_machine[i][start_list[i]:start_list[i] + rand_n_small]

            for a in selected_data:
                a['ts'] = round(dt.datetime.utcnow().timestamp())
                produced_data_small.append(a)
            start_list[i] = start_list[i] + rand_n_small
        print(f"iteration: {iteration}")
        if iteration == 0:
            X = produced_data
            print("////////////// before publish message //////////////")
            publish_message(producer, topic, X)
            print("////////////// after publish message //////////////")
            Y[0] = produced_data_small

        else:
            Y[1] = Y[0] + produced_data_small
            X = produced_data
            X_plus_Y = X + Y[0]
            print("////////////// before publish message //////////////")
            publish_message(producer, topic, X_plus_Y)
            print("////////////// after publish message //////////////")
            Y[0] = Y[1]

        iteration = iteration + 1
        print("memory produce done")
        sleep(10)
