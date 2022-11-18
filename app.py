# To run streamlit, go to terminal and type: 'streamlit run app.py'
# Core Packages ###########################
import streamlit as st
import os

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
import datetime as dt
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel

#######
import memory_producer
import memory_consumer
import process_producer
import process_consumer
#######

project_title = "Detecting Linux System Hacking Activities"
st.set_page_config(page_title=project_title, initial_sidebar_state='collapsed')
#######################################################################################################################

def main():
    st.title(project_title)
    st.write("Investigating the open data from the Cyber Range Labs of UNSW Canberra and build models based on the data to identify abnormal system behaviour. In addition, integrate the machine learning models into the streaming platform using Apache Kafka and Apache Spark Streaming to detect any real-time threats, in order to stop the hacking.")
    st.write("Source Project: https://github.com/RakeshNain/Cyber-Security-Application---Detecting-Linux-system-hacking-activities")
    st.markdown("***")
    st.subheader("")
#########################################
    # NOTE: Step 1: Run Memory and Process producers
    st.write("Step 1: Run Memory and Process producers")
    memory_producer.memory_produce()
    process_producer.process_produce()
    # NOTE: Step 2: Run Memory and Process Consumers
    st.write("Step 2: Run Memory and Process Consumers")
    memory_consumer.memory_consume()
    process_consumer.process_consume()

    # NOTE: Step 3: Create SparkSession
    st.write("Create SparkSession")
    # local[2]: run Spark in local mode with 2 working processors as logical cores on your machine
    master = "local[2]"
    # The `appName` field is a name to be shown on the Spark cluster UI page
    app_name = "Assignment-2B-Task3"
    # Setup configuration parameters for Spark
    spark_conf = SparkConf().setMaster(master).setAppName(app_name)

    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    st.write("Ingest data into Spark streaming")
    # NOTE: Step 4.1: ingest the streaming data into Spark Streaming for memory
    topic1 = "Streaming_Linux_memory5"
    linux_memory = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", topic1) \
        .load()
    # NOTE: STEP 4.2: ingest the streaming data into Spark Streaming for process
    topic2 = "Streaming_Linux_process7"
    linux_process = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", topic2) \
        .load()
############################################
    # showing data from memory producer
    query_memory = linux_memory \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("linux_memory_tab") \
        .trigger(processingTime='5 seconds')

    # showing data from memory producer
    query_process = linux_process \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("linux_process_tab") \
        .trigger(processingTime='5 seconds')
############################################

    if st.button("Show Memory data stream"):
        query_memory.start()
        spark.sql("select * from linux_memory_tab").show()
    if st.button("Show Process data stream"):
        query_process.start()
        spark.sql("select * from linux_process_tab").show()
    if st.button("Stop Memory data stream"):
        query_memory.stop()
    if st.button("Stop Process data stream"):
        query_process.stop()

    st.markdown("***")


if __name__ == '__main__':
    main()

# To run streamlit, go to terminal and type: 'streamlit run app-source.py'