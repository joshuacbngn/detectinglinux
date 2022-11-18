# Import SparkConf class into program
import sys

from pyspark import SparkConf

# Import SparkContext and SparkSession classes
from pyspark import SparkContext  # Spark
from pyspark.sql import SparkSession  # Spark SQL

from pyspark.sql.types import *

from pyspark.sql.functions import *

import matplotlib.pyplot as plt

from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.mllib.evaluation import BinaryClassificationMetrics as bcm
import pandas as pd

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel

from pyspark.ml.clustering import KMeans

from sklearn.metrics import roc_curve, auc
def strip_mk(s):
    striped_s = s.replace('K', '000').replace("M", "000000").replace("G", "000000000").replace(" ", "")
    return float(striped_s)

def strip_mk_int(s):
    striped_s = s.replace('K', '000').replace("M", "000000").replace("G", "000000000").replace(" ", "")
    return int(striped_s)

def strip_gt_lt(s):
    striped_s = s.lstrip("<").rstrip(">")
    return striped_s

def main():
    print("*****DATA PREPARATION AND EXPLORATION*****")

    # local[*]: run Spark in local mode with as many working processors as logical cores on your machine
    master = "local[3]"

    # The `appName` field is a name to be shown on the Spark cluster UI page
    app_name = "Machine learning models to find hackers"

    # Setup configuration parameters for Spark
    spark_conf = SparkConf().setMaster(master).setAppName(app_name)


    # Using SparkSession
    spark = SparkSession.builder.config(conf=spark_conf).config("spark.sql.files.maxPartitionBytes", 31000000).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    process_dtype = [("ts", IntegerType()), ("PID", IntegerType()), ("TRUN", IntegerType()),
                     ("TSLPI", IntegerType()), ("TSLPU", IntegerType()), ("POLI", StringType()),
                     ("NICE", IntegerType()), ("PRI", IntegerType()), ("RTPR", IntegerType()),
                     ("CPUNR", IntegerType()), ("Status", StringType()), ("EXC", IntegerType()),
                     ("State", StringType()), ("CPU", DoubleType()), ("CMD", StringType()),
                     ("attack", IntegerType()), ("type", StringType())]

    memory_dtype = [("ts", IntegerType()), ("PID", IntegerType()), ("MINFLT", StringType()),
                    ("MAJFLT", StringType()), ("VSTEXT", StringType()), ("VSIZE", StringType()),
                    ("RSIZE", StringType()), ("VGROW", StringType()), ("RGROW", StringType()),
                    ("MEM", StringType()), ("CMD", StringType()), ("attack", IntegerType()),
                    ("type", StringType())]

    process_fields = [StructField(field_name[0], field_name[1], True) for field_name in process_dtype]
    memory_fields = [StructField(field_name[0], field_name[1], True) for field_name in memory_dtype]
    process_schema = StructType(process_fields)
    memory_schema = StructType(memory_fields)

    print("Attempting spark.read.csv")
    linux_process = spark.read.csv('linux_process_*.csv', header=True, schema=process_schema)
    print("Attempting spark.read.csv for process done")
    linux_memory = spark.read.csv('linux_memory_*.csv', header=True, schema=memory_schema)
    linux_process_updated = spark.read.csv('linux_process_*.csv', header=True, schema=process_schema)
    linux_memory_updated = spark.read.csv('linux_memory_*.csv', header=True, schema=memory_schema)

    linux_process = linux_process.cache()
    print("process data total rows: ", linux_process.count())
    linux_memory = linux_memory.cache()
    print("memory data total rows: ", linux_memory.count())

    print("Null values in Process Data:")
    linux_process.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in linux_process.columns]).show()
    print("Null values in Memory Data:")
    linux_memory.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in linux_memory.columns]).show()

    strip_mk_udf = udf(strip_mk, FloatType())
    strip_mk_int_udf = udf(strip_mk_int, IntegerType())
    strip_gt_lt_udf = udf(strip_gt_lt, StringType())

    linux_memory_cleaned = linux_memory_updated.withColumn('MAJFLT', strip_mk_udf('MAJFLT')).withColumn('MINFLT',
                                                                                                        strip_mk_int_udf(
                                                                                                            'MINFLT')). \
        withColumn('VSTEXT', strip_mk_udf('VSTEXT')).withColumn('RSIZE', strip_mk_udf('RSIZE')). \
        withColumn('VGROW', strip_mk_udf('VGROW')).withColumn('RGROW', strip_mk_udf('RGROW')). \
        withColumn('MEM', strip_mk_udf('MEM')).withColumn('VSIZE', strip_mk_udf('VSIZE')).withColumn('CMD',
                                                                                                     strip_gt_lt_udf(
                                                                                                         'CMD'))

    linux_process_cleaned = linux_process_updated.withColumn('CMD', strip_gt_lt_udf('CMD'))
    linux_memory_cleaned = linux_memory_cleaned.cache()
    linux_process_cleaned = linux_process_cleaned.cache()
    print("Null values in Memory Data:")
    linux_memory_cleaned.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in linux_memory_cleaned.columns]).show()
    print("Null values in Process Data:")
    linux_process_cleaned.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in linux_process_cleaned.columns]).show()

    print('NULL VALUES PROCESSED / DATA CLEANED')
    check1 = input('Process done. Next stage: Data Exploration \nContinue? Y/N')
    if check1.lower() == 'n':
        sys.exit()

    print("*****EXPLORING THE DATA*****")

    memory_attack = linux_memory_cleaned.groupby('attack').agg(count(linux_memory_cleaned.attack).alias('Total'))
    print("memory attack:")
    memory_attack.show()
    print("process attack:")
    process_attack = linux_process_cleaned.groupby('attack').agg(count(linux_process_cleaned.attack).alias('Total'))
    process_attack.show()

    print("memory numeric features")
    linux_memory_cleaned.toPandas().describe().round(2)
    print("process numeric features")
    linux_process_cleaned.toPandas().describe().round(2)

    print("memory non-numeric features")
    linux_memory_cleaned.groupby('CMD').agg(count(linux_memory_cleaned.CMD).alias('Total CMD')).sort(col('Total CMD'), ascending=False).show(10)
    linux_memory_cleaned.groupby('type').agg(count(linux_memory_cleaned.type).alias('Total type')).sort(col('Total type'), ascending=False).show(10)
    print("process non-numeric features")
    linux_process_cleaned.groupby('State').agg(count(linux_process_cleaned.State).alias('Total')).sort(col('Total'), ascending=False).show(10)
    linux_process_cleaned.groupby('Status').agg(count(linux_process_cleaned.Status).alias('Total')).sort(col('Total'), ascending=False).show(10)
    linux_process_cleaned.groupby('POLI').agg(count(linux_process_cleaned.POLI).alias('Total')).sort(col('Total'), ascending=False).show(10)
    linux_process_cleaned.groupby('type').agg(count(linux_process_cleaned.type).alias('Total')).sort(col('Total'), ascending=False).show(10)
    linux_process_cleaned.groupby('CMD').agg(count(linux_process_cleaned.CMD).alias('Total')).sort(col('Total'), ascending=False).show(10)

    print('TABLES DONE')
    check2 = input('Process done. Next stage: Data Visualization 1/4 \nContinue? Y/N')
    if check2.lower() == 'n':
        sys.exit()

    # since plotting library works only with pandas data frame that is why converting PySpark dataframe into Pandas Data Frame
    full_process = linux_process_cleaned.toPandas()
    full_memory = linux_memory_cleaned.toPandas()

    # bar graph with x-axis(attack types) and y-axis(Total attacks)
    attack_types = linux_process_cleaned.groupBy('type').agg(sum('attack').alias('Total attacks')).sort(
        col('Total attacks'), ascending=False).toPandas()
    plt.figure(figsize=(15, 10))

    plt.bar(attack_types['type'], attack_types['Total attacks'])
    plt.xlabel("Attack Types", fontdict=None, labelpad=None)
    plt.ylabel("Total Attacks", fontdict=None, labelpad=None)
    plt.title("linux_process bar graph - Total attacks for each attack type")
    plt.show()

    check3 = input('Process done. Next stage: Data Visualization 2/4 \nContinue? Y/N')
    if check3.lower() == 'n':
        sys.exit()

    # scatter plot
    plt.figure(figsize=(15, 10))

    plt.scatter(full_process["CPU"], full_process["ts"],
                c=full_process["attack"])
    plt.xlabel("CPU time consumption", fontdict=None, labelpad=None)
    plt.ylabel("time-stamp", fontdict=None, labelpad=None)
    plt.title("linux_process Scatter Plot - CPU time consumption vs time-stamp")
    plt.show()

    check4 = input('Process done. Next stage: Data Visualization 3/4 \nContinue? Y/N')
    if check4.lower() == 'n':
        sys.exit()

    plt.figure(figsize=(15, 10))
    plt.scatter(full_memory["VSTEXT"], full_memory["RSIZE"],
                c=full_memory["attack"])
    plt.xlabel("VSTEXT(virtual memory size used by the shared text of process)", fontdict=None, labelpad=None)
    plt.ylabel("RSIZE(total resident memory usage consumed by process)", fontdict=None, labelpad=None)
    plt.title("linux_memory Scatter Plot - VSTEXT vs RSIZE")
    plt.show()

    check5 = input('Process done. Next stage: Data Visualization 4/4 \nContinue? Y/N')
    if check5.lower() == 'n':
        sys.exit()

    f = plt.figure(figsize=(10, 10))
    plt.matshow(full_memory.corr(), fignum=f.number)
    plt.xticks(range(full_memory.shape[1]), full_memory.columns, fontsize=14, rotation=45)
    plt.yticks(range(full_memory.shape[1]), full_memory.columns, fontsize=14)
    cb = plt.colorbar()
    cb.ax.tick_params(labelsize=14)
    plt.title('Correlation Matrix', fontsize=16)
    plt.show()

    check6 = input('Process done. Next stage: Feature Extraction \nContinue? Y/N')
    if check6.lower() == 'n':
        sys.exit()

    seed = 0
    process_train, process_test = linux_process_cleaned.randomSplit([0.8, 0.2], seed)
    memory_train, memory_test = linux_memory_cleaned.randomSplit([0.8, 0.2], seed)

    count_attack_memory = memory_train.groupBy("attack").count().orderBy("attack")
    total_non_attack_memory = count_attack_memory.where(col("attack") == 0).collect()[0][1]
    total_attack_memory = count_attack_memory.where(col("attack") == 1).collect()[0][1]

    ration_attack_memory = (((total_attack_memory) * 0.2) * 2) / total_non_attack_memory
    sampled_memory = memory_train.sampleBy("attack", fractions={0: ration_attack_memory, 1: 0.2}, seed=0)

    memory_t = sampled_memory.cache()
    memory_t.groupBy("attack").count().orderBy("attack").show()
    #################
    count_attack_process = process_train.groupBy("attack").count().orderBy("attack")
    total_non_attack_process = count_attack_process.where(col("attack") == 0).collect()[0][1]
    total_attack_process = count_attack_process.where(col("attack") == 1).collect()[0][1]

    ration_attack_process = (((total_attack_process) * 0.2) * 2) / total_non_attack_process
    sampled_process = process_train.sampleBy("attack", fractions={0: ration_attack_process, 1: 0.2}, seed=0)

    process_t = sampled_process.cache()
    process_t.groupBy("attack").count().orderBy("attack").show()

    check7 = input('Process done. Next stage: Creating Machine Learning Pipelines \nContinue? Y/N')
    if check7.lower() == 'n':
        sys.exit()
###################################
    categoryInputCols_process = ['Status', 'State', 'CMD']
    numericInputCols_process = ['TSLPI', 'CPU', 'CPUNR']
    numericOutputCol_process = 'attack'

    allNumericCols_process = numericInputCols_process + [numericOutputCol_process]
#################################
    outputCols_index_process = [f'{x}_index' for x in categoryInputCols_process]
    inputIndexer_process = StringIndexer(inputCols=categoryInputCols_process,
                                         outputCols=outputCols_index_process).setHandleInvalid("skip")

    inputCols_OHE_process = outputCols_index_process
    outputCols_OHE_process = [f'{x}_vec' for x in categoryInputCols_process]
    encoder_process = OneHotEncoder(inputCols=inputCols_OHE_process, outputCols=outputCols_OHE_process)

    assemblerInputs_process = outputCols_OHE_process + numericInputCols_process
    assembler_process = VectorAssembler(inputCols=assemblerInputs_process, outputCol="features")
###############################
    dt_process = DecisionTreeClassifier(featuresCol='features', labelCol='attack')
    gbt_process = GBTClassifier(featuresCol='features', labelCol='attack')

    stage_process_1 = inputIndexer_process
    stage_process_2 = encoder_process
    stage_process_3 = assembler_process
    stage_process_dt = dt_process
    stage_process_gbt = gbt_process

    stages_dt_process = [stage_process_1, stage_process_2, stage_process_3, stage_process_dt]
    stages_gbt_process = [stage_process_1, stage_process_2, stage_process_3, stage_process_gbt]

    pipeline_dt_process = Pipeline(stages=stages_dt_process)
    pipeline_gbt_process = Pipeline(stages=stages_gbt_process)
#############################
    categoryInputCols_memory = ['CMD']
    numericInputCols_memory = ['VSTEXT', 'RSIZE', 'MINFLT', 'MAJFLT']
    numericOutputCol_memory = 'attack'

    allNumericCols_memory = numericInputCols_memory + [numericOutputCol_memory]
#############################
    outputCols_index = [f'{x}_index' for x in categoryInputCols_memory]
    inputIndexer = StringIndexer(inputCols=categoryInputCols_memory, outputCols=outputCols_index).setHandleInvalid(
        "skip")

    inputCols_OHE = outputCols_index
    outputCols_OHE = [f'{x}_vec' for x in categoryInputCols_memory]
    encoder = OneHotEncoder(inputCols=inputCols_OHE, outputCols=outputCols_OHE)

    assemblerInputs = outputCols_OHE + numericInputCols_memory
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
##############################
    dt_memory = DecisionTreeClassifier(featuresCol='features', labelCol='attack')
    gbt_memory = GBTClassifier(featuresCol='features', labelCol='attack')

    stage_1 = inputIndexer
    stage_2 = encoder
    stage_3 = assembler
    stage_dt = dt_memory
    stage_gbt = gbt_memory

    stages_dt_memory = [stage_1, stage_2, stage_3, stage_dt]
    stages_gbt_memory = [stage_1, stage_2, stage_3, stage_gbt]

    pipeline_dt_memory = Pipeline(stages=stages_dt_memory)
    pipeline_gbt_memory = Pipeline(stages=stages_gbt_memory)

    check8 = input('Process done. Next stage: Training and Evaluating Models \nContinue? Y/N')
    if check8.lower() == 'n':
        sys.exit()

    pipelineModel_dt_process = pipeline_dt_process.fit(process_t)
    predictions_dt_process = pipelineModel_dt_process.transform(process_test)

    pipelineModel_gbt_process = pipeline_gbt_process.fit(process_t)
    predictions_gbt_process = pipelineModel_gbt_process.transform(process_test)

    pipelineModel_dt_memory = pipeline_dt_memory.fit(memory_t)
    predictions_dt_memory = pipelineModel_dt_memory.transform(memory_test)

    pipelineModel_gbt_memory = pipeline_gbt_memory.fit(memory_t)
    predictions_gbt_memory = pipelineModel_gbt_memory.transform(memory_test)
###############################################
    predictions_dt_process = pipelineModel_dt_process.transform(process_test)
    predictions_gbt_process = pipelineModel_gbt_process.transform(process_test)
    predictions_dt_memory = pipelineModel_dt_memory.transform(memory_test)
    predictions_gbt_memory = pipelineModel_gbt_memory.transform(memory_test)

    predictions_dt_process = predictions_dt_process.withColumn('label', col('attack').cast(IntegerType()))
    predictions_gbt_process = predictions_gbt_process.withColumn('label', col('attack').cast(IntegerType()))
    predictions_dt_memory = predictions_dt_memory.withColumn('label', col('attack').cast(IntegerType()))
    predictions_gbt_memory = predictions_gbt_memory.withColumn('label', col('attack').cast(IntegerType()))

    evaluatorMulti = MulticlassClassificationEvaluator(labelCol="attack", predictionCol="prediction")
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

    print("Area Under ROC Decision Tree (Process Data): " + str(
        evaluator.evaluate(predictions_dt_process, {evaluator.metricName: "areaUnderROC"})))
    print("Accuracy Decision Tree (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_process, {evaluatorMulti.metricName: "accuracy"})))
    print("Precision Decision Tree (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_process, {evaluatorMulti.metricName: "weightedPrecision"})))
    print("Recall Decision Tree (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_process, {evaluatorMulti.metricName: "weightedRecall"})))
    print("F1 Decision Tree (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_process, {evaluatorMulti.metricName: "f1"})))
    print("---------------------------------------------------------------------------------------------------------")
    print("Area Under ROC GBT (Process Data): " + str(
        evaluator.evaluate(predictions_gbt_process, {evaluator.metricName: "areaUnderROC"})))
    print("Accuracy GBT (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_gbt_process, {evaluatorMulti.metricName: "accuracy"})))
    print("Precision GBT (Process Data): " + str(
        evaluatorMulti.evaluate(predictions_gbt_process, {evaluatorMulti.metricName: "weightedPrecision"})))
    print("Recall GBT (Process Data) " + str(
        evaluatorMulti.evaluate(predictions_gbt_process, {evaluatorMulti.metricName: "weightedRecall"})))
    print("F1 GBT (Process Data) " + str(
        evaluatorMulti.evaluate(predictions_gbt_process, {evaluatorMulti.metricName: "f1"})))
    print("---------------------------------------------------------------------------------------------------------")
    print("Area Under ROC DT (Memory Data): " + str(
        evaluator.evaluate(predictions_dt_memory, {evaluator.metricName: "areaUnderROC"})))
    print("Accuracy DT (Memory Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_memory, {evaluatorMulti.metricName: "accuracy"})))
    print("Precision DT (Memory Data): " + str(
        evaluatorMulti.evaluate(predictions_dt_memory, {evaluatorMulti.metricName: "weightedPrecision"})))
    print("Recall DT (Memory Data) " + str(
        evaluatorMulti.evaluate(predictions_dt_memory, {evaluatorMulti.metricName: "weightedRecall"})))
    print(
        "F1 DT (Memory Data) " + str(evaluatorMulti.evaluate(predictions_dt_memory, {evaluatorMulti.metricName: "f1"})))
    print("---------------------------------------------------------------------------------------------------------")
    print("Area Under GBT (Memory Data): " + str(
        evaluator.evaluate(predictions_gbt_memory, {evaluator.metricName: "areaUnderROC"})))
    print("Accuracy GBT (Memory Data): " + str(
        evaluatorMulti.evaluate(predictions_gbt_memory, {evaluatorMulti.metricName: "accuracy"})))
    print("Precision GBT (Memory Data): " + str(
        evaluatorMulti.evaluate(predictions_gbt_memory, {evaluatorMulti.metricName: "weightedPrecision"})))
    print("Recall GBT (Memory Data) " + str(
        evaluatorMulti.evaluate(predictions_gbt_memory, {evaluatorMulti.metricName: "weightedRecall"})))
    print("F1 GBT (Memory Data) " + str(
        evaluatorMulti.evaluate(predictions_gbt_memory, {evaluatorMulti.metricName: "f1"})))

    check9 = input('Process done. Next stage: Display Process ROC Curve \nContinue, Exit or Skip? Y/N/S')
    if check9.lower() != 's':
        if check9.lower() == 'n':
            sys.exit()
        else:
            selected_predictions = predictions_gbt_process.select(["probability", "attack"])
            selected_predictions_collect = selected_predictions.collect()
            prediction_list = [(float(value[0][0]), 1.0 - float(value[1])) for value in selected_predictions_collect]

            SandL = sc.parallelize(prediction_list)

            result_metrix = bcm(SandL)

            false_PR = dict()
            true_PR = dict()
            AUC = dict()

            test = [i[1] for i in prediction_list]
            result = [i[0] for i in prediction_list]

            false_PR, true_PR, _ = roc_curve(test, result)
            AUC = auc(false_PR, true_PR)

            plt.figure()
            plt.plot(false_PR, true_PR, label="ROC CURVE" % AUC)
            plt.plot([0, 1], [0, 1], 'p--')
            plt.xlabel('False positive rate')
            plt.ylabel('True positive rate')
            plt.title("predictions_gbt_process")
            plt.show()

    check10 = input('Process done. Next stage: Display Process ROC Curve \nContinue, Exit or Skip? Y/N/S')
    if check10.lower() != 's':
        if check10.lower() == 'n':
            sys.exit()
        else:
            selected_predictions = predictions_gbt_memory.select(["probability", "attack"])
            selected_predictions_collect = selected_predictions.collect()
            prediction_list = [(float(value[0][0]), 1.0 - float(value[1])) for value in selected_predictions_collect]

            SandL = sc.parallelize(prediction_list)

            result_metrix = bcm(SandL)

            false_PR = dict()
            true_PR = dict()
            AUC = dict()

            test = [i[1] for i in prediction_list]
            result = [i[0] for i in prediction_list]

            false_PR, true_PR, _ = roc_curve(test, result)
            AUC = auc(false_PR, true_PR)

            plt.figure()
            plt.plot(false_PR, true_PR, label="ROC CURVE" % AUC)
            plt.plot([0, 1], [0, 1], 'p--')
            plt.xlabel('False positive rate')
            plt.ylabel('True positive rate')
            plt.title("predictions_gbt_memory")
            plt.show()

    check11 = input('Process done. Next stage: Save the Process Model \nContinue? Y/N/S')
    if check11.lower() != 's':
        if check11.lower() == 'n':
            sys.exit()
        else:
            count_attack_process = linux_process_cleaned.groupBy("attack").count().orderBy("attack")

            total_non_attack_process = count_attack_process.where(col("attack") == 0).collect()[0][1]

            total_attack_process = count_attack_process.where(col("attack") == 1).collect()[0][1]

            ration_attack_process = (((total_attack_process) * 0.2) * 2) / total_non_attack_process

            sampled_process_big = linux_process_cleaned.sampleBy("attack", fractions={0: ration_attack_process, 1: 0.2},
                                                                 seed=0)

            sampled_process_big_cached = sampled_process_big.cache()
###############################
            categoryInputCols_process = ['State', 'Status', 'CMD']
            numericInputCols_process = ['CPU', 'TSLPI', 'CPUNR']
            numericOutputCol_process = 'attack'

            allNumericCols_process = numericInputCols_process + [numericOutputCol_process]

            outputCols_index = [f'{x}_index' for x in categoryInputCols_process]
            inputIndexer = StringIndexer(inputCols=categoryInputCols_process, outputCols=outputCols_index)

            inputCols_OHE = outputCols_index
            outputCols_OHE = [f'{x}_vec' for x in categoryInputCols_process]
            encoder = OneHotEncoder(inputCols=inputCols_OHE, outputCols=outputCols_OHE)

            assemblerInputs = outputCols_OHE + numericInputCols_process
            assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

            gbt_process = GBTClassifier(featuresCol='features', labelCol='attack')

            stage_1 = inputIndexer
            stage_2 = encoder
            stage_3 = assembler
            stage_gbt = gbt_process

            stages_gbt_process = [stage_1, stage_2, stage_3, stage_gbt]

            pipeline_gbt_process = Pipeline(stages=stages_gbt_process)
            pipelineModel_gbt_process = pipeline_gbt_process.fit(sampled_process_big_cached)
###########################################
            pipelineModel_gbt_process.save('attack_process_prediction_model')
###########################################
    check12 = input('Process done. Next stage: Save the Memory Model \nContinue? Y/N/S')
    if check12.lower() != 's':
        if check12.lower() == 'n':
            sys.exit()
        else:
            count_attack_process = linux_memory_cleaned.groupBy("attack").count().orderBy("attack")

            total_non_attack_process = count_attack_process.where(col("attack") == 0).collect()[0][1]

            total_attack_process = count_attack_process.where(col("attack") == 1).collect()[0][1]

            ration_attack_process = (((total_attack_process) * 0.2) * 2) / total_non_attack_process

            sampled_process_big = linux_memory_cleaned.sampleBy("attack", fractions={0: ration_attack_process, 1: 0.2},
                                                                seed=0)

            sampled_memory_big_cached = sampled_process_big.cache()
###############################
            categoryInputCols_memory = ['CMD']
            numericInputCols_memory = ['VSTEXT', 'RSIZE', 'MINFLT', 'MAJFLT']
            numericOutputCol_memory = 'attack'

            allNumericCols_memory = numericInputCols_memory + [numericOutputCol_memory]
#################################
            outputCols_index = [f'{x}_index' for x in categoryInputCols_memory]
            inputIndexer = StringIndexer(inputCols=categoryInputCols_memory,
                                         outputCols=outputCols_index).setHandleInvalid("skip")

            inputCols_OHE = outputCols_index
            outputCols_OHE = [f'{x}_vec' for x in categoryInputCols_memory]
            encoder = OneHotEncoder(inputCols=inputCols_OHE, outputCols=outputCols_OHE)

            assemblerInputs = outputCols_OHE + numericInputCols_memory
            assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

            gbt_memory = GBTClassifier(featuresCol='features', labelCol='attack')

            stage_1 = inputIndexer
            stage_2 = encoder
            stage_3 = assembler
            stage_gbt = gbt_memory

            stages_gbt_memory = [stage_1, stage_2, stage_3, stage_gbt]

            pipelineModel_gbt_memory = pipeline_gbt_memory.fit(sampled_memory_big_cached)
###########################################
            pipelineModel_gbt_memory.save('attack_memory_prediction_model')
###########################################
    check13 = input('Process done. Next stage: Part B \nContinue? Y/N')
    if check13.lower() == 'n':
        sys.exit()

if __name__ == '__main__':
    main()