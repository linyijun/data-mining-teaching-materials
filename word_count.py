import pyspark


if __name__ == '__main__':

    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    input_path = './work-count-sample-doc.txt'
    data = sc.textFile(input_path)

    count = data.flatMap(lambda line:line.split(' ')) \
        .map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()

    print(count)


