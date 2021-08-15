import argparse
import pyspark
import json


if __name__ == '__main__':

    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default='./backup/data/hw1/review.json', help='the input file ')
    parser.add_argument('--output_file', type=str, default='./backup/data/hw1/a1t1.json',
                        help='the output file contains your answers')
    parser.add_argument('--stopwords ', type=str, default='./backup/data/hw1/stopwords',
                        help='the file contains the stopwords')
    parser.add_argument('--y', type=int, default=2018, help='year')
    parser.add_argument('--m', type=int, default=10, help='top m users')
    parser.add_argument('--n', type=int, default=10, help='top n frequent words')

    args = parser.parse_args()

    # args.input_file = './data/hw1/review.json'

    review_rdd = sc.textFile(args.input_file)
    review_rdd = review_rdd.map(lambda line: json.loads(line))





