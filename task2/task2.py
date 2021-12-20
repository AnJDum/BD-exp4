#!/usr/bin/python3.6

import findspark
findspark.init()

from pyspark.context import SparkContext

sc = SparkContext(appName="LoanDistribution")
lines = sc.textFile("train_data.csv")
data = lines.filter(lambda line: line.split(',')[0]!="loan_id")
output = data.map(lambda line: (int(int(line.split(',')[2].split('.')[0])/1000), 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[0]).collect()
with open("LoanDistribution.txt", "w") as f:
    for (scale, count) in output:
        f.write("((%i,%i),%i)\n" % (scale*1000, (scale+1)*1000, count))
f.close()
sc.stop()