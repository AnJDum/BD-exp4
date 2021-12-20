import findspark
findspark.init()

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import udf, col 
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from pyspark import SparkContext
from pyspark import SparkConf

# 统计所有用户所在公司类型 employer_type 的数量分布占比情况
spark = SparkSession.builder.appName("task3").getOrCreate()
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(r'train_data.csv')
num = df.count()
df1 = df.groupBy('employer_type').count()
df2 = df1.withColumn('count', df1['count']/num)
df2.show()
output = df2.collect()
with open(r"EmployerType.csv", "w") as file1:
    for (type_name, prop) in output:
        file1.write("%s,%f\n" % (type_name, prop))
file1.close()

# 统计每个用户最终须缴纳的利息金额
df3 = df.select("user_id", "total_loan", "year_of_loan", "monthly_payment")
df_expand = df3.withColumn("total_money", df3['year_of_loan']*df3['monthly_payment']*12-df3['total_loan'])
df4 = df_expand.select("user_id", "total_money")
output2 = df4.collect()
with open(r"TotalMoney.csv", "w") as file2:
    for (user_id, total_money) in output2:
        file2.write("%s,%f\n" % (user_id, total_money))
file2.close()

# 统计工作年限 work_year 超过 5 年的用户的房贷情况 censor_status 的数量分布占⽐比情况
df_filtered = df.where(
    (col('work_year') != '< 1 year') &
    (col('work_year') != '1 year') &
    (col('work_year') != '2 years') &
    (col('work_year') != '3 years') &
    (col('work_year') != '4 years') &
    (col('work_year') != '5 years'))
df5 = df_filtered.select("user_id", "censor_status", "work_year")
df6 = df5.withColumn('work_year', F.when(df5.work_year == '10+ years', '10').when(df.work_year == '9 years', '9').when(df.work_year == '8 years', '8').when(df.work_year == '7 years', '7').when(df.work_year == '6 years', '6'))
output3 = df6.collect()
with open(r"CensorStatus.csv", "w") as file3:
    for (user_id, censor_status, work_year) in output3:
        file3.write("%s,%s,%s\n" % (user_id, censor_status, work_year))
file3.close()