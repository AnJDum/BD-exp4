import findspark
findspark.init()

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, LinearSVC
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

sc = SparkContext(appName="LoanDistribution")
spark = SparkSession.builder.appName("task4").getOrCreate()
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(r'train_data.csv')

# 补充空值
df = df.na.fill(-1)
df = df.na.fill('-1')
print(df.head())

# class
class_stringIndexer = StringIndexer(inputCol="class",outputCol="class_class",stringOrderType="frequencyDesc")
df = class_stringIndexer.fit(df).transform(df)
class_encoder = OneHotEncoder(inputCol="class_class",outputCol="class_onehot").setDropLast(False)
df = class_encoder.fit(df).transform(df)
print('======================class====================')
print(df.head())
# sub_class
sub_class_stringIndexer = StringIndexer(inputCol="sub_class",outputCol="sub_class_class",stringOrderType="frequencyDesc")
df = sub_class_stringIndexer.fit(df).transform(df)
sub_class_encoder = OneHotEncoder(inputCol="sub_class_class",outputCol="sub_class_onehot").setDropLast(False)
df = sub_class_encoder.fit(df).transform(df)

# work_type
work_type_stringIndexer = StringIndexer(inputCol="work_type",outputCol="work_type_class",stringOrderType="frequencyDesc")
df = work_type_stringIndexer.fit(df).transform(df)
work_type_encoder = OneHotEncoder(inputCol="work_type_class",outputCol="work_type_onehot").setDropLast(False)
df = work_type_encoder.fit(df).transform(df)

#employer_type
employer_type_stringIndexer = StringIndexer(inputCol="employer_type",outputCol="employer_type_class",stringOrderType="frequencyDesc")
df = employer_type_stringIndexer.fit(df).transform(df)
employer_type_encoder = OneHotEncoder(inputCol="employer_type_class",outputCol="employer_type_onehot").setDropLast(False)
df = employer_type_encoder.fit(df).transform(df)

# industry
industry_stringIndexer = StringIndexer(inputCol="industry",outputCol="industry_class",stringOrderType="frequencyDesc")
df = industry_stringIndexer.fit(df).transform(df)
industry_encoder = OneHotEncoder(inputCol="industry_class",outputCol="industry_onehot").setDropLast(False)
df = industry_encoder.fit(df).transform(df)
print('======================industry====================')
print(df.head())

# work_year ok
df = df.withColumn("work_year", F.when(df.work_year == '10+ years', '10').when(df.work_year == '9 years', '9').when(df.work_year == '8 years', '8').when(df.work_year == '7 years', '7').when(df.work_year == '6 years', '6').when(df.work_year == '5 years', '5').when(df.work_year == '4 years', '4').when(df.work_year == '3 years', '3').when(df.work_year == '2 years', '2').when(df.work_year == '1 year', '1').when(df.work_year == '< 1 year', '0'))
df = df.withColumn("work_year", df['work_year'].cast('int'))
print(df.head())

df.select('issue_date').show(5)
#issue_date
@F.udf(returnType = IntegerType())
def process_issue_data(x):
    result = 0
    if x == "-1":
        result = 0
    else:
        result = (int(x[0:4])-2000)*12 + int(x[5:7])
    return result
df = df.withColumn("issue_date", process_issue_data(F.col("issue_date")))
df.select('issue_date').show(5)
df.select('earlies_credit_mon').show(5)

# earlies_credit_mon
@F.udf(returnType = IntegerType())
def process_credit_mon(x):
    result = 0
    if x == "-1":
        result = 0
    else:
        month = x[0:3]
        day = int(x[-4:])
        if month == 'Jan':
            result = (2021 - day)*12 + 11
        elif month == 'Feb':
            result = (2021 - day)*12 + 10
        elif month == 'Mar':
            result = (2021 - day)*12 + 9
        elif month == 'Apr':
            result = (2021 - day)*12 + 8
        elif month == 'May':
            result = (2021 - day)*12 + 7
        elif month == 'Jun':
            result = (2021 - day)*12 + 6
        elif month == 'Jul':
            result = (2021 - day)*12 + 5
        elif month == 'Aug':
            result = (2021 - day)*12 + 4
        elif month == 'Sep':
            result = (2021 - day)*12 + 3
        elif month == 'Oct':
            result = (2021 - day)*12 + 2
        elif month == 'Nov':
            result = (2021 - day)*12 + 1
        else:
            result = (2021 - day)*12
    return result

df = df.withColumn("earlies_credit_mon", process_credit_mon(F.col("earlies_credit_mon")))
df.select('earlies_credit_mon').show(5)
print(df.head())
df = df.na.fill(-1)

# 删除不需要的列
df = df.drop('loan_id', 'user_id', 'class', 'sub_class', 'work_type', 'employer_type', 'industry', 'class_onehot', 'sub_class_onehot', 'work_type_onehot', 'employer_type_onehot', 'industry_onehot')
df_feature = df.drop('is_default')
assembler = VectorAssembler(inputCols=df_feature.columns, outputCol="features")
print(df_feature.columns)
data = assembler.transform(df)

# 划分训练集与测试集
df_train, df_test = data.randomSplit([0.8, 0.2])
# 评估指标
def cal_evaluate(predictions):
    print('========================-----------------------==========================')
    tp = predictions[(predictions.is_default == 1) & (predictions.prediction == 1)].count()
    tn = predictions[(predictions.is_default == 0) & (predictions.prediction == 0)].count()
    fp = predictions[(predictions.is_default == 0) & (predictions.prediction == 1)].count()
    fn = predictions[(predictions.is_default == 1) & (predictions.prediction == 0)].count()
    print(tp, tn, fp, fn)
    try:
        precision = float(tp) / float(tp + fp)
    except:
        precision = 0
    try:
        recall = float(tp) / float(tp + fn)
    except:
        recall = 0
    try:
        f1 = 2 * precision * recall / (precision + recall)
    except:
        f1 = 0
    acc = predictions[predictions.is_default == predictions.prediction].count()/predictions.count()
    auc = BinaryClassificationEvaluator(labelCol='is_default').evaluate(predictions)
    print("精确率: ", precision)
    print("召回率: ", recall)
    print("F1分数: ", f1)
    print("准确率: ", acc)
    print("auc: ", auc)
    return 1

# 决策树
dt = DecisionTreeClassifier(labelCol="is_default").fit(df_train)
predictions_dt = dt.transform(df_test)
cal_evaluate(predictions_dt)

# 逻辑回归
lr = LogisticRegression(regParam=0.01, labelCol='is_default').fit(df_train)
predictions_lr = lr.transform(df_test)
cal_evaluate(predictions_lr)

# 随机森林
rf = RandomForestClassifier(labelCol='is_default', maxDepth=7, maxBins=700, numTrees=50).fit(df_train)
predictions_rf = rf.transform(df_test)
cal_evaluate(predictions_rf)

spark.stop()