# 大数据实验四报告

徐冰清 191098273

实验环境：IDEA & BDkit

[TOC]

## Task1

编写 MapReduce 程序，统计每个工作领域 industry 的⽹贷记录的数量，并按数量从大到小进行排序。

#### 框架

总体上看，使用两个job完成实验。第一个job统计⽹贷记录的数量，第二个job按数量从大到小进行排序。

```java
Job job = Job.getInstance(conf, "word count");
job.setJarByClass(WordCount.class);
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
List<String> otherArgs = new ArrayList<String>();
for (int i = 0; i < remainingArgs.length; ++i) {
	otherArgs.add(remainingArgs[i]);
}
FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
Path tempPath = new Path("tmp-count" );
FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
job.setOutputFormatClass(SequenceFileOutputFormat.class);
System.out.print("ok1");
if(!job.waitForCompletion(true)){
	Job sortjob = Job.getInstance(conf, "sort count");
	sortjob.setJarByClass(WordCount.class);
	FileInputFormat.addInputPath(sortjob, tempPath);
	sortjob.setInputFormatClass(SequenceFileInputFormat.class);
	sortjob.setMapperClass(InverseMapper.class);
	sortjob.setReducerClass(SortReducer.class);
	FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs.get(1)));
	sortjob.setOutputKeyClass(IntWritable.class);
	sortjob.setOutputValueClass(Text.class);
  
	sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
	FileSystem.get(conf).deleteOnExit(tempPath);
}
```

#### job 1

**<u>TokenizerMapper</u>**：主要操作为在map函数中，忽略表格第一行标题，选取industry记录作为key、1为value写入。

```java
@Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (!key.toString().equals("0")){
                String[] line = value.toString().split(",");
                context.write(new Text(line[10]), one);
            }
        }
```

<u>**IntSumReducer**</u>：对同一个key下的value求和，无特殊操作。

```java
@Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
```



#### job 2

**<u>InverseMapper</u>**：调换key和value的位置。

<u>**SortReducer**</u>：对value进行排序，设置输出格式。

```java
public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text val: values){
                result.set(val.toString());
                context.write(new Text(result+" "+key), NullWritable.get());
            }
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
```

运行结果如下

<img src="/img/output1.png" alt="output1" style="zoom:20%;" />



## Task2

编写 Spark 程序，统计⽹络信用贷产品记录数据中所有用户的贷款金额 total_loan 的分布情况。

思路：

数据读取，用“，”对每一行数据进行分隔，选取loan_id。

```java
sc = SparkContext(appName="LoanDistribution")
lines = sc.textFile("train_data.csv")
data = lines.filter(lambda line: line.split(',')[0]!="loan_id")
```

因间隔1000，故通过除以1000的余数确定分组。先map形成key-value对，后reduce统计频数。

```java
output = data.map(lambda line: (int(int(line.split(',')[2].split('.')[0])/1000), 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[0]).collect()
```

调整格式，写入文件夹

```java
with open("LoanDistribution.txt", "w") as f:
    for (scale, count) in output:
        f.write("((%i,%i),%i)\n" % (scale*1000, (scale+1)*1000, count))
```

运行结果如下：

<img src="/img/output2.png" alt="output2" style="zoom:67%;" />



## Task3

#### 统计所有用户所在公司类型 employer_type 的数量分布占比情况。

采用SparkSession打开文件：

```python
spark = SparkSession.builder.appName("task3").getOrCreate()
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(r'train_data.csv')
```

使用groupby对公司类型进行统计；对所有员工数量进行统计；二者相除即公司类型占比。

```python
num = df.count()
df1 = df.groupBy('employer_type').count()
df2 = df1.withColumn('count', df1['count']/num)
```

计算结果如下：

<img src="/img/op31.png" alt="op31" style="zoom:50%;" />

#### 统计每个用户最终须缴纳的利息金额

使用withColumn对各列进行四则运算，得出每个用户最终须缴纳的利息金额，选取计算结果列和id列存入csv。

```python
df3 = df.select("user_id", "total_loan", "year_of_loan", "monthly_payment")
df_expand = df3.withColumn("total_money", df3['year_of_loan']*df3['monthly_payment']*12-df3['total_loan'])
df4 = df_expand.select("user_id", "total_money")
output2 = df4.collect()
```

计算结果如下：

<img src="/img/op32.png" alt="op32" style="zoom:50%;" />

#### 统计⼯作年限 work_year 超过 5 年的用户的房贷情况 censor_status 的数量分布占比情况

首先删除⼯作年限不满五年的数据，接着选取需要的id、房贷情况、⼯作年限这三列。再对工作年限进行转换，使用withColumn替代，消除特殊符号和years。

```python
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
with open(r"CensorStatus.csv",
```

计算结果如下：

<img src="/img/op33.png" alt="op33" style="zoom:50%;" />

## Task4

根据给定的数据集，基于 Spark MLlib 或者Spark ML编写程序预测有可能违约的借贷人，并评估实验结果的准确率。

#### 数据读取

在读csv时，设置`inferSchema='True'`模式推理，使spark能够推断出数据类型。

```python
sc = SparkContext(appName="LoanDistribution")
spark = SparkSession.builder.appName("task4").getOrCreate()
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(r'train_data.csv')
```



#### 数据预处理

1. 填充缺失值

   对于数值变量，填入数字-1；对于string，填入字符串-1。

```python
df = df.na.fill(-1)
df = df.na.fill('-1')
```

2. 将string转为数字。
   1. 运用StringIndexer对分类数据进行处理。

```python
# class
class_stringIndexer = StringIndexer(inputCol="class",outputCol="class_class",stringOrderType="frequencyDesc")
df = class_stringIndexer.fit(df).transform(df)
class_encoder = OneHotEncoder(inputCol="class_class",outputCol="class_onehot").setDropLast(False)
df = class_encoder.fit(df).transform(df)

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
```

  2. 将含义不明的数据转化成数字。对work_year，使用替换去除years和符号，再转化为int类型。

     ```python
     df = df.withColumn("work_year", F.when(df.work_year == '10+ years', '10').when(df.work_year == '9 years', '9').when(df.work_year == '8 years', '8').when(df.work_year == '7 years', '7').when(df.work_year == '6 years', '6').when(df.work_year == '5 years', '5').when(df.work_year == '4 years', '4').when(df.work_year == '3 years', '3').when(df.work_year == '2 years', '2').when(df.work_year == '1 year', '1').when(df.work_year == '< 1 year', '0'))
     df = df.withColumn("work_year", df['work_year'].cast('int'))
     ```



3. 将issue_date日期转换为对应的数字，时间越早数字越小。将earlies_credit_mon日期转换为对应的数字，时间越晚数字越小。

```python
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
```

经过这些处理，可以不遗漏地得到所有有效信息。我们删除无效的数据，留下这些列做为特征：`['total_loan', 'year_of_loan', 'interest', 'monthly_payment', 'work_year', 'house_exist', 'house_loan_status', 'censor_status', 'marriage', 'offsprings', 'issue_date', 'use', 'post_code', 'region', 'debt_loan_ratio', 'del_in_18month', 'scoring_low', 'scoring_high', 'pub_dero_bankrup', 'early_return', 'early_return_amount', 'early_return_amount_3mon', 'recircle_b', 'recircle_u', 'initial_list_status', 'earlies_credit_mon', 'title', 'policy_code', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5', 'class_class', 'sub_class_class', 'work_type_class', 'employer_type_class', 'industry_class']`

```python
df.drop(['loan_id', 'user_id', 'class', 'sub_class', 'work_type', 'employer_type', 'industry', 'class_onehot', 'sub_class_onehot', 'work_type_onehot', 'employer_type_onehot', 'industry_onehot'])
```

4. 特征集成

   ```python
   df_feature = df.drop('is_default')
   assembler = VectorAssembler(inputCols=df_feature.columns, outputCol="features")
   data = assembler.transform(df)
   ```

5. 划分训练集与测试集

```python
df_train, df_test = data.randomSplit([0.8, 0.2])
```



#### 评估指标

采取精确率、召回率、F1、准确率、auc这5个指标对机器学习效果做出判断。

```python
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
```



#### 决策树

```python
dt = DecisionTreeClassifier(labelCol="is_default").fit(df_train)
predictions_dt = dt.transform(df_test)
cal_evaluate(predictions_dt)
```

评价指标为

精确率:  0.6276339070291137                                                     
召回率:  0.32261264985531213
F1分数:  0.4261686325906509
准确率:  0.824951273551117
auc:  0.6896668886716617



#### 逻辑回归

```python
lr = LogisticRegression(regParam=0.01, labelCol='is_default').fit(df_train)
predictions_lr = lr.transform(df_test)
cal_evaluate(predictions_lr)
```

评价指标为

精确率:  0.6626301253452305                                                     
召回率:  0.2569404399044402
F1分数:  0.3702956191380743
准确率:  0.823699518032242
auc:  0.814343835958197



#### 随机森林

```
rf = RandomForestClassifier(labelCol='is_default', maxDepth=7, maxBins=700, numTrees=50).fit(df_train)
predictions_rf = rf.transform(df_test)
cal_evaluate(predictions_rf)
```

评价指标为

精确率:  0.7373461012311902                                                     
召回率:  0.17760935826674357
F1分数:  0.2862643563699131
准确率:  0.8213229183978727
auc:  0.8284648716818422



**总结如下：**

1. 三个算法具有相似的准确度；
2. 随机森林的精确率最高、召回率最低、auc最高；
3. 召回率都很低，说明样本中违约的人在模型里大多数未被预测准确。



## 遇到的困难

1. 报错`py4j.protocol.Py4JError: org.apache.spark.api.python.PythonUtils.isEncryptionEnabled does not exist in the JVM`

<img src="/img/Error1.png" alt="Error1" style="zoom:50%;" />

解决方法：

在代码前加上即可。

```python
import findspark
findspark.init()
```



2. 在程序执行的时候，总是找别的python路径。其实本人并没有安装conda。

   <img src=/img/Error2.png" alt="Error2" style="zoom:50%;" />

经检查发现`echo $PYSPARK_PYTHON` 和 `/usr/local/spark/conf/spark-env.sh `里的`PYSPARK_PYTHON`路径不一样。

<img src="/img/Error3.png" alt="Error3" style="zoom:50%;" />

解决方案：

运行程序前，在命令行里输 `export PYSPARK_PYTHON=/usr/bin/python3.6`。



3. 出现java、spark内部通讯的问题（可能描述不准确）。把bdkit关掉重开就不报错了，神奇！

   

## 实验感想

这次实验四让我对pyspark接口熟悉起来。在写代码的过程中，我发现很多操作和我之前熟悉的pandas、sklearn库很相似，颇有些融会贯通的意味。

总体来看，这门课程的四个实验带着我走进了处理大数据的世界，docker、hadoop、hbase这些高深的工具走到我身边，成为了解决问题的好帮手，痛苦与快乐并存。很感谢这门课带给我的奇妙体验。
