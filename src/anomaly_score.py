# I use this in order to find spark in my disk
import findspark
# then I import the python spark library
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# I will use some SQL functions to make a few transformations on the data
import pyspark.sql.functions as f


def load_data():
    findspark.init(spark_home='/opt/spark-3..0.1/')
    # I use spark SQL to create a dataframe from the file
    sql = pyspark.sql.SparkSession.builder.getOrCreate()
    # I specify the schema of my data in order to load the file without any problems
    schema = StructType([StructField('Person', StringType(), True),
                         StructField('1/1/18', IntegerType(), True),
                         StructField('1/2/18', IntegerType(), True),
                         StructField('1/3/18', IntegerType(), True),
                         StructField('1/4/18', IntegerType(), True),
                         StructField('1/5/18', IntegerType(), True),
                         StructField('1/6/18', IntegerType(), True),
                         StructField('1/7/18', IntegerType(), True),
                         StructField('1/8/18', IntegerType(), True),
                         StructField('1/9/18', IntegerType(), True),
                         StructField('1/10/18', IntegerType(), True),
                         StructField('1/11/18', IntegerType(), True),
                         StructField('1/12/18', IntegerType(), True),
                         StructField('1/13/18', IntegerType(), True),
                         StructField('1/14/18', IntegerType(), True),
                         StructField('1/15/18', IntegerType(), True),
                         StructField('1/16/18', IntegerType(), True),
                         StructField('1/17/18', IntegerType(), True)])

    df = (sql.read
          .format("com.databricks.spark.csv")
          .option("header", "true").schema(schema)
          .load("input_data.csv")
          )
    return df


def save_data(df, file_name):
    df.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",',').save(file_name)
    pyspark.sql.SparkSession.builder.getOrCreate().stop()


def rename_columns(old_cols, df):
    # just renaming the columns to the original setting
    for c in old_cols:
        df = df.drop(c)
    cols = df.columns[1:]
    for c1, c2 in zip(old_cols, cols):
        df = df.withColumn(c1, f.col(c2))
    for c in cols:
        df = df.drop(c)
    return df


def calculate_score_solution_1():
    df = load_data()
    # Here comes the tricky logic on why I made two different solutions, from the e-mail
    # I understood: calculate a score of a date based on the previous dates. That would mean that my first day
    # wouldn't have a score because I don't have a previous day do compare to.
    # I chose to evaluate the anomaly score using the z score,
    # this means I would need at least two days to start calculating scores.
    # Following this line of thinking
    # I have the first day with score 0. Then I apply the logic of calculating the z score for the remaining days

    # get the dates
    cols = df.columns[1:]
    # index to know which iteration I am
    i = 0
    for c in cols:
        i += 1
        # if it's the first day = no score (no anomaly)
        if i < 2:
            df = df.withColumn('score_' + str(i), f.lit(0))
        else:
            # get all previous days
            cols_ = [x for x in cols if x <= c]
            # get mean value of previous days
            row_mean_ = (sum(f.col(x) for x in cols_) / i).alias("mean")
            df = df.withColumn('mean', row_mean_)
            # get standard deviation from previous days
            row_std_ = f.sqrt(sum(((f.col(x) - f.col('mean')) ** 2 for x in cols_)) / i).alias('std')
            # get z score for current date
            score = ((f.col(c) - f.col('mean')) / f.col('std')).alias("score_" + str(i))
            df = df.withColumn('std', row_std_)
            df = df.withColumn('score_' + str(i), score)
            # dropping mean and std since it changes every iteration
            df = df.drop('mean', 'std')
    df = rename_columns(cols, df)
    save_data(df, 'output_data_solution_1.csv')


def calculate_score_solution_2():
    # For this solution I calculate the overall mean and standard deviation and I apply to every column
    # in my humble opinion this approach calculates a better overall anomaly score
    # that's why I got a little confused
    df = load_data()
    n = f.lit(len(df.columns) - 1.0)
    row_mean = (sum(f.col(x) for x in df.columns[1:]) / n).alias("mean")
    row_std = f.sqrt(sum(((f.col(x) - f.col('mean')) ** 2 for x in df.columns[1:17])) / n).alias('std')
    cols = df.columns[1:]
    i = 0
    df = df.withColumn('mean', row_mean)
    df = df.withColumn('std', row_std)
    for c in cols:
        i += 1
        score = ((f.col(c) - f.col('mean')) / f.col('std')).alias("score_" + str(i))
        df = df.withColumn('score_' + str(i), score)
    df = df.drop('mean', 'std')
    df = rename_columns(cols, df)
    save_data(df, 'output_data_solution_2.csv')

