from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
l_file = [
    "./yelp-data/yelp_businesses.csv"
    ,"./yelp-data/yelp_top_reviewers_with_reviews.csv"
    ,"./yelp-data/yelp_top_users_friendship_graph.csv"
    ]

def load_data(context,url):
    data = context.textFile(url)
    header = data.first()
    data = data.filter(lambda line: line != header)
    return data

#Basic, all column DataTypes are StringType by default
def create_df(spark: SparkSession,file_index: int=0):
    df = spark.read.csv(l_file[file_index],sep="\t",header=True)
    df.printSchema()
    return df

#Inferred datatypes
'''
    This works reasonably well. There are some issues.
    For instance in yelp_business.csv;
        latitude and long are inferred to double
        stars -> integer
    This is probably good enough for most applications.
'''
def create_inferred_df(spark: SparkSession, file_index: int=0):
    df = spark.read.option("inferSchema","True").csv(l_file[file_index],header=True,sep="\t")
    df.printSchema()
    return df

#Hardcoded types, a bit more work
def create_business_df(spark: SparkSession):
    df = spark.read.csv(l_file[0],header=True,sep="\t")
    df = df.withColumn("latitude",df["latitude"].cast("float"))
    df = df.withColumn("longitude",df["longitude"].cast("float"))
    df = df.withColumn("stars",df["stars"].cast("float"))
    df = df.withColumn("review_count",df["review_count"].cast("int"))
    df.printSchema()
    return df

'''
    Converting the StringType from review_date to LongType.
    This is the closest format to a Unix timestamp.
    Considered using DateType or TimeStamp type, but we considered LongType the closest.
'''
def create_review_df(spark: SparkSession):
    df = spark.read.csv(l_file[1],header=True,sep="\t")
    df = df.withColumn("review_date",df["review_date"].cast("long"))
    df.printSchema()
    return df

def create_friendship_df(spark: SparkSession):
    df = spark.read.csv(l_file[2],header=True,sep=",")
    df.printSchema()
    return df

spark = SparkSession.builder.appName("hello_dataframe").config("spark.some.config.option", "some-value").getOrCreate()
df = create_review_df(spark)
#df = create_business_df(spark)
#df = create_friendship_df(spark)
df.show()