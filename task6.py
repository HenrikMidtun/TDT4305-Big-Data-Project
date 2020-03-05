from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
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
    #df.printSchema()
    return df

#Hardcoded types, a bit more work
def create_business_df(spark: SparkSession):
    df = spark.read.csv(l_file[0],header=True,sep="\t")
    df = df.withColumn("latitude",df["latitude"].cast("float"))
    df = df.withColumn("longitude",df["longitude"].cast("float"))
    df = df.withColumn("stars",df["stars"].cast("float"))
    df = df.withColumn("review_count",df["review_count"].cast("int"))
    #df.printSchema()
    return df

def create_review_df(spark: SparkSession):
    df = spark.read.csv(l_file[1],header=True,sep="\t")
    df = df.withColumn("review_date",df["review_date"].cast("long"))
    #df.printSchema()
    return df

spark = SparkSession.builder.appName("hello_dataframe").config("spark.some.config.option", "some-value").getOrCreate()
dfr = create_review_df(spark)
dfb = create_business_df(spark)

joined = dfr.join(dfb, dfb.business_id == dfr.business_id)

top_reviewers = joined.groupBy(joined.user_id).count().orderBy('count', ascending=False).take(20)

print(top_reviewers)

with open('Task 6.txt', 'w') as f:
    for item in top_reviewers:
        f.write("%s\n" % str(item))