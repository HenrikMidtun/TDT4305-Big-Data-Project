import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles

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

def top10_in(data):
    lines = data.map(lambda x: (x.split(",")[1],1))
    sum_in = lines.reduceByKey(lambda a,b: a+b)
    top10 = sum_in.sortBy(lambda x: x[1], ascending=False).take(10)
    for u in top10:
        print(u)

    return top10

def top10_out(data):
    lines = data.map(lambda x: (x.split(",")[0],1))
    sum_out = lines.reduceByKey(lambda a,b: a+b)
    top10 = sum_out.sortBy(lambda x: x[1], ascending=False).take(10)
    for u in top10:
        print(u)

    return top10

def get_means(data):
    #return mean of in and out
    lines = data.map(lambda x: (x.split(",")[0],1))
    sum_out = lines.reduceByKey(lambda a,b: a+b)
    lines = data.map(lambda x: (x.split(",")[1],1))
    sum_in = lines.reduceByKey(lambda a,b: a+b)
    avg_out = sum_out.values().mean()
    avg_in = sum_in.values().mean()
    print('Average in: {0}\nAverage out: {1}'.format(avg_in,avg_out))
    return avg_in,avg_out

def get_median():
    return None

context = SparkContext("local","friendship")
data = load_data(context, l_file[2])
#top10_out(data)
#get_means(data)
get_median(data)
