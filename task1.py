import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles


sc =SparkContext()
url=['yelp_businesses.csv','yelp_top_reviewers_with_reviews.csv','yelp_top_users_friendship_graph.csv']


for i in range(len(url)):
    data=sc.textFile(url[i])
    print(data.count())