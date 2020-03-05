import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles


sc =SparkContext()
l_file = [
    "./yelp-data/yelp_businesses.csv"
    ,"./yelp-data/yelp_top_reviewers_with_reviews.csv"
    ,"./yelp-data/yelp_top_users_friendship_graph.csv"
    ]

with open('Task 1.txt', 'w') as f:
    for i in range(len(l_file)):
        data=sc.textFile(l_file[i])
        count = data.count()
        print(count)
        f.write("Number of rows in {1}: {0}\n".format(str(count),l_file[i]))