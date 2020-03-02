from pyspark import SparkContext
from operator import add
from datetime import datetime

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

def get_num_users(data):
    line_lengths = data.map(lambda s: (s.split("\t")[1],1))
    total_length = line_lengths.reduceByKey(lambda a,b: (a+b))
    print(total_length.count())
    return total_length.count()

def get_avg_char_review(data):
    lines = data.map(lambda s: len(s.split("\t")[3]))
    count = lines.count()
    total = lines.reduce(lambda a,b:a+b)
    print(total/count)
    return total/count

def get_top_businesses(data):
    lines = data.map(lambda s: (s.split("\t")[2],1))
    agg_reviews = lines.reduceByKey(lambda a,b: a+b)
    sort_reviews = agg_reviews.sortBy(lambda x: x[1], ascending=False)
    r_list = []
    for business in sort_reviews.take(10):
        r_list.append(business[0])
    print(r_list)
    return r_list

def get_reviews_year(data):
    lines = data.map(lambda s: (get_year(s.split("\t")[4]),1))
    rev_year = lines.reduceByKey(lambda a,b: a+b)
    sorted_rev = rev_year.sortBy(lambda x: x[0])
    r_list = rev_year.collect()
    for year in r_list:
        print(year)
    return r_list

def get_first_last(data):
    times = data.map(lambda s: s.split("\t")[4])
    sorted_times = times.sortBy(lambda x: x[0])
    first = sorted_times.first()
    last = sorted_times.max()
    print("first")
    print(get_timestring(first))
    print("last")
    print(get_timestring(last))

def get_amount_reviews(data):
    #(user_id, num_reviews)

def get_average_reviews(data):
    #average_reviews

def get_avg_lenreview(data):
    #(user_id, avg_len)

def get_avg_len_total(data):
    #average_length
    
def final_func(data)
    #(userid, rating)

def get_year(uni_stamp):
    return int(datetime.fromtimestamp(float(uni_stamp)).year)

def get_timestring(uni_stamp):
    return datetime.fromtimestamp(float(uni_stamp))

context = SparkContext("local", "first app")
data = load_data(context,l_file[1])
get_reviews_year(data)
#get_first_last(data)
'''
for url in l_file:
    data = context.textFile(url)
    num = data.count()
    print(num)
'''