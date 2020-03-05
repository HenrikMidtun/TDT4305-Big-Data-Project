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
# ----helpers----
def load_data(context,url):
    data = context.textFile(url)
    header = data.first()
    data = data.filter(lambda line: line != header)
    return data

#Returns (id, count) already sorted descending
def get_counts(data, counts_in = True):
    index = 1 if counts_in else 0
    in_lines = data.map(lambda x: (x.split(",")[index],1))
    counts = in_lines.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False)
    return counts


# ----tasks----
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

def get_median(data):
    sorted_counts = get_counts(data,counts_in=True)
    indexed = sorted_counts.zipWithIndex().map(lambda x: (x[1],x[0])).cache()
    count = indexed.count()
    odd = count%2 == 1
    if odd:
        tup = indexed.lookup(int(count/2))[0]
        r_val = tup[1]
    else:
        tup_1 = indexed.lookup(count/2)[0]
        tup_2 = indexed.lookup(count/2-1)[0]
        r_val = (tup_1[1]+tup_2[1])/2
    print("Median: ", r_val)
    return r_val


context = SparkContext("local","friendship")
data = load_data(context, l_file[2])
#top10_out(data)
#get_means(data)

###Task 4a
out = top10_out(data)
inndata = top10_in(data)

'''
with open('./Task4/Task 4.txt', 'w') as f:
    f.write('Out data:')
    for o in out:
        f.write("{0}\n".format(str(o)))
    for melding in inndata:
        f.write(str(melding))
    f.close()
'''

###Task 4b
avg_in,avg_out = get_means(data)
median = get_median(data)
with open('./Task4/Task 4b.txt', 'w') as f:
    f.write('Average in: {0}'.format(avg_in))
    f.write('Average out: {0}'.format(avg_out))
    f.write('Median: {0}'.format(median))
    f.close()
