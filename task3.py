from pyspark import SparkContext

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

def avg_review_by_city(data):
    lines = data.map(lambda x: (x.split("\t")[3],int(x.split("\t")[8])))

    #https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth
    #aggregateByKey(zero_val, in_part, inter_part)
    city_sum_count = lines.aggregateByKey(
        (0,0),
        lambda a,b: (a[0]+b, a[1]+1),
        lambda a,b: (a[0]+b[0], a[1]+b[1])) 
    avg_rev_city = city_sum_count.mapValues(lambda v: v[0]/v[1]).collect()

    for city in avg_rev_city:
        print(city)
    return avg_rev_city

def most_frequent_categories(data):
    lines = data.map(lambda x: (x.split("\t")[10],1))
    cat_counts = lines.reduceByKey(lambda a,b: a+b)
    sort_cat = cat_counts.sortBy(lambda e: e[1], ascending=False)
    top10 = sort_cat.take(10)
    for cat in top10:
        print(cat)
    return top10

def avg_postals(data):
    #(p_code, (lat, long, count=1))
    lines = data.map(
        lambda x: (x.split("\t")[5], (float(x.split("\t")[6]), float(x.split("\t")[7]),1)))
    #Iterative weighted average    
    post_avg = lines.reduceByKey(
        lambda a,b: (
            (a[0]*a[2]+b[0]*b[2])/(a[2]+b[2]), #latitude
            (a[1]*a[2]+b[1]*b[2])/(a[2]+b[2]), #longitude
            a[2]+b[2]                          #count
            )).collect()

    for s in post_avg:
        print(s)
    return post_avg

context = SparkContext("local","task3")
data = load_data(context, l_file[0])
avg_postals(data)