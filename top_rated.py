import psycopg2
import config
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

try:
    connection = psycopg2.connect(
        database=config.DATABASE, user=config.USER, password=config.PASSWORD, host=config.HOST
    )
    print("connected to db")

    cursor = connection.cursor()

    query1 = "SELECT listing_id,rating,number_of_ratings from data_warehouse.facts_listing order by number_of_ratings desc FETCH FIRST 10 ROWS ONLY"

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["listing_id","rating","number_of_ratings"])
    listing_id = df.listing_id
    rating = df.rating
    number_of_ratings = df.number_of_ratings

    plt.figure(figsize=[15, 15])

    X = np.arange(len(rating))
    plt.bar(X, rating, color = 'green', width = 0.25)
    i = 1.0
    j = 1
    for i in range(len(listing_id)):
        plt.annotate(number_of_ratings[i], (i - 0.1, rating[i]))
    plt.legend(['rating'])     
    plt.xticks([i for i in range(10)], listing_id)
    plt.title("Which are the top-rated properties by number of listings", fontsize="20")
    plt.xlabel('listing_id')
    plt.ylabel('rating')
    plt.tick_params(axis='both',which='major',labelsize=10)
    plt.show()
    plt.savefig('top_rated.png')

    cursor.close()

except Exception as e:
    print(str(e))    
