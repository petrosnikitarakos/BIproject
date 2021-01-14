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

    query1 = """SELECT COUNT(CASE WHEN rating =100 THEN 1 ELSE NULL END) AS rating_100,
                       COUNT(CASE WHEN rating =99  THEN 1 ELSE NULL END) AS rating_99,
                       COUNT(CASE WHEN rating =98  THEN 1 ELSE NULL END) AS rating_98,
                       COUNT(CASE WHEN rating =97  THEN 1 ELSE NULL END) AS rating_97,
                       COUNT(CASE WHEN rating =96  THEN 1 ELSE NULL END) AS rating_96,
                       COUNT(CASE WHEN rating =95  THEN 1 ELSE NULL END) AS rating_95,
                       COUNT(CASE WHEN rating =94  THEN 1 ELSE NULL END) AS rating_94,
                       COUNT(CASE WHEN rating =93  THEN 1 ELSE NULL END) AS rating_93,
                       COUNT(CASE WHEN rating =92  THEN 1 ELSE NULL END) AS rating_92,
                       COUNT(CASE WHEN rating =91  THEN 1 ELSE NULL END) AS rating_91
                       from data_warehouse.facts_listing"""

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["rating_100","rating_99","rating_98","rating_97","rating_96","rating_95","rating_94","rating_93","rating_92","rating_91"])
    ratings  = [df.rating_100[0],df.rating_99[0],df.rating_98[0],df.rating_97[0],df.rating_96[0],df.rating_95[0],df.rating_94[0],df.rating_93[0],df.rating_92[0],df.rating_91[0]]
    columns=["rating_100","rating_99","rating_98","rating_97","rating_96","rating_95","rating_94","rating_93","rating_92","rating_91"]

    plt.figure(figsize=[15,15])

    X = np.arange(len(ratings))
    plt.bar(columns, ratings, color = 'green', width = 0.25)
    for i in range(len(columns)):
        plt.annotate(ratings[i], (i - 0.1, ratings[i]))
    plt.title("Which are the top-ratings properties", fontsize="20")
    plt.xlabel('rating scores')
    plt.ylabel('number of listings')    

    plt.show()




except Exception as e:
    print(str(e))    
