import psycopg2
import config
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import locale
locale.setlocale(locale.LC_ALL, 'el_GR.UTF-8')
from collections import OrderedDict

try:
    connection = psycopg2.connect(
        database=config.DATABASE, user=config.USER, password=config.PASSWORD, host=config.HOST
    )
    print("connected to db")

    cursor = connection.cursor()

    query1 = """SELECT listing_id,total_earnings from  data_warehouse.facts_listing
                order by total_earnings desc
                fetch first 10 rows only"""

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["listing_id","total_earnings"])
    listing_id = df.listing_id
    total_earnings = df.total_earnings

    plt.figure(figsize=[15, 15])
    X = np.arange(len(total_earnings))
    plt.bar(X, total_earnings, color = 'green', width = 0.25)
    i = 1.0
    j = 1
    for i in range(len(listing_id)):
        plt.annotate(total_earnings[i], (i - 0.1, total_earnings[i]))  
    plt.xticks([i for i in range(10)], listing_id)
    plt.title("Which are the top sales properties by total earnings", fontsize="20")
    plt.xlabel('listing_id')
    plt.ylabel('total_earnings in millions €')
    plt.tick_params(axis='both',which='major',labelsize=10)
    plt.savefig('total_earnings.png')
    plt.show()
    

    cursor.close()

except Exception as e:
    print(str(e))    
