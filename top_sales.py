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

    query1 = """select COUNT(CASE WHEN total_sales = 365 THEN 1 ELSE NULL END) AS SALES_365,
                       COUNT(CASE WHEN total_sales = 364 THEN 1 ELSE NULL END) AS SALES_364,
                       COUNT(CASE WHEN total_sales = 363 THEN 1 ELSE NULL END) AS SALES_363,
                       COUNT(CASE WHEN total_sales = 362 THEN 1 ELSE NULL END) AS SALES_362,
                       COUNT(CASE WHEN total_sales = 361 THEN 1 ELSE NULL END) AS SALES_361,
                       COUNT(CASE WHEN total_sales = 360 THEN 1 ELSE NULL END) AS SALES_360,
                       COUNT(CASE WHEN total_sales = 359 THEN 1 ELSE NULL END) AS SALES_359,
                       COUNT(CASE WHEN total_sales = 358 THEN 1 ELSE NULL END) AS SALES_358,
                       COUNT(CASE WHEN total_sales = 357 THEN 1 ELSE NULL END) AS SALES_357,
                       COUNT(CASE WHEN total_sales = 356 THEN 1 ELSE NULL END) AS SALES_356
                       from data_warehouse.facts_listing"""

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["SALES_365","SALES_364","SALES_363","SALES_362","SALES_361","SALES_360","SALES_359","SALES_358","SALES_357","SALES_356"])
    sales  = [df.SALES_365[0],df.SALES_364[0],df.SALES_363[0],df.SALES_362[0],df.SALES_361[0],df.SALES_360[0],df.SALES_359[0],df.SALES_358[0],df.SALES_357[0],df.SALES_356[0]]
    columns=["SALES_365","SALES_364","SALES_363","SALES_362","SALES_361","SALES_360","SALES_359","SALES_358","SALES_357","SALES_356"]

    plt.figure(figsize=[15,15])

    X = np.arange(len(sales))
    plt.bar(columns, sales, color = 'green', width = 0.25)
    for i in range(len(columns)):
        plt.annotate(sales[i], (i - 0.1, sales[i]))
    plt.title("Which are the top-sales properties", fontsize="20")
    plt.xlabel('days of sales')
    plt.ylabel('number of listings')    

    plt.show()




except Exception as e:
    print(str(e))    
