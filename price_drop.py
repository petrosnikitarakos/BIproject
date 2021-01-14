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

    query1 = "select sum(total_sales),sum(winter_sales),sum(spring_sales),sum(summer_sales),sum(autumn_sales) from data_warehouse.facts_listing" 

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["total_sales","winter_sales","spring_sales","summer_sales","autumn_sales"])
    total_sales = df.total_sales
    winter_sales = df.winter_sales
    spring_sales = df.spring_sales
    summer_sales = df.summer_sales
    autumn_sales = df.autumn_sales
    slices = [df.winter_sales[0],df.spring_sales[0],df.summer_sales[0],df.autumn_sales[0]]
    sales = ["winter_sales","spring_sales","summer_sales","autumn_sales"]
    colors = [ 'grey','green','orange','yellow']

    plt.title("Seasonal Sales of the company", fontsize="20")
    plt.pie(slices,labels = sales,colors=colors,startangle=90,shadow=True,explode=(0,0,0,0),autopct='%1.1f%%')
    plt.show()




except Exception as e:
    print(str(e))    
