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

    query1 = "select listing_id,winter_sales,spring_sales,summer_sales,autumn_sales from data_warehouse.facts_listing where winter_sales> 90 and summer_sales < 90 fetch first 5 rows only"

    cursor.execute(query1)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["listing_id","winter_sales","spring_sales","summer_sales","autumn_sales"])

    # Declaring the figure or the plot (y, x) or (width, height)
    plt.figure(figsize=[15, 15])
    # Data to be plotted
    listing_id = df.listing_id
    winter_sales = df.winter_sales
    spring_sales = df.spring_sales
    summer_sales = df.summer_sales
    autumn_sales = df.autumn_sales
    # Using numpy to group different data with bars
    X = np.arange(len(winter_sales))
    # Passing the parameters to the bar function, this is the main function which creates the bar plot
    # Using X now to align the bars side by side
    plt.bar(X, winter_sales, color = 'grey', width = 0.15)
    plt.bar(X + 0.15, spring_sales, color = 'green', width = 0.15)
    plt.bar(X + 0.30, summer_sales, color = 'yellow', width = 0.15)
    plt.bar(X + 0.45, autumn_sales, color = 'orange', width = 0.15)
    # This is the location for the annotated text
    i = 1.0
    j = 1.0
    # Annotating the bar plot with the values
    for i in range(len(listing_id)):
        plt.annotate(winter_sales[i], (-0.1 + i, winter_sales[i] + j))
        plt.annotate(spring_sales[i], (i + 0.15, spring_sales[i] + j))
        plt.annotate(summer_sales[i], (i + 0.30, summer_sales[i] + j))
        plt.annotate(autumn_sales[i], (i + 0.45, autumn_sales[i] + j))
    # Creating the legend of the bars in the plot
    plt.legend(['winter_sales', 'spring_sales','summer_sales','autumn_sales'])
    # Overiding the x axis with the country names
    plt.xticks([i + 0.15 for i in range(5)], listing_id)
    # Giving the tilte for the plot
    plt.title("Seasonal sales for listings with soldout winter_sales", fontsize="20")
    # Namimg the x and y axis
    plt.xlabel('listings')
    plt.ylabel('number of sales')
    plt.tick_params(axis='both',which='major',labelsize=10)
    # Displaying the bar plot
    plt.show()

    cursor.close()

except Exception as e:
    print(str(e))    

    
