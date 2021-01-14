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

    query = """SELECT name,calculated_listings,total_listings 
            FROM data_warehouse.facts_host
            WHERE NAME NOT IN (SELECT name FROM data_warehouse.facts_host ORDER BY calculated_listings DESC FETCH FIRST 10 ROWS ONLY)
            ORDER BY total_listings DESC 
            FETCH FIRST 10 ROWS ONLY"""

    cursor.execute(query)
    result = cursor.fetchall()

    df = pd.DataFrame(list(result),columns=["name","calculated_listings","total_listings"])

    # Declaring the figure or the plot (y, x) or (width, height)
    plt.figure(figsize=[15, 15])
    # Data to be plotted
    calculated_listings = df.calculated_listings
    total_listings = df.total_listings
    name = df.name
    # Using numpy to group different data with bars
    X = np.arange(len(calculated_listings))
    # Passing the parameters to the bar function, this is the main function which creates the bar plot
    # Using X now to align the bars side by side
    plt.bar(X, calculated_listings, color = 'green', width = 0.25)
    plt.bar(X + 0.25, total_listings, color = 'red', width = 0.25)
    # This is the location for the annotated text
    i = 1.0
    j = 1.0
    # Annotating the bar plot with the values
    for i in range(len(name)):
        plt.annotate(calculated_listings[i], (-0.1 + i, calculated_listings[i] + j))
        plt.annotate(total_listings[i], (i + 0.15, total_listings[i] + j))
    # Creating the legend of the bars in the plot
    plt.legend(['calculated_listings', 'total_listings'])
    # Overiding the x axis with the country names
    plt.xticks([i + 0.25 for i in range(10)], name)
    # Giving the tilte for the plot
    plt.title("Which hosts are the idlest", fontsize="20")
    # Namimg the x and y axis
    plt.xlabel('name')
    plt.ylabel('listings')
    plt.tick_params(axis='both',which='major',labelsize=10)
    # Displaying the bar plot
    plt.show()

    cursor.close()

except Exception as e:
    print(str(e))    

    
