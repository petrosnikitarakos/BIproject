import psycopg2
import config

try:
    connection = psycopg2.connect(
        database=config.DATABASE, user=config.USER, password=config.PASSWORD, host=config.HOST
    )
    print("connected to db")   
  
    cursor = connection.cursor()

    create_schema = """CREATE SCHEMA IF NOT EXISTS data_warehouse;"""

    create_table1 = """CREATE TABLE IF NOT EXISTS data_warehouse.facts_listing (
        listing_id INT NOT NULL PRIMARY KEY,
        rating INT NOT NULL,
        number_of_ratings INT NOT NULL,
        number_of_ratings_ltm INT NOT NULL,
        first_rating_date DATE NOT NULL,
        last_rating_date DATE NOT NULL,
        total_sales INT NOT NULL,
        winter_sales INT NOT NULL,
        spring_sales INT NOT NULL,
        summer_sales INT NOT NULL,
        autumn_sales INT NOT NULL,
        average_price FLOAT NOT NULL,
        average_price_sold FLOAT NOT NULL,
        total_earnings FLOAT NOT NULL
    );"""

    create_table2 = """CREATE TABLE IF NOT EXISTS data_warehouse.facts_host (
        host_id INT NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        since date,
        calculated_listings INT NOT NULL,
        total_listings INT NOT NULL,
        entire_homes INT NOT NULL,
        private_rooms INT NOT NULL,
        shared_rooms INT NOT NULL
    );"""

    create_table3 = """CREATE TABLE IF NOT EXISTS data_warehouse.dim_host (
        host_id INT NOT NULL PRIMARY KEY,
        location VARCHAR NOT NULL,
        response_time VARCHAR NOT NULL,
        response_rate VARCHAR NOT NULL,
        superhost VARCHAR NOT NULL,
        neighbourhood VARCHAR NOT NULL,
        verifications VARCHAR NOT NULL,
        profile_pic VARCHAR NOT NULL,
        identity_verified VARCHAR NOT NULL,
        about VARCHAR NOT NULL
    );"""    

    create_table4 = """CREATE TABLE IF NOT EXISTS data_warehouse.dim_ratings (
        listing_id INT NOT NULL PRIMARY KEY,
        accuracy VARCHAR NOT NULL,
        cleanliness VARCHAR NOT NULL,
        checkin VARCHAR NOT NULL,
        communication VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        reviews_per_month VARCHAR NOT NULL
    );"""        

    create_table5 = """CREATE TABLE IF NOT EXISTS data_warehouse.dim_costs_and_fees (
        listing_id INT NOT NULL PRIMARY KEY,
        cleaning_cost VARCHAR NOT NULL,
        extra_people_cost VARCHAR NOT NULL,
        security_deposit_cost VARCHAR NOT NULL,
        weekly_offer VARCHAR NOT NULL,
        monthly_offer VARCHAR NOT NULL
    );"""  

    create_table6 = """CREATE TABLE IF NOT EXISTS data_warehouse.dim_utilities (
        listing_id INT NOT NULL PRIMARY KEY,
        accommodates INT NOT NULL,
        bathrooms VARCHAR NOT NULL,
        bedrooms VARCHAR NOT NULL,
        beds VARCHAR NOT NULL,
        bed_type TEXT NOT NULL,
        property_type TEXT NOT NULL,
        room_type TEXT NOT NULL,
        cancellation_policy VARCHAR NOT NULL,
        is_business_travel_ready TEXT NOT NULL,
        require_guest_phone_verification TEXT NOT NULL,
        require_guest_profile_picture TEXT NOT NULL,
        requires_license TEXT NOT NULL,
        city VARCHAR NOT NULL,
        country VARCHAR NOT NULL,
        latitude VARCHAR NOT NULL,
        longitude VARCHAR NOT NULL,
        neighbourhood VARCHAR NOT NULL,     
        zipcode VARCHAR NOT NULL  
    );"""    

    create_table7 = """CREATE TABLE IF NOT EXISTS data_warehouse.host_listing_map (
    listing_id INT NOT NULL PRIMARY KEY,
    host_id INT NOT NULL
    );"""      

    cursor.execute(create_schema)
    cursor.execute(create_table1)
    cursor.execute(create_table2)
    cursor.execute(create_table3)
    cursor.execute(create_table4)
    cursor.execute(create_table5)
    cursor.execute(create_table6)
    cursor.execute(create_table7)
    connection.commit()
    print("DW TABLES CREATED")

    def insert_query(query):
        cursor = connection.cursor()
        try: cursor.execute(query)
        except psycopg2.IntegrityError as d:
            connection.rollback()
            print("row already exists")
        else:
            connection.commit()
            print("query executed successfully")  

    query1= """INSERT INTO data_warehouse.facts_listing (listing_id,rating,number_of_ratings,number_of_ratings_ltm,first_rating_date,last_rating_date,total_sales,winter_sales,spring_sales,summer_sales,autumn_sales,average_price,average_price_sold,total_earnings)
                        SELECT A.LISTING_ID,
                        CAST(B.REVIEW_SCORES_RATING AS INT),
                        B.NUMBER_OF_REVIEWS,
                        B.number_of_reviews_ltm,
                        B.first_review,B.last_review,
                        COUNT(A.LISTING_ID),
                        COUNT(CASE WHEN date_part('month', DATE) IN (12,01,02) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (03,04,05) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (06,07,08) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (09,10,11) THEN 1 ELSE NULL END),
                        C.PRICE,
                        round(avg(CAST(A.PRICE as numeric)), 2),
                        round(sum(CAST(A.PRICE as numeric)), 2)
                        FROM staging_athens.CALENDAR A,staging_athens.RATINGS B,staging_athens.PRICE C
                        where A.LISTING_ID = B.LISTING_ID
                        AND A.LISTING_ID = C.LISTING_ID
                        AND A.AVAILABLE = 'NO'
                        AND B.NUMBER_OF_REVIEWS <> 0
                        AND B.REVIEW_SCORES_RATING <> 'N/A'
                        GROUP BY A.LISTING_ID,B.NUMBER_OF_REVIEWS,B.number_of_reviews_ltm,B.first_review,B.last_review,B.REVIEW_SCORES_RATING,C.PRICE
                        UNION
                        SELECT A.LISTING_ID,
                        CAST(B.REVIEW_SCORES_RATING AS INT),
                        B.NUMBER_OF_REVIEWS,
                        B.number_of_reviews_ltm,
                        B.first_review,B.last_review,
                        COUNT(A.LISTING_ID),
                        COUNT(CASE WHEN date_part('month', DATE) IN (12,01,02) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (03,04,05) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (06,07,08) THEN 1 ELSE NULL END),
                        COUNT(CASE WHEN date_part('month', DATE) IN (09,10,11) THEN 1 ELSE NULL END),
                        C.PRICE,
                        round(avg(CAST(A.PRICE as numeric)), 2),
                        round(sum(CAST(A.PRICE as numeric)), 2)
                        FROM staging_thessaloniki.CALENDAR A,staging_thessaloniki.RATINGS B,staging_thessaloniki.PRICE C
                        where A.LISTING_ID = B.LISTING_ID
                        AND A.LISTING_ID = C.LISTING_ID
                        AND A.AVAILABLE = 'NO'
                        AND B.NUMBER_OF_REVIEWS <> 0
                        AND B.REVIEW_SCORES_RATING <> 'N/A'
                        GROUP BY A.LISTING_ID,B.NUMBER_OF_REVIEWS,B.number_of_reviews_ltm,B.first_review,B.last_review,B.REVIEW_SCORES_RATING,C.PRICE
                    """

    query2 = """INSERT INTO data_warehouse.facts_host (host_id,name,since,calculated_listings,total_listings,entire_homes,private_rooms,shared_rooms)
						select host_id,
                        host_name,
                        host_since,
                        calculated_host_listings_count,
                        host_total_listings_count,
                        calculated_host_listings_count_entire_homes,
                        calculated_host_listings_count_private_rooms,
                        calculated_host_listings_count_shared_rooms
                        from staging_athens.host
                        UNION
                        select host_id,
                        host_name,
                        host_since,
                        CAST(calculated_host_listings_count AS INT),
                        CAST(host_total_listings_count AS INT),
                        CAST(calculated_host_listings_count_entire_homes AS INT),
                        CAST(calculated_host_listings_count_private_rooms AS INT),
                        CAST(calculated_host_listings_count_shared_rooms AS INT)
                        from staging_thessaloniki.host 
                        where host_name <> 'N/A' 
						AND HOST_ID NOT IN (SELECT HOST_ID FROM STAGING_ATHENS.HOST)
                    """

    query3 = """INSERT INTO data_warehouse.dim_host (host_id,location,response_time,response_rate,superhost,neighbourhood,verifications,profile_pic,identity_verified,about)
                        select host_id,
                        host_location,
                        host_response_time,
                        host_response_rate,
                        host_is_superhost,
                        host_neighbourhood,
                        host_verifications,
                        host_has_profile_pic,
                        host_identity_verified,
                        host_about
                        from staging_athens.host
                        where host_location <> 'N/A'
                        AND    host_response_time <>  'N/A'
                        AND    host_response_rate <>  'N/A'
                        AND    host_is_superhost <>  'N/A'
                        AND    host_neighbourhood <>  'N/A'
                        AND    host_verifications <>  'N/A'
                        AND    host_has_profile_pic <>  'N/A'
                        AND    host_identity_verified <>  'N/A'
                        AND    host_about <>  '''N/A'''
                        UNION
                        select host_id,
                        host_location,
                        host_response_time,
                        host_response_rate,
                        host_is_superhost,
                        host_neighbourhood,
                        host_verifications,
                        host_has_profile_pic,
                        host_identity_verified,
                        host_about
                        from staging_thessaloniki.host
                        where host_location <> 'N/A'
                        AND    host_response_time <>  'N/A'
                        AND    host_response_rate <>  'N/A'
                        AND    host_is_superhost <>  'N/A'
                        AND    host_neighbourhood <>  'N/A'
                        AND    host_verifications <>  'N/A'
                        AND    host_has_profile_pic <>  'N/A'
                        AND    host_identity_verified <>  'N/A'
                        AND    host_about <>  '''N/A'''
                    """

    query4 = """INSERT INTO data_warehouse.dim_ratings (listing_id,accuracy,cleanliness,checkin,communication,location,value,reviews_per_month)
                        select listing_id,
                        review_scores_accuracy,
                        review_scores_cleanliness,
                        review_scores_checkin,
                        review_scores_communication,
                        review_scores_location,
                        review_scores_value,
                        reviews_per_month
                        from staging_athens.ratings
                        where  review_scores_accuracy <>  'N/A'
                        AND    review_scores_cleanliness <>  'N/A'
                        AND    review_scores_checkin <>  'N/A'
                        AND    review_scores_communication <>  'N/A'
                        AND    review_scores_location <>  'N/A'
                        AND    review_scores_value <>  'N/A'
                        AND    reviews_per_month <>  'N/A'
                        UNION                         
                        select listing_id,
                        review_scores_accuracy,
                        review_scores_cleanliness,
                        review_scores_checkin,
                        review_scores_communication,
                        review_scores_location,
                        review_scores_value,
                        reviews_per_month
                        from staging_thessaloniki.ratings
                        where  review_scores_accuracy <>  'N/A'
                        AND    review_scores_cleanliness <>  'N/A'
                        AND    review_scores_checkin <>  'N/A'
                        AND    review_scores_communication <>  'N/A'
                        AND    review_scores_location <>  'N/A'
                        AND    review_scores_value <>  'N/A'
                        AND    reviews_per_month <>  'N/A'
                    """        

    query5 = """INSERT INTO data_warehouse.dim_costs_and_fees (listing_id,cleaning_cost,extra_people_cost,security_deposit_cost,weekly_offer,monthly_offer)
                        select listing_id,
                        CASE WHEN (CLEANING_FEE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS cleaning_cost,
                        CASE WHEN (EXTRA_PEOPLE IN (0)) THEN 'NO' ELSE ('YES') END AS extra_people_cost,
                        CASE WHEN (SECURITY_DEPOSIT IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS security_deposit_cost,
                        CASE WHEN (WEEKLY_PRICE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS weekly_offer,
                        CASE WHEN (MONTHLY_PRICE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS monthly_offer
                        from staging_athens.price
                        UNION
                        select listing_id,
                        CASE WHEN (CLEANING_FEE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS cleaning_cost,
                        CASE WHEN (EXTRA_PEOPLE IN (0)) THEN 'NO' ELSE ('YES') END AS extra_people_cost,
                        CASE WHEN (SECURITY_DEPOSIT IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS security_deposit_cost,
                        CASE WHEN (WEEKLY_PRICE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS weekly_offer,
                        CASE WHEN (MONTHLY_PRICE IN ('N/A','0.0')) THEN 'NO' ELSE ('YES') END AS monthly_offer
                        from staging_thessaloniki.price
                    """   

    query6 = """INSERT INTO data_warehouse.dim_utilities (listing_id,accommodates,bathrooms,bedrooms,beds,bed_type,property_type,room_type,cancellation_policy,is_business_travel_ready,require_guest_phone_verification,require_guest_profile_picture,requires_license,city,country,latitude,longitude,neighbourhood,zipcode)
                        select A.LISTING_ID,
                        A.ACCOMMODATES,
                        A.BATHROOMS,
                        A.BEDROOMS,
                        A.BEDS,
                        A.BED_TYPE,
                        A.PROPERTY_TYPE,
                        A.ROOM_TYPE,
                        B.CANCELLATION_POLICY,
                        B.IS_BUSINESS_TRAVEL_READY,
                        B.REQUIRE_GUEST_PHONE_VERIFICATION,
                        B.REQUIRE_GUEST_PROFILE_PICTURE,
                        B.REQUIRES_LICENSE,
                        C.CITY,
                        C.COUNTRY,
                        C.LATITUDE,
                        C.LONGITUDE,
                        C.NEIGHBOURHOOD,
                        CASE WHEN (C.ZIPCODE IN ('N/A','close Athens')) THEN '00000' ELSE (REPLACE(C.ZIPCODE,' ','')) END
                        from staging_athens.properties A,staging_athens.FACILITIES B,staging_athens.LOCATION C
                        WHERE A.LISTING_ID = B.LISTING_ID
                        AND A.LISTING_ID = C.LISTING_ID
                        UNION
                        select A.LISTING_ID,
                        A.ACCOMMODATES,
                        A.BATHROOMS,
                        A.BEDROOMS,
                        A.BEDS,
                        A.BED_TYPE,
                        A.PROPERTY_TYPE,
                        A.ROOM_TYPE,
                        B.CANCELLATION_POLICY,
                        B.IS_BUSINESS_TRAVEL_READY,
                        B.REQUIRE_GUEST_PHONE_VERIFICATION,
                        B.REQUIRE_GUEST_PROFILE_PICTURE,
                        B.REQUIRES_LICENSE,
                        C.CITY,
                        C.COUNTRY,
                        C.LATITUDE,
                        C.LONGITUDE,
                        C.NEIGHBOURHOOD,
                        CASE WHEN (C.ZIPCODE IN ('N/A','close Athens')) THEN '00000' ELSE (REPLACE(C.ZIPCODE,' ','')) END
                        from staging_thessaloniki.properties A,staging_thessaloniki.FACILITIES B,staging_thessaloniki.LOCATION C
                        WHERE A.LISTING_ID = B.LISTING_ID
                        AND A.LISTING_ID = C.LISTING_ID
                    """   

    query7 = """INSERT INTO data_warehouse.host_listing_map (listing_id,host_id)
                        select LISTING_ID,
                        host_id
                        from staging_thessaloniki.host_listing_map
                        where host_id in (select host_id from staging_thessaloniki.host)
                        UNION
                        select LISTING_ID,
                        host_id
                        from staging_athens.host_listing_map
                        where host_id in (select host_id from staging_athens.host)
                    """        

    insert_query(query1)
    insert_query(query2)
    insert_query(query3)
    insert_query(query4)
    insert_query(query5)
    insert_query(query6)
    insert_query(query7)


except Exception as e:
    print(str(e))    