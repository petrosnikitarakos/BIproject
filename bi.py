# -*- coding: utf-8 -*--------------
"""
Created on Sun Nov 29 19:12:34 2020
@author: user
"""
import psycopg2
import config
from csv import reader


try:
    connection = psycopg2.connect(
        database=config.DATABASE, user=config.USER, password=config.PASSWORD, host=config.HOST
    )
    print("connected to db")
    
    
    cursor = connection.cursor()

    create_schema = """CREATE SCHEMA IF NOT EXISTS staging_athens;"""

    create_table1 = """CREATE TABLE IF NOT EXISTS staging_athens.ratings (
        rating_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        first_review DATE NOT NULL,
        last_review DATE NOT NULL,
        number_of_reviews INT NOT NULL,
        number_of_reviews_ltm INT NOT NULL,
        review_scores_rating VARCHAR NOT NULL,
        review_scores_accuracy VARCHAR NOT NULL,
        review_scores_cleanliness VARCHAR NOT NULL,
        review_scores_checkin VARCHAR NOT NULL,
        review_scores_communication VARCHAR NOT NULL,
        review_scores_location VARCHAR NOT NULL,
        review_scores_value VARCHAR NOT NULL,
        reviews_per_month VARCHAR NOT NULL
    );"""

    create_table2 = """CREATE TABLE IF NOT EXISTS staging_athens.booking (
        booking_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        availability_30 INT NOT NULL,
        availability_60 INT NOT NULL,
        availability_90 INT NOT NULL,
        availability_365 INT NOT NULL,
        has_availability TEXT NOT NULL,
        instant_bookable TEXT NOT NULL,
        minimum_nights INT NOT NULL,
        maximum_nights INT NOT NULL,
        minimum_minimum_nights INT NOT NULL,
        maximum_minimum_nights INT NOT NULL,
        minimum_maximum_nights INT NOT NULL,
        maximum_maximum_nights INT NOT NULL,
        minimum_nights_avg_ntm FLOAT NOT NULL,
        maximum_nights_avg_ntm FLOAT NOT NULL
        );"""

    create_table3 = """CREATE TABLE IF NOT EXISTS staging_athens.properties (
        properties_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        accommodates INT NOT NULL,
        amenities VARCHAR NOT NULL,
        bathrooms VARCHAR NOT NULL,
        bedrooms VARCHAR NOT NULL,
        beds VARCHAR NOT NULL,
        bed_type TEXT NOT NULL,
        property_type TEXT NOT NULL,
        room_type TEXT NOT NULL,
        square_feet VARCHAR NOT NULL,
        space VARCHAR NOT NULL,
        summary VARCHAR NOT NULL,
        description VARCHAR NOT NULL
        );"""    

    create_table4 = """CREATE TABLE IF NOT EXISTS staging_athens.facilities (
        facilities_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        cancellation_policy VARCHAR NOT NULL,
        is_business_travel_ready TEXT NOT NULL,
        license VARCHAR NOT NULL,
        market TEXT NOT NULL,
        require_guest_phone_verification TEXT NOT NULL,
        require_guest_profile_picture TEXT NOT NULL,
        requires_license TEXT NOT NULL,
        access VARCHAR NOT NULL,
        experiences_offered VARCHAR NOT NULL,
        interaction VARCHAR NOT NULL,
        notes VARCHAR NOT NULL,
        transit VARCHAR NOT NULL
        );"""  

    create_table5 = """CREATE TABLE IF NOT EXISTS staging_athens.location (
        location_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        city VARCHAR NOT NULL,
        country VARCHAR NOT NULL,
        country_code VARCHAR NOT NULL,
        is_location_exact TEXT NOT NULL,
        latitude VARCHAR NOT NULL,
        longitude VARCHAR NOT NULL,
        smart_location VARCHAR NOT NULL,
        state VARCHAR NOT NULL,
        street VARCHAR NOT NULL,
        zipcode VARCHAR NOT NULL,
        neighbourhood VARCHAR NOT NULL
        );""" 

    create_table6 = """CREATE TABLE IF NOT EXISTS staging_athens.price (
        price_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        cleaning_fee VARCHAR NOT NULL,
        extra_people FLOAT NOT NULL,
        price FLOAT NOT NULL,
        security_deposit VARCHAR NOT NULL,
        weekly_price VARCHAR NOT NULL,
        monthly_price VARCHAR NOT NULL,
        currency TEXT NOT NULL
        );"""         

    create_table7 = """CREATE TABLE IF NOT EXISTS staging_athens.host (
        host_id INT NOT NULL PRIMARY KEY,
        host_name	TEXT NOT NULL,
        host_since	DATE NOT NULL,
        host_location	VARCHAR NOT NULL,
        host_response_time	VARCHAR NOT NULL,
        host_response_rate	VARCHAR NOT NULL,
        host_is_superhost	TEXT NOT NULL,
        host_neighbourhood	VARCHAR NOT NULL,
        host_listings_count	INTEGER NOT NULL,
        host_total_listings_count	INT NOT NULL,
        host_verifications	VARCHAR NOT NULL,
        host_has_profile_pic	TEXT NOT NULL,
        host_identity_verified	TEXT NOT NULL,
        calculated_host_listings_count	INTEGER NOT NULL,
        calculated_host_listings_count_entire_homes	INTEGER NOT NULL,
        calculated_host_listings_count_private_rooms	INTEGER NOT NULL,
        calculated_host_listings_count_shared_rooms	INTEGER NOT NULL,
        host_about VARCHAR NOT NULL
        );"""    

    create_table8 = """CREATE TABLE IF NOT EXISTS staging_athens.host_listing_map (
        host_id INT NOT NULL,
        listing_id INT NOT NULL PRIMARY KEY
        );"""      

    create_table9 = """CREATE TABLE IF NOT EXISTS staging_athens.logs (
        log_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        calendar_updated VARCHAR NOT NULL,
        calendar_last_scraped DATE NOT NULL,
        scrape_id VARCHAR NOT NULL,
        last_scraped DATE NOT NULL
        );"""          

    create_table10 = """CREATE TABLE IF NOT EXISTS staging_athens.calendar (
        calendar_id SERIAL,
        listing_id INT NOT NULL,
        date DATE NOT NULL,
        available VARCHAR NOT NULL,
        price FLOAT NOT NULL,
        adjusted_price FLOAT NOT NULL,
        minimum_nights VARCHAR NOT NULL,
        maximum_nights VARCHAR NOT NULL
        );"""   

    create_table11 = """CREATE TABLE IF NOT EXISTS staging_athens.reviews (
        listing_id INT NOT NULL,
        review_id INT NOT NULL PRIMARY KEY,
        date DATE NOT NULL,
        reviewer_id INT NOT NULL,
        reviewer_name VARCHAR NOT NULL,
        comments VARCHAR NOT NULL
        );"""         

    cursor.execute(create_schema)
    cursor.execute(create_table1)
    cursor.execute(create_table2)
    cursor.execute(create_table3)
    cursor.execute(create_table4)
    cursor.execute(create_table5)  
    cursor.execute(create_table6)    
    cursor.execute(create_table7)   
    cursor.execute(create_table8)
    cursor.execute(create_table9)
    cursor.execute(create_table10) 
    cursor.execute(create_table11)         
    connection.commit()
    print("STAGING TABLES CREATED")
    
    def check_null(value):
        if(value == ""):
            return "'N/A'"   
        return "'" + value + "'"

    def check_boolean(value):
        if(value == ""):
            value = "'N/A'" 
        elif(value == "t"):
            value = "YES"
        else: 
            value = "NO"    
        return "'" + value + "'"
    
    def check_date_null(value):
        if(value == ""):
            return "'01-01-0001'"
        return "'" + value + "'"  

    def value_in_euro(value):
        if(value == ""):
            return "'N/A'"
        value = value.replace(",","")           
        return str(round(float(value[1:]) * 1.22,2))

    def replace_quotes(value):
        value = value.replace("'","''")
        return value

    def any_value(value):
        return "'" + value + "'"

    def transform_reviews():
        
        with open('reviews.csv', 'r', encoding ="utf8") as listings_data:    
          old_reviews = reader(listings_data)
          next(old_reviews)
          for reviews in old_reviews:
            listing_id = reviews[0]
            review_id = reviews[1]
            date = check_date_null(reviews[2])
            reviewer_id = reviews[3]
            reviewer_name = any_value(replace_quotes(reviews[4]))      
            comments = any_value(replace_quotes(reviews[5]))          
            new_reviews = [listing_id,review_id,date,reviewer_id,reviewer_name,comments]
            
            upload_table("staging_athens.reviews",config.reviews_names,','.join(new_reviews))

        return print("reviews table inserted correctly")

    def transform_calendar():

        with open('calendar.csv', 'r', encoding ="utf8") as listings_data:
          old_calendar = reader(listings_data)
          next(old_calendar)
          for calendar in old_calendar:
            listing_id = calendar[0]
            date = check_date_null(calendar[1])
            available = check_boolean(calendar[2])
            price = value_in_euro(calendar[3])
            adjusted_price = value_in_euro(calendar[4])      
            minimum_nights = calendar[5]
            maximum_nights = calendar[6]                 
            new_calendar = [listing_id,date,available,price,adjusted_price,minimum_nights,maximum_nights]
            
            upload_table("staging_athens.calendar",config.calendar_names,','.join(new_calendar))

        return print("calendar table inserted correctly")

    def transform_listings():
        
        with open('listings.csv', 'r', encoding ="utf8") as listings_data:
          old_listings = reader(listings_data)
          next(old_listings)
          for listings in old_listings:
            listing_id = listings[0]
            host_id = listings[19]
            calendar_updated = check_null(listings[75])
            calendar_last_scraped = check_date_null(listings[81])
            scrape_id = check_null(listings[2])
            last_scraped = check_date_null(listings[3])    
            host_name = check_null(replace_quotes(listings[21]))
            host_since = check_date_null(listings[22])
            host_location = check_null(replace_quotes(listings[23]))
            host_response_time = check_null(listings[25]) 
            host_response_rate = check_null(listings[26])
            host_is_superhost = check_boolean(listings[28])
            host_neighbourhood = check_null(replace_quotes(listings[31]))
            host_listings_count = check_null(listings[32])
            host_total_listings_count = check_null(listings[33])
            host_verifications = check_null(replace_quotes(listings[34]))
            host_has_profile_pic = check_boolean(listings[35])
            host_identity_verified = check_boolean(listings[36])
            calculated_host_listings_count = check_null(listings[101])
            calculated_host_listings_count_entire_homes = check_null(listings[102])
            calculated_host_listings_count_private_rooms = check_null(listings[103])
            calculated_host_listings_count_shared_rooms = check_null(listings[104])
            host_about = any_value(replace_quotes(check_null(listings[24])))
            cleaning_fee = value_in_euro(listings[64])
            extra_people = value_in_euro(listings[66])
            price = value_in_euro(listings[60])
            security_deposit = value_in_euro(listings[63])
            weekly_price = value_in_euro(listings[61])
            monthly_price = value_in_euro(listings[62])
            currency = "'EUR'"
            city = check_null(listings[41])
            country = check_null(listings[47])
            country_code = check_null(listings[46])
            is_location_exact = check_boolean(listings[50])
            latitude = check_null(listings[48])
            longitude = check_null(listings[49])
            smart_location = check_null(listings[45])
            state = check_null(listings[42])
            street = check_null(listings[37])
            zipcode = check_null(listings[43])
            neighbourhood = check_null(listings[38])
            cancellation_policy = check_null(listings[98])
            is_business_travel_ready = check_boolean(listings[97])
            license = check_null(listings[94])
            market = check_null(listings[44])
            require_guest_phone_verification = check_boolean(listings[99])
            require_guest_profile_picture = check_boolean(listings[100])
            requires_license = check_boolean(listings[93])
            access = any_value(replace_quotes(check_null(listings[12])))
            experiences_offered = any_value(replace_quotes(check_null(listings[8])))
            interaction = any_value(replace_quotes(check_null(listings[13])))
            notes= any_value(replace_quotes(check_null(listings[10])))
            transit = any_value(replace_quotes(check_null(listings[11])))
            accommodates = check_null(listings[53])
            amenities = check_null(listings[58])
            bathrooms = check_null(listings[54])
            bedrooms = check_null(str(listings[55]))
            beds = check_null(listings[56]) 
            bed_type = check_null(listings[57])
            property_type = check_null(listings[51])
            room_type = check_null(listings[52])
            square_feet = check_null(listings[59])
            space = any_value(replace_quotes(check_null(listings[6])))
            summary = any_value(replace_quotes(check_null(listings[5])))
            description = any_value(replace_quotes(check_null(listings[7])))
            availability_30 = check_null(listings[77])
            availability_60 = check_null(listings[78])
            availability_90 = check_null(listings[79])
            availability_365 = check_null(listings[80])
            has_availability = check_boolean(listings[76]) 
            instant_bookable = check_boolean(listings[96])
            minimum_nights = check_null(listings[67])
            maximum_nights = check_null(listings[68])
            minimum_minimum_nights = check_null(listings[69])
            maximum_minimum_nights = check_null(listings[70])
            minimum_maximum_nights = check_null(listings[71])
            maximum_maximum_nights = check_null(listings[72])
            minimum_nights_avg_ntm = check_null(listings[73])
            maximum_nights_avg_ntm = check_null(listings[74])
            first_review = check_date_null(listings[84])
            last_review = check_date_null(listings[85])
            number_of_reviews = listings[82]
            number_of_reviews_ltm = listings[83]
            review_scores_rating = check_null(listings[86]) 
            review_scores_accuracy = check_null(listings[87])
            review_scores_cleanliness = check_null(listings[88])
            review_scores_checkin = check_null(listings[89])
            review_scores_communication = check_null(listings[90])
            review_scores_location = check_null(listings[91])
            review_scores_value = check_null(listings[92])
            reviews_per_month = check_null(listings[105])
             
            new_host = [host_id,host_name,host_since,host_location,host_response_time,host_response_rate,host_is_superhost,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms,host_about] 
            new_logs = [listing_id,calendar_updated,calendar_last_scraped,scrape_id,last_scraped]
            new_host_listing_map = [host_id,listing_id,]
            new_price = [listing_id,cleaning_fee,extra_people,price,security_deposit,weekly_price,monthly_price,currency]
            new_location = [listing_id,city,country,country_code,is_location_exact,latitude,longitude,smart_location,state,street,zipcode,neighbourhood]
            new_facilities = [listing_id,cancellation_policy,is_business_travel_ready,license,market,require_guest_phone_verification,require_guest_profile_picture,requires_license,access,experiences_offered,interaction,notes,transit]
            new_properties = [listing_id,accommodates,amenities,bathrooms,bedrooms,beds,bed_type,property_type,room_type,square_feet,space,summary,description]
            new_booking = [listing_id, availability_30, availability_60,availability_90,availability_365,has_availability,instant_bookable,minimum_nights,maximum_nights,minimum_minimum_nights,maximum_minimum_nights,minimum_maximum_nights,maximum_maximum_nights,minimum_nights_avg_ntm,maximum_nights_avg_ntm ]
            new_ratings = [listing_id, first_review, last_review, number_of_reviews, number_of_reviews_ltm, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, reviews_per_month ]

            upload_table("staging_athens.logs",config.logs_names,','.join(new_logs))
            upload_table("staging_athens.host_listing_map",config.host_listing_map_names,','.join(new_host_listing_map))
            upload_table("staging_athens.host",config.host_names,','.join(new_host)) 
            upload_table("staging_athens.price",config.price_names,','.join(new_price)) 
            upload_table("staging_athens.location",config.location_names,','.join(new_location))     
            upload_table("staging_athens.facilities",config.facilities_names,','.join(new_facilities))  
            upload_table("staging_athens.properties",config.properties_names,','.join(new_properties)) 
            upload_table("staging_athens.booking",config.booking_names,','.join(new_booking))   
            upload_table("staging_athens.ratings",config.rating_names,','.join(new_ratings))   

        return print("listings tables inserted correctly")

    def upload_table(table,columns,values):

        query = "INSERT INTO " + table + "("  + columns + ")" + "VALUES (" + values + ")"
        try: cursor.execute(query)
        except psycopg2.IntegrityError as d:
            connection.rollback()
        else:
            connection.commit()
            print(table + " insert executed correctly")

    transform_calendar()
    transform_reviews()
    transform_listings()
  

except Exception as e:
    print(str(e))    
