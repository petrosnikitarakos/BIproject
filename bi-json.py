# -*- coding: utf-8 -*-
"""
@author: Petros Nikitarakos
"""

import urllib.request, json, psycopg2,config
    
try:
    connection = psycopg2.connect(
        database=config.DATABASE, user=config.USER, password=config.PASSWORD, host=config.HOST
    )
    print("connected to db")
    
    cursor = connection.cursor()
    
    create_schema = """CREATE SCHEMA IF NOT EXISTS staging_thessaloniki"""
    
    cursor.execute(create_schema)
    connection.commit()
    print("SCHEMA FOR THESSALONIKI TABLE CREATED")
    
    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.ratings (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING RATINGS TABLE CREATED")
    
    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.booking (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING BOOKING TABLE CREATED")
    
    
    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.properties (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING PROPERTIES TABLE CREATED")
    
    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.facilities (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING FACILITIES TABLE CREATED")

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.location (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING LOCATION TABLE CREATED")

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.price (
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
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING PRICE TABLE CREATED")    

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.host (
        host_id INT NOT NULL PRIMARY KEY,
        host_name	TEXT NOT NULL,
        host_since	DATE NOT NULL,
        host_location	VARCHAR NOT NULL,
        host_response_time	VARCHAR NOT NULL,
        host_response_rate	VARCHAR NOT NULL,
        host_is_superhost	TEXT NOT NULL,
        host_neighbourhood	VARCHAR NOT NULL,
        host_listings_count	VARCHAR NOT NULL,
        host_total_listings_count	VARCHAR NOT NULL,
        host_verifications	VARCHAR NOT NULL,
        host_has_profile_pic	TEXT NOT NULL,
        host_identity_verified	TEXT NOT NULL,
        calculated_host_listings_count	VARCHAR NOT NULL,
        calculated_host_listings_count_entire_homes	VARCHAR NOT NULL,
        calculated_host_listings_count_private_rooms	VARCHAR NOT NULL,
        calculated_host_listings_count_shared_rooms	VARCHAR NOT NULL,
        host_about VARCHAR NOT NULL
        );"""  
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING HOST TABLE CREATED")

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.host_listing_map (
        host_id INT NOT NULL,
        listing_id INT NOT NULL PRIMARY KEY
        );""" 
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING HOST LISTING MAP TABLE CREATED")      

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.calendar (
        calendar_id SERIAL,
        listing_id INT NOT NULL,
        date DATE NOT NULL,
        available VARCHAR NOT NULL,
        price FLOAT NOT NULL,
        adjusted_price FLOAT NOT NULL,
        minimum_nights VARCHAR NOT NULL,
        maximum_nights VARCHAR NOT NULL
        );"""
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING CALENDAR TABLE CREATED")

    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.reviews (
        review_id SERIAL,
        listing_id INT NOT NULL,
        date DATE NOT NULL,
        reviewer_name VARCHAR NOT NULL,
        comments VARCHAR NOT NULL
        );"""      
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING REVIEWS TABLE CREATED")
    
    create_table = """CREATE TABLE IF NOT EXISTS staging_thessaloniki.logs (
        log_id SERIAL,
        listing_id INT NOT NULL PRIMARY KEY,
        calendar_updated VARCHAR NOT NULL,
        calendar_last_scraped DATE NOT NULL,
        scrape_id VARCHAR NOT NULL,
        last_scraped DATE NOT NULL
        );"""  
    
    cursor.execute(create_table)
    connection.commit()
    print("STAGING LOGS TABLE CREATED")       
    
    
except Exception as e:
    print(str(e))    
    

def upload_table(table,columns,values):

        query = "INSERT INTO " + table + "("  + columns + ")" + "VALUES (" + values + ")"
        try: cursor.execute(query)
        except psycopg2.IntegrityError:
            connection.rollback()
        else:
            connection.commit()
            print("insert executed correctly")
            
            
def insert_row(table,columns,values,key,value):
        
    query = "INSERT INTO " + table + "("  + columns + ")" + "SELECT " + values + " WHERE NOT EXISTS ( SELECT * FROM " + table + " WHERE " + key + " = " + value + ");"
    try: cursor.execute(query)
    except psycopg2.IntegrityError:
        connection.rollback()
    else:
        rowcount = cursor.rowcount
        connection.commit()
        
        print("query executed , rows affected: "+str(rowcount))
            
            
            
def check_null(value):
    if(value == ""):
        return "'N/A'"
    return "'" + value + "'"
    
def check_date_null(value):
    if(value == ""):
        return "'01-01-0001'"
    return "'" + value + "'" 

def check_boolean(value):
        if(value == ""):
            value = "N/A" 
        elif(value == "t"):
            value = "YES"
        else: 
            value = "NO"    
        return "'" + value + "'"
    
def value_in_euro(value):
        if(value == "" or value is None):
            return "'N/A'"
        value = value.replace(",","")           
        return "'"+str(round(float(value[1:]) * 1.22,2))+"'"

def replace_quotes(value):
        value = value.replace("'","''")
        return value

def any_value(value):
        return "'" + value + "'"
            
            
def get_rating_info(id):
        listing_id = id["_id"]
        first_review = check_date_null(id["listing_info"]["first_review"])
        last_review = check_date_null(id["listing_info"]["last_review"])
        number_of_reviews = id["listing_info"]["number_of_reviews"]
        number_of_reviews_ltm = id["listing_info"]["number_of_reviews_ltm"]
        review_scores_rating = check_null(id["listing_info"]["review_scores_rating"])
        review_scores_accuracy = check_null(id["listing_info"]["review_scores_accuracy"])
        review_scores_cleanliness = check_null(id["listing_info"]["review_scores_cleanliness"])
        review_scores_checkin = check_null(id["listing_info"]["review_scores_checkin"])
        review_scores_communication = check_null(id["listing_info"]["review_scores_communication"])
        review_scores_location = check_null(id["listing_info"]["review_scores_location"])
        review_scores_value = check_null(id["listing_info"]["review_scores_value"])
        reviews_per_month = check_null(id["listing_info"]["reviews_per_month"])
        new_ratings = [listing_id, first_review, last_review, number_of_reviews, number_of_reviews_ltm, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, reviews_per_month ]
        row = ','.join(new_ratings)
        rating_names = "listing_id,first_review,last_review,number_of_reviews,number_of_reviews_ltm,review_scores_rating,review_scores_accuracy,review_scores_cleanliness,review_scores_checkin,review_scores_communication,review_scores_location,review_scores_value,reviews_per_month"
        upload_table("staging_thessaloniki.ratings",rating_names,row)
    
    
def get_booking_info(id):
        listing_id = id["_id"]
        availability_30 = id["listing_info"]["availability_30"]
        availability_60 = id["listing_info"]["availability_60"]
        availability_90 = id["listing_info"]["availability_90"]
        availability_365 = id["listing_info"]["availability_365"]
        has_availability = check_boolean(id["listing_info"]["has_availability"])
        instant_bookable = check_boolean(id["listing_info"]["instant_bookable"])
        minimum_nights = id["listing_info"]["minimum_nights"]
        maximum_nights = id["listing_info"]["maximum_nights"]
        minimum_minimum_nights = id["listing_info"]["minimum_minimum_nights"]
        maximum_minimum_nights = id["listing_info"]["maximum_minimum_nights"]
        minimum_maximum_nights = id["listing_info"]["minimum_maximum_nights"]
        maximum_maximum_nights = id["listing_info"]["maximum_maximum_nights"]
        minimum_nights_avg_ntm = float(id["listing_info"]["minimum_nights_avg_ntm"])
        maximum_nights_avg_ntm = float(id["listing_info"]["maximum_nights_avg_ntm"])
        new_booking = [listing_id, availability_30, availability_60, availability_90, availability_365, has_availability, instant_bookable, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, str(minimum_nights_avg_ntm), str(maximum_nights_avg_ntm) ]
        row = ','.join(new_booking)
        booking_names = "listing_id, availability_30, availability_60, availability_90, availability_365,has_availability, instant_bookable, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm"
        upload_table("staging_thessaloniki.booking",booking_names,row)
    
    
def get_properties_info(id):
        listing_id = id["_id"]
        accommodates = check_null(id["listing_info"]["accommodates"])
        amenities = check_null(id["listing_info"]["amenities"])
        bathrooms = check_null(id["listing_info"]["bathrooms"])
        bedrooms = check_null(id["listing_info"]["bedrooms"])
        beds = check_null(id["listing_info"]["beds"])
        bed_type = check_null(id["listing_info"]["bed_type"])
        property_type = check_null(id["listing_info"]["property_type"])
        room_type = check_null(id["listing_info"]["room_type"])
        square_feet = check_null(id["listing_info"]["square_feet"])
        space = any_value(replace_quotes(check_null(id["listing_info"]["space"])))
        summary = any_value(replace_quotes(check_null(id["listing_info"]["summary"])))
        description = any_value(replace_quotes(check_null(id["listing_info"]["description"])))
        new_properties = [listing_id,accommodates,amenities,bathrooms,bedrooms,beds,bed_type,property_type,room_type,square_feet,space,summary,description]
        row = ','.join(new_properties)
        properties_names = "listing_id,accommodates,amenities,bathrooms,bedrooms,beds,bed_type,property_type,room_type,square_feet,space,summary,description"
        upload_table("staging_thessaloniki.properties",properties_names,row)
    
    
def get_facilities_info(id):
        listing_id = id["_id"]
        cancellation_policy = check_null(id["listing_info"]["cancellation_policy"])
        is_business_travel_ready = check_boolean(id["listing_info"]["is_business_travel_ready"])
        license = check_null(id["listing_info"]["license"])
        market = check_null(id["listing_info"]["market"])
        require_guest_phone_verification = check_boolean(id["listing_info"]["require_guest_phone_verification"])
        require_guest_profile_picture = check_boolean(id["listing_info"]["require_guest_profile_picture"])
        requires_license = check_boolean(id["listing_info"]["requires_license"])
        access = any_value(replace_quotes(check_null(id["listing_info"]["access"])))
        experiences_offered = any_value(replace_quotes(check_null(id["listing_info"]["experiences_offered"])))
        interaction = any_value(replace_quotes(check_null(id["listing_info"]["interaction"])))
        notes = any_value(replace_quotes(check_null(id["listing_info"]["notes"])))
        transit = any_value(replace_quotes(check_null(id["listing_info"]["transit"])))
        new_facilities = [listing_id,cancellation_policy,is_business_travel_ready,license,market,require_guest_phone_verification,require_guest_profile_picture,requires_license,access,experiences_offered,interaction,notes,transit]
        row = ','.join(new_facilities)
        facilities_names = "listing_id,cancellation_policy,is_business_travel_ready,license,market,require_guest_phone_verification,require_guest_profile_picture,requires_license,access,experiences_offered,interaction,notes,transit"
        upload_table("staging_thessaloniki.facilities",facilities_names,row)
    


def get_location_info(id):
        listing_id = id["_id"]
        city = check_null(id["listing_info"]["city"])
        country = check_null(id["listing_info"]["country"])
        country_code = check_null(id["listing_info"]["country_code"])
        is_location_exact = check_boolean(id["listing_info"]["is_location_exact"])
        latitude = check_null(id["listing_info"]["latitude"])
        longitude = check_null(id["listing_info"]["longitude"])
        smart_location = check_null(id["listing_info"]["smart_location"])
        state = check_null(id["listing_info"]["state"])
        street = check_null(id["listing_info"]["street"])
        zipcode = check_null(id["listing_info"]["zipcode"])
        neighbourhood = check_null(id["listing_info"]["neighbourhood"])
        new_location = [listing_id,city,country,country_code,is_location_exact,latitude,longitude,smart_location,state,street,zipcode,neighbourhood]
        row = ','.join(new_location)
        location_names = "listing_id,city,country,country_code,is_location_exact,latitude,longitude,smart_location,state,street,zipcode,neighbourhood"
        upload_table("staging_thessaloniki.location",location_names,row)
    

def get_price_info(id):
        listing_id = id["_id"]
        cleaning_fee = value_in_euro(id["listing_info"]["cleaning_fee"])
        extra_people = value_in_euro(id["listing_info"]["extra_people"])
        price = value_in_euro(id["listing_info"]["price"])
        security_deposit = value_in_euro(id["listing_info"]["security_deposit"])
        weekly_price = value_in_euro(id["listing_info"]["weekly_price"])
        monthly_price = value_in_euro(id["listing_info"]["monthly_price"])
        currency = "'EUR'"
        new_price = [listing_id,cleaning_fee,extra_people,price,security_deposit,weekly_price,monthly_price,currency]
        row = ','.join(new_price)
        price_names = "listing_id,cleaning_fee,extra_people,price,security_deposit,weekly_price,monthly_price,currency"
        upload_table("staging_thessaloniki.price",price_names,row)
    
    
def get_host_info(id):
        host_id = id["listing_info"]["host_id"]
        host_name = check_null(replace_quotes(id["listing_info"]["host_name"]))
        host_since = check_date_null(id["listing_info"]["host_since"])
        host_location = check_null(replace_quotes(id["listing_info"]["host_location"]))
        host_response_time = check_null(id["listing_info"]["host_response_time"])
        host_response_rate =  check_null(id["listing_info"]["host_response_rate"])
        host_is_superhost = check_boolean(id["listing_info"]["host_is_superhost"])
        host_neighbourhood = check_null(replace_quotes(id["listing_info"]["host_neighbourhood"]))
        host_listings_count = check_null(id["listing_info"]["host_listings_count"])
        host_total_listings_count = check_null(id["listing_info"]["host_total_listings_count"])
        host_verifications = check_null(replace_quotes(id["listing_info"]["host_verifications"]))
        host_has_profile_pic = check_boolean(id["listing_info"]["host_has_profile_pic"])
        host_identity_verified = check_boolean(id["listing_info"]["host_identity_verified"])
        calculated_host_listings_count = check_null(id["listing_info"]["calculated_host_listings_count"])
        calculated_host_listings_count_entire_homes = check_null(id["listing_info"]["calculated_host_listings_count_entire_homes"])
        calculated_host_listings_count_private_rooms = check_null(id["listing_info"]["calculated_host_listings_count_private_rooms"])
        calculated_host_listings_count_shared_rooms = check_null(id["listing_info"]["calculated_host_listings_count_shared_rooms"])
        host_about = any_value(replace_quotes(check_null(id["listing_info"]["host_about"])))
        new_host = [host_id,host_name,host_since,host_location,host_response_time,host_response_rate,host_is_superhost,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms,host_about]
        row = ','.join(new_host)
        host_names = "host_id,host_name,host_since,host_location,host_response_time,host_response_rate,host_is_superhost,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms,host_about"
        upload_table("staging_thessaloniki.host",host_names,row)
    
def get_host_listing_map_info(id):
        listing_id = id["_id"]
        host_id = id["listing_info"]["host_id"]
        new_host_listing_map = [listing_id,host_id]
        row = ','.join(new_host_listing_map)
        host_listing_map_names = "listing_id,host_id"
        upload_table("staging_thessaloniki.host_listing_map",host_listing_map_names,row)
    

def get_calendar_info(id):
        listing_id = id["_id"]
        for index in id["listing_calendar"]:
            new_calendar = []
            new_calendar.append(listing_id)
            new_calendar.append(check_date_null(index["date"]))
            new_calendar.append(check_boolean(index["available"]))
            new_calendar.append(value_in_euro(index["price"]))
            new_calendar.append(value_in_euro(index["adjusted_price"]))
            new_calendar.append(index["minimum_nights"])
            new_calendar.append(index["maximum_nights"])
            record = ','.join(new_calendar)
            calendar_names = "listing_id,date,available,price,adjusted_price,minimum_nights,maximum_nights"
            upload_table("staging_thessaloniki.calendar",calendar_names,record)
            
        print("calendar records successfully inserted")
    
    
def get_reviews_info(id):
        listing_id = id["_id"]
        for index in id["listing_reviews"]:
            new_reviews = []
            new_reviews.append(listing_id)
            new_reviews.append(check_date_null(index["date"]))
            new_reviews.append(any_value(replace_quotes(index["reviewer_name"])))
            new_reviews.append(any_value(replace_quotes(index["comments"])))
            record = ','.join(new_reviews)
            reviews_names = "listing_id,date,reviewer_name,comments"
            upload_table("staging_thessaloniki.reviews",reviews_names,record)

        print("review records successfully inserted")
    
    
def get_log_info(id):
        listing_id = id["_id"]
        calendar_updated = check_null(id["listing_info"]["calendar_updated"])
        calendar_last_scraped = check_date_null(id["listing_info"]["calendar_last_scraped"])
        scrape_id = check_null(id["listing_info"]["scrape_id"])
        last_scraped = check_date_null(id["listing_info"]["last_scraped"])
        new_log = [listing_id,calendar_updated,calendar_last_scraped,scrape_id,last_scraped]
        row = ','.join(new_log)
        log_names = "listing_id,calendar_updated,calendar_last_scraped,scrape_id,last_scraped"
        upload_table("staging_thessaloniki.logs",log_names,row)
    

get_listing_keys =  []

with urllib.request.urlopen("https://bi-athtech-cw.herokuapp.com/listings") as url:
    data = json.loads(url.read().decode())
    for listing_id in data["listing_ids"]:
        get_listing_keys.append(listing_id)
        

#print(get_listing_keys)

 
for listing_key in get_listing_keys:
    with urllib.request.urlopen("https://bi-athtech-cw.herokuapp.com/listings/"+listing_key) as url:
            index = json.loads(url.read().decode())
            get_rating_info(index)
            get_booking_info(index)
            get_properties_info(index)
            get_facilities_info(index)
            get_location_info(index)
            get_price_info(index)
            get_host_info(index)
            get_host_listing_map_info(index)
            get_calendar_info(index)
            get_reviews_info(index)
            get_log_info(index)
    

"""with urllib.request.urlopen("https://bi-athtech-cw.herokuapp.com/listings") as url:
    #i = 0
    data = json.loads(url.read().decode())
   
    for listing_id in data["listing_ids"]:
        #insert here finalized
        
        with urllib.request.urlopen("https://bi-athtech-cw.herokuapp.com/listings/"+listing_id) as url:
            index = json.loads(url.read().decode())
            get_rating_info(index)
            #get_booking_info(listing_id)
            #get_properties_info(listing_id)
            #get_facilities_info(listing_id)
            #get_location_info(listing_id)
            #get_price_info(listing_id)
            #get_host_info(listing_id)
            #get_host_listing_map_info(listing_id)
            #get_calendar_info(listing_id)
            #get_reviews_info(listing_id)
            #get_log_info(listing_id)
        i = i+1
        if(i<=4):
            #testing purposes only
            
        if(i==4):
            break;"""
