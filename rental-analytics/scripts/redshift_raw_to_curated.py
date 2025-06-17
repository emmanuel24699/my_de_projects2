import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node stg_bookings
stg_bookings_node1750010305430 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.stg_bookings", "connectionName": "Redshift connection"}, transformation_ctx="stg_bookings_node1750010305430")

# Script generated for node stg_apartment_attributes
stg_apartment_attributes_node1750010306325 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.stg_apartment_attributes", "connectionName": "Redshift connection"}, transformation_ctx="stg_apartment_attributes_node1750010306325")

# Script generated for node stg_apartments
stg_apartments_node1750010304997 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.stg_apartments", "connectionName": "Redshift connection"}, transformation_ctx="stg_apartments_node1750010304997")

# Script generated for node stg_user_viewing
stg_user_viewing_node1750010305887 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.stg_user_viewing", "connectionName": "Redshift connection"}, transformation_ctx="stg_user_viewing_node1750010305887")

# Script generated for node transform_fact_bookings
SqlQuery7311 = '''
SELECT 
    booking_id,
    user_id,
    apartment_id AS listing_id,
    booking_date,
    checkin_date,
    checkout_date,
    total_price,
    currency,
    booking_status
FROM stg_bookings
WHERE booking_status LIKE '%confirmed%'
AND total_price > 0
AND checkin_date IS NOT NULL
AND checkout_date IS NOT NULL
'''
transform_fact_bookings_node1750099266941 = sparkSqlQuery(glueContext, query = SqlQuery7311, mapping = {"stg_bookings":stg_bookings_node1750010305430}, transformation_ctx = "transform_fact_bookings_node1750099266941")

# Script generated for node transform_dim_listings
SqlQuery7313 = '''
-- SELECT 
--     a.id AS listing_id,
--     a.title,
--     a.source,
--     a.price,
--     a.currency,
--     a.listing_created_on,
--     a.is_active,
--     a.last_modified_timestamp,
--     aa.category,
--     aa.amenities,
--     aa.bathrooms,
--     aa.bedrooms,
--     aa.fee,
--     aa.has_photo,
--     aa.pets_allowed,
--     aa.square_feet,
--     aa.address,
--     aa.cityname,
--     aa.state,
--     aa.latitude,
--     aa.longitude
-- FROM stg_apartments a
-- INNER JOIN stg_apartment_attributes aa ON a.id = aa.id
-- WHERE a.price IS NOT NULL
--     AND aa.cityname IS NOT NULL

SELECT 
    stg_apartments.id AS listing_id,
    stg_apartments.title,
    stg_apartments.source,
    stg_apartments.price,
    stg_apartments.currency,
    stg_apartments.listing_created_on,
    stg_apartments.is_active,
    stg_apartments.last_modified_timestamp,
    stg_apartment_attributes.category,
    stg_apartment_attributes.amenities,
    stg_apartment_attributes.bathrooms,
    stg_apartment_attributes.bedrooms,
    stg_apartment_attributes.fee,
    stg_apartment_attributes.has_photo,
    stg_apartment_attributes.pets_allowed,
    stg_apartment_attributes.square_feet,
    stg_apartment_attributes.address,
    stg_apartment_attributes.cityname,
    stg_apartment_attributes.state,
    stg_apartment_attributes.latitude,
    stg_apartment_attributes.longitude
FROM stg_apartments
INNER JOIN stg_apartment_attributes 
ON stg_apartments.id = stg_apartment_attributes.id
WHERE (stg_apartments.price IS NOT NULL) 
AND (stg_apartment_attributes.cityname IS NOT NULL);
'''
transform_dim_listings_node1750093564971 = sparkSqlQuery(glueContext, query = SqlQuery7313, mapping = {"stg_apartments":stg_apartments_node1750010304997, "stg_apartment_attributes":stg_apartment_attributes_node1750010306325}, transformation_ctx = "transform_dim_listings_node1750093564971")

# Script generated for node transform_fact_user_views
SqlQuery7310 = '''
SELECT 
    user_id,
    apartment_id AS listing_id,
    CAST(viewed_at AS date),
    is_wishlisted,
    call_to_action
FROM stg_user_viewing
WHERE viewed_at IS NOT NULL
'''
transform_fact_user_views_node1750100763860 = sparkSqlQuery(glueContext, query = SqlQuery7310, mapping = {"stg_user_viewing":stg_user_viewing_node1750010305887}, transformation_ctx = "transform_fact_user_views_node1750100763860")

# Script generated for node transform_dim_users
SqlQuery7312 = '''
SELECT 
    user_id,
    COUNT(*) AS total_views,
    SUM(CASE WHEN is_wishlisted THEN 1 ELSE 0 END) AS total_wishlists,
    CAST(MAX(viewed_at) AS date) AS last_viewed_date
FROM stg_user_viewing
GROUP BY user_id;
'''
transform_dim_users_node1750098044544 = sparkSqlQuery(glueContext, query = SqlQuery7312, mapping = {"stg_user_viewing":stg_user_viewing_node1750010305887}, transformation_ctx = "transform_dim_users_node1750098044544")

# Script generated for node write_fact_bookings
write_fact_bookings_node1750127742454 = glueContext.write_dynamic_frame.from_options(frame=transform_fact_bookings_node1750099266941, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO curated.fact_bookings USING curated.fact_bookings_temp_1sizga ON fact_bookings.booking_id = fact_bookings_temp_1sizga.booking_id WHEN MATCHED THEN UPDATE SET booking_id = fact_bookings_temp_1sizga.booking_id, user_id = fact_bookings_temp_1sizga.user_id, listing_id = fact_bookings_temp_1sizga.listing_id, booking_date = fact_bookings_temp_1sizga.booking_date, checkin_date = fact_bookings_temp_1sizga.checkin_date, checkout_date = fact_bookings_temp_1sizga.checkout_date, total_price = fact_bookings_temp_1sizga.total_price, currency = fact_bookings_temp_1sizga.currency, booking_status = fact_bookings_temp_1sizga.booking_status WHEN NOT MATCHED THEN INSERT VALUES (fact_bookings_temp_1sizga.booking_id, fact_bookings_temp_1sizga.user_id, fact_bookings_temp_1sizga.listing_id, fact_bookings_temp_1sizga.booking_date, fact_bookings_temp_1sizga.checkin_date, fact_bookings_temp_1sizga.checkout_date, fact_bookings_temp_1sizga.total_price, fact_bookings_temp_1sizga.currency, fact_bookings_temp_1sizga.booking_status); DROP TABLE curated.fact_bookings_temp_1sizga; END;", "redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.fact_bookings_temp_1sizga", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.fact_bookings (booking_id INTEGER, user_id INTEGER, listing_id INTEGER, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR); DROP TABLE IF EXISTS curated.fact_bookings_temp_1sizga; CREATE TABLE curated.fact_bookings_temp_1sizga (booking_id INTEGER, user_id INTEGER, listing_id INTEGER, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="write_fact_bookings_node1750127742454")

# Script generated for node write_dim_listings
write_dim_listings_node1750127760385 = glueContext.write_dynamic_frame.from_options(frame=transform_dim_listings_node1750093564971, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO curated.dim_listings USING curated.dim_listings_temp_difh1f ON dim_listings.listing_id = dim_listings_temp_difh1f.listing_id WHEN MATCHED THEN UPDATE SET listing_id = dim_listings_temp_difh1f.listing_id, title = dim_listings_temp_difh1f.title, source = dim_listings_temp_difh1f.source, price = dim_listings_temp_difh1f.price, currency = dim_listings_temp_difh1f.currency, listing_created_on = dim_listings_temp_difh1f.listing_created_on, is_active = dim_listings_temp_difh1f.is_active, last_modified_timestamp = dim_listings_temp_difh1f.last_modified_timestamp, category = dim_listings_temp_difh1f.category, amenities = dim_listings_temp_difh1f.amenities, bathrooms = dim_listings_temp_difh1f.bathrooms, bedrooms = dim_listings_temp_difh1f.bedrooms, fee = dim_listings_temp_difh1f.fee, has_photo = dim_listings_temp_difh1f.has_photo, pets_allowed = dim_listings_temp_difh1f.pets_allowed, square_feet = dim_listings_temp_difh1f.square_feet, address = dim_listings_temp_difh1f.address, cityname = dim_listings_temp_difh1f.cityname, state = dim_listings_temp_difh1f.state, latitude = dim_listings_temp_difh1f.latitude, longitude = dim_listings_temp_difh1f.longitude WHEN NOT MATCHED THEN INSERT VALUES (dim_listings_temp_difh1f.listing_id, dim_listings_temp_difh1f.title, dim_listings_temp_difh1f.source, dim_listings_temp_difh1f.price, dim_listings_temp_difh1f.currency, dim_listings_temp_difh1f.listing_created_on, dim_listings_temp_difh1f.is_active, dim_listings_temp_difh1f.last_modified_timestamp, dim_listings_temp_difh1f.category, dim_listings_temp_difh1f.amenities, dim_listings_temp_difh1f.bathrooms, dim_listings_temp_difh1f.bedrooms, dim_listings_temp_difh1f.fee, dim_listings_temp_difh1f.has_photo, dim_listings_temp_difh1f.pets_allowed, dim_listings_temp_difh1f.square_feet, dim_listings_temp_difh1f.address, dim_listings_temp_difh1f.cityname, dim_listings_temp_difh1f.state, dim_listings_temp_difh1f.latitude, dim_listings_temp_difh1f.longitude); DROP TABLE curated.dim_listings_temp_difh1f; END;", "redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.dim_listings_temp_difh1f", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.dim_listings (listing_id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp TIMESTAMP, category VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL); DROP TABLE IF EXISTS curated.dim_listings_temp_difh1f; CREATE TABLE curated.dim_listings_temp_difh1f (listing_id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp TIMESTAMP, category VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="write_dim_listings_node1750127760385")

# Script generated for node write_fact_user_views
write_fact_user_views_node1750127752892 = glueContext.write_dynamic_frame.from_options(frame=transform_fact_user_views_node1750100763860, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO curated.fact_user_views USING curated.fact_user_views_temp_7pwa7f ON fact_user_views.user_id = fact_user_views_temp_7pwa7f.user_id AND fact_user_views.viewed_at = fact_user_views_temp_7pwa7f.viewed_at AND fact_user_views.listing_id = fact_user_views_temp_7pwa7f.listing_id WHEN MATCHED THEN UPDATE SET user_id = fact_user_views_temp_7pwa7f.user_id, listing_id = fact_user_views_temp_7pwa7f.listing_id, viewed_at = fact_user_views_temp_7pwa7f.viewed_at, is_wishlisted = fact_user_views_temp_7pwa7f.is_wishlisted, call_to_action = fact_user_views_temp_7pwa7f.call_to_action WHEN NOT MATCHED THEN INSERT VALUES (fact_user_views_temp_7pwa7f.user_id, fact_user_views_temp_7pwa7f.listing_id, fact_user_views_temp_7pwa7f.viewed_at, fact_user_views_temp_7pwa7f.is_wishlisted, fact_user_views_temp_7pwa7f.call_to_action); DROP TABLE curated.fact_user_views_temp_7pwa7f; END;", "redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.fact_user_views_temp_7pwa7f", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.fact_user_views (user_id INTEGER, listing_id INTEGER, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR); DROP TABLE IF EXISTS curated.fact_user_views_temp_7pwa7f; CREATE TABLE curated.fact_user_views_temp_7pwa7f (user_id INTEGER, listing_id INTEGER, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="write_fact_user_views_node1750127752892")

# Script generated for node write_dim_users
write_dim_users_node1750127725676 = glueContext.write_dynamic_frame.from_options(frame=transform_dim_users_node1750098044544, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO curated.dim_users USING curated.dim_users_temp_4hbrdq ON dim_users.user_id = dim_users_temp_4hbrdq.user_id WHEN MATCHED THEN UPDATE SET user_id = dim_users_temp_4hbrdq.user_id, total_views = dim_users_temp_4hbrdq.total_views, total_wishlists = dim_users_temp_4hbrdq.total_wishlists, last_viewed_date = dim_users_temp_4hbrdq.last_viewed_date WHEN NOT MATCHED THEN INSERT VALUES (dim_users_temp_4hbrdq.user_id, dim_users_temp_4hbrdq.total_views, dim_users_temp_4hbrdq.total_wishlists, dim_users_temp_4hbrdq.last_viewed_date); DROP TABLE curated.dim_users_temp_4hbrdq; END;", "redshiftTmpDir": "s3://aws-glue-assets-198170203035-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.dim_users_temp_4hbrdq", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.dim_users (user_id INTEGER, total_views BIGINT, total_wishlists BIGINT, last_viewed_date DATE); DROP TABLE IF EXISTS curated.dim_users_temp_4hbrdq; CREATE TABLE curated.dim_users_temp_4hbrdq (user_id INTEGER, total_views BIGINT, total_wishlists BIGINT, last_viewed_date DATE);"}, transformation_ctx="write_dim_users_node1750127725676")

job.commit()