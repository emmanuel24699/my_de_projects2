-- transform_dim_listings
SELECT 
    a.id AS listing_id,
    a.title,
    a.source,
    a.price,
    a.currency,
    a.listing_created_on,
    a.is_active,
    a.last_modified_timestamp,
    aa.category,
    aa.amenities,
    aa.bathrooms,
    aa.bedrooms,
    aa.fee,
    aa.has_photo,
    aa.pets_allowed,
    aa.square_feet,
    aa.address,
    aa.cityname,
    aa.state,
    aa.latitude,
    aa.longitude
FROM stg_apartments a
INNER JOIN stg_apartment_attributes aa ON a.id = aa.id
WHERE a.price IS NOT NULL
    AND aa.cityname IS NOT NULL


-- transform_dim_users
SELECT 
    user_id,
    COUNT(*) AS total_views,
    SUM(CASE WHEN is_wishlisted THEN 1 ELSE 0 END) AS total_wishlists,
    CAST(MAX(viewed_at) AS date) AS last_viewed_date
FROM stg_user_viewing
GROUP BY user_id;

-- transform_fact_bookings
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

-- transform_fact_user
SELECT 
    user_id,
    apartment_id AS listing_id,
    CAST(viewed_at AS date),
    is_wishlisted,
    call_to_action
FROM stg_user_viewing
WHERE viewed_at IS NOT NULL
