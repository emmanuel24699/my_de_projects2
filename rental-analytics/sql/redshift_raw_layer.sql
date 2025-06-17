-- Staging table for apartments
CREATE TABLE raw_data.stg_apartments (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(100),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP
)
DISTKEY(id)
SORTKEY(last_modified_timestamp);

-- Staging table for bookings
CREATE TABLE raw_data.stg_bookings (
    booking_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    apartment_id INTEGER,
    booking_date DATE,
    checkin_date DATE,
    checkout_date DATE,
    total_price DECIMAL(10,2),
    currency VARCHAR(10),
    booking_status VARCHAR(50)
)
DISTKEY(apartment_id)
SORTKEY(booking_date);

-- Staging table for user_viewing
CREATE TABLE raw_data.stg_user_viewing (
    user_id INTEGER,
    apartment_id INTEGER,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    PRIMARY KEY (user_id, apartment_id, viewed_at)
)
DISTKEY(apartment_id)
SORTKEY(viewed_at);

-- Staging table for apartment_attributes
CREATE TABLE raw_data.stg_apartment_attributes (
    id INTEGER PRIMARY KEY,
    category VARCHAR(50),
    body VARCHAR(MAX),
    amenities VARCHAR(MAX),
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(50),
    price_type VARCHAR(20),
    square_feet INTEGER,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
)
DISTKEY(id)
SORTKEY(id);