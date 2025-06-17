CREATE TABLE apartments (
    id INT PRIMARY KEY NOT NULL,
    title VARCHAR(255),
    source VARCHAR(100),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp DATETIME
)
ENGINE=InnoDB;

CREATE TABLE bookings (
    booking_id INT PRIMARY KEY NOT NULL,
    user_id INT NOT NULL,
    apartment_id INT NOT NULL,
    booking_date DATE,
    checkin_date DATE,
    checkout_date DATE,
    total_price DECIMAL(10,2),
    currency VARCHAR(10),
    booking_status VARCHAR(50),
    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
) ENGINE=InnoDB;

CREATE TABLE user_viewing (
    user_id INT NOT NULL,
    apartment_id INT NOT NULL,
    viewed_at DATETIME NOT NULL,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    PRIMARY KEY (user_id, apartment_id, viewed_at),
    FOREIGN KEY (apartment_id) REFERENCES apartments(id)
) ENGINE=InnoDB;

CREATE TABLE apartment_attributes (
    id INT PRIMARY KEY NOT NULL,
    category VARCHAR(50),
    body TEXT,
    amenities TEXT,
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10,2),
    has_photo TINYINT(1),
    pets_allowed TINYINT(1),
    price_display VARCHAR(50),
    price_type VARCHAR(20),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
) ENGINE=InnoDB;
