-- This script creates views in the presentation schema for various weekly and monthly metrics related to listings, bookings, and user interactions.

-- Create presentation schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS presentation;
GRANT ALL ON SCHEMA presentation TO <username>;

-- Average Listing price per week
CREATE VIEW presentation.vw_avg_listing_price_weekly AS
SELECT 
    DATE_TRUNC('week', listing_created_on) AS week_start,
    AVG(price) AS avg_price,
    currency
FROM curated.dim_listings
WHERE price IS NOT NULL
GROUP BY DATE_TRUNC('week', listing_created_on), currency
ORDER BY week_start;-- Average Listing price per week
CREATE VIEW presentation.vw_avg_listing_price_weekly AS
SELECT 
    DATE_TRUNC('week', listing_created_on) AS week_start,
    AVG(price) AS avg_price,
    currency
FROM curated.dim_listings
WHERE price IS NOT NULL
GROUP BY DATE_TRUNC('week', listing_created_on), currency
ORDER BY week_start;

-- Most popular locations weekly
CREATE VIEW presentation.vw_popular_locations_weekly AS
SELECT 
    l.cityname,
    DATE_TRUNC('week', v.viewed_at) AS week_start,
    COUNT(*) AS view_count
FROM curated.fact_user_views v
JOIN curated.dim_listings l ON v.listing_id = l.listing_id
GROUP BY l.cityname, DATE_TRUNC('week', v.viewed_at)
ORDER BY week_start, view_count DESC;

-- Top performing listings (weekly)
CREATE VIEW presentation.vw_top_listings_weekly AS
SELECT 
    b.listing_id,
    l.title,
    DATE_TRUNC('week', b.booking_date) AS week_start,
    COUNT(*) AS booking_count,
    SUM(total_price) AS total_revenue
FROM curated.fact_bookings b
JOIN curated.dim_listings l ON b.listing_id = l.listing_id
GROUP BY b.listing_id, l.title, DATE_TRUNC('week', b.booking_date)
ORDER BY week_start, total_revenue DESC
LIMIT 10;

-- Total Bookings per User (Weekly)
CREATE VIEW presentation.vw_bookings_per_user_weekly AS
SELECT 
    user_id,
    DATE_TRUNC('week', booking_date) AS week_start,
    COUNT(*) AS booking_count
FROM curated.fact_bookings
GROUP BY user_id, DATE_TRUNC('week', booking_date)
ORDER BY week_start, booking_count DESC;

-- Average booking Duration
CREATE VIEW presentation.vw_avg_booking_duration AS
SELECT 
    listing_id,
    AVG(DATEDIFF(day, checkin_date, checkout_date)) AS avg_duration_days
FROM curated.fact_bookings
GROUP BY listing_id
ORDER BY avg_duration_days DESC;

-- Repeat Customer Rate (30-day Rolling)
CREATE VIEW presentation.vw_repeat_customer_rate AS
WITH user_bookings AS (
    SELECT 
        user_id,
        booking_date,
        LAG(booking_date) OVER (PARTITION BY user_id ORDER BY booking_date) AS prev_booking_date
    FROM curated.fact_bookings
)
SELECT 
    DATE_TRUNC('day', booking_date) AS report_date,
    COUNT(DISTINCT CASE 
        WHEN prev_booking_date IS NOT NULL 
        AND booking_date - prev_booking_date <= 30 
        THEN user_id 
        END) / NULLIF(COUNT(DISTINCT user_id), 0) AS repeat_customer_rate
FROM user_bookings
GROUP BY DATE_TRUNC('day', booking_date)
ORDER BY report_date;


-- Occupancy rate (Monthly)
CREATE OR REPLACE VIEW presentation.vw_occupancy_rate_monthly AS
WITH RECURSIVE month_sequence (month_start) AS (
    SELECT 
        DATE_TRUNC('month', MIN(listing_created_on)) AS month_start
    FROM curated.dim_listings
    WHERE listing_created_on IS NOT NULL
    UNION ALL
    SELECT 
        DATEADD(month, 1, month_start) AS month_start
    FROM month_sequence
    WHERE month_start < DATE_TRUNC('month', CURRENT_DATE)
),
month_series AS (
    SELECT 
        l.listing_id,
        m.month_start
    FROM curated.dim_listings l
    CROSS JOIN month_sequence m
    WHERE m.month_start >= DATE_TRUNC('month', l.listing_created_on)
        AND m.month_start <= DATE_TRUNC('month', CURRENT_DATE)
),
booked_days AS (
    SELECT 
        listing_id,
        DATE_TRUNC('month', checkin_date) AS month_start,
        SUM(DATEDIFF(day, checkin_date, checkout_date)) AS booked_days
    FROM curated.fact_bookings
    WHERE checkin_date IS NOT NULL
        AND checkout_date IS NOT NULL
    GROUP BY listing_id, DATE_TRUNC('month', checkin_date)
),
total_days AS (
    SELECT 
        listing_id,
        DATE_TRUNC('month', listing_created_on) AS month_start,
        DATEDIFF(day, 
                 DATE_TRUNC('month', listing_created_on), 
                 DATEADD(month, 1, DATE_TRUNC('month', listing_created_on))
        ) AS total_days
    FROM curated.dim_listings
    WHERE listing_created_on IS NOT NULL
    GROUP BY listing_id, DATE_TRUNC('month', listing_created_on)
),
occupancy_per_listing AS (
    SELECT 
        m.listing_id,
        m.month_start,
        EXTRACT(YEAR FROM m.month_start) AS year,
        EXTRACT(MONTH FROM m.month_start) AS month,
        COALESCE(b.booked_days, 0)::FLOAT / NULLIF(t.total_days, 0) AS occupancy_rate
    FROM month_series m
    LEFT JOIN booked_days b 
        ON m.listing_id = b.listing_id 
        AND m.month_start = b.month_start
    LEFT JOIN total_days t 
        ON m.listing_id = t.listing_id 
        AND m.month_start = t.month_start
    WHERE t.total_days IS NOT NULL
)
SELECT 
    year,
    month,
    ROUND(AVG(occupancy_rate), 2) AS occupancy_rate
FROM occupancy_per_listing
GROUP BY year, month
ORDER BY year, month;

-- Most popular locations weekly
CREATE VIEW presentation.vw_popular_locations_weekly AS
SELECT 
    l.cityname,
    DATE_TRUNC('week', v.viewed_at) AS week_start,
    COUNT(*) AS view_count
FROM curated.fact_user_views v
JOIN curated.dim_listings l ON v.listing_id = l.listing_id
GROUP BY l.cityname, DATE_TRUNC('week', v.viewed_at)
ORDER BY week_start, view_count DESC;

-- Top performing listings (weekly)
CREATE VIEW presentation.vw_top_listings_weekly AS
SELECT 
    b.listing_id,
    l.title,
    DATE_TRUNC('week', b.booking_date) AS week_start,
    COUNT(*) AS booking_count,
    SUM(total_price) AS total_revenue
FROM curated.fact_bookings b
JOIN curated.dim_listings l ON b.listing_id = l.listing_id
GROUP BY b.listing_id, l.title, DATE_TRUNC('week', b.booking_date)
ORDER BY week_start, total_revenue DESC
LIMIT 10;

-- Total Bookings per User (Weekly)
CREATE VIEW presentation.vw_bookings_per_user_weekly AS
SELECT 
    user_id,
    DATE_TRUNC('week', booking_date) AS week_start,
    COUNT(*) AS booking_count
FROM curated.fact_bookings
GROUP BY user_id, DATE_TRUNC('week', booking_date)
ORDER BY week_start, booking_count DESC;

-- Average booking Duration
CREATE VIEW presentation.vw_avg_booking_duration AS
SELECT 
    listing_id,
    AVG(DATEDIFF(day, checkin_date, checkout_date)) AS avg_duration_days
FROM curated.fact_bookings
GROUP BY listing_id
ORDER BY avg_duration_days DESC;

-- Repeat Customer Rate (30-day Rolling)
CREATE VIEW presentation.vw_repeat_customer_rate AS
WITH user_bookings AS (
    SELECT 
        user_id,
        booking_date,
        LAG(booking_date) OVER (PARTITION BY user_id ORDER BY booking_date) AS prev_booking_date
    FROM curated.fact_bookings
)
SELECT 
    DATE_TRUNC('day', booking_date) AS report_date,
    COUNT(DISTINCT CASE 
        WHEN prev_booking_date IS NOT NULL 
        AND booking_date - prev_booking_date <= 30 
        THEN user_id 
        END) / NULLIF(COUNT(DISTINCT user_id), 0) AS repeat_customer_rate
FROM user_bookings
GROUP BY DATE_TRUNC('day', booking_date)
ORDER BY report_date;


-- Occupancy rate (Monthly)
CREATE OR REPLACE VIEW presentation.vw_occupancy_rate_monthly AS
WITH RECURSIVE month_sequence (month_start) AS (
    SELECT 
        DATE_TRUNC('month', MIN(listing_created_on)) AS month_start
    FROM curated.dim_listings
    WHERE listing_created_on IS NOT NULL
    UNION ALL
    SELECT 
        DATEADD(month, 1, month_start) AS month_start
    FROM month_sequence
    WHERE month_start < DATE_TRUNC('month', CURRENT_DATE)
),
month_series AS (
    SELECT 
        l.listing_id,
        m.month_start
    FROM curated.dim_listings l
    CROSS JOIN month_sequence m
    WHERE m.month_start >= DATE_TRUNC('month', l.listing_created_on)
        AND m.month_start <= DATE_TRUNC('month', CURRENT_DATE)
),
booked_days AS (
    SELECT 
        listing_id,
        DATE_TRUNC('month', checkin_date) AS month_start,
        SUM(DATEDIFF(day, checkin_date, checkout_date)) AS booked_days
    FROM curated.fact_bookings
    WHERE checkin_date IS NOT NULL
        AND checkout_date IS NOT NULL
    GROUP BY listing_id, DATE_TRUNC('month', checkin_date)
),
total_days AS (
    SELECT 
        listing_id,
        DATE_TRUNC('month', listing_created_on) AS month_start,
        DATEDIFF(day, 
                 DATE_TRUNC('month', listing_created_on), 
                 DATEADD(month, 1, DATE_TRUNC('month', listing_created_on))
        ) AS total_days
    FROM curated.dim_listings
    WHERE listing_created_on IS NOT NULL
    GROUP BY listing_id, DATE_TRUNC('month', listing_created_on)
),
occupancy_per_listing AS (
    SELECT 
        m.listing_id,
        m.month_start,
        EXTRACT(YEAR FROM m.month_start) AS year,
        EXTRACT(MONTH FROM m.month_start) AS month,
        COALESCE(b.booked_days, 0)::FLOAT / NULLIF(t.total_days, 0) AS occupancy_rate
    FROM month_series m
    LEFT JOIN booked_days b 
        ON m.listing_id = b.listing_id 
        AND m.month_start = b.month_start
    LEFT JOIN total_days t 
        ON m.listing_id = t.listing_id 
        AND m.month_start = t.month_start
    WHERE t.total_days IS NOT NULL
)
SELECT 
    year,
    month,
    ROUND(AVG(occupancy_rate), 2) AS occupancy_rate
FROM occupancy_per_listing
GROUP BY year, month
ORDER BY year, month;