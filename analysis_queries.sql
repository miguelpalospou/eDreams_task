-- 1. Top 10 itineraries by number of bookings
WITH itinerary_bookings AS (
    SELECT 
        carrier,
        flight_number,
        DATE(departure_timestamp) as departure_date,
        departure_geo_node_id,
        arrival_geo_node_id,
        COUNT(*) as booking_count
    FROM test_search.booking_sections
    GROUP BY 
        carrier,
        flight_number,
        DATE(departure_timestamp),
        departure_geo_node_id,
        arrival_geo_node_id
)
SELECT 
    carrier,
    flight_number,
    departure_date,
    departure_geo_node_id,
    arrival_geo_node_id,
    booking_count
FROM itinerary_bookings
ORDER BY booking_count DESC
LIMIT 10;



-- Query 2: Count bookings made at minimum price
WITH min_price AS (
    SELECT MIN(booking_price) as min_price 
    FROM test_search.provider_booking
)
SELECT COUNT(*) as bookings_at_min_price 
FROM test_search.provider_booking pb, min_price mp 
WHERE pb.booking_price = mp.min_price;




-- 3. Count itineraries booked with only 1 search
-- Step 1: Get all unique booked itineraries (by carrier, flight number, date, origin, destination)
WITH booked_itineraries AS (
    SELECT DISTINCT
        bs.carrier, -- integer
        bs.flight_number, -- numeric
        DATE(bs.departure_timestamp) AS departure_date, -- date
        bs.departure_geo_node_id, -- numeric
        bs.arrival_geo_node_id -- numeric
    FROM test_search.booking_sections bs
),
-- Step 2: Unnest search results to extract all itineraries ever shown in a search
search_itineraries AS (
    SELECT
        ss.search_id, 
        s.marketingcarriercode AS carrier, 
        s.flightnumber AS flight_number, -- varchar
        DATE(s.departuredatetime::timestamp) AS departure_date, -- date
        s.departuregeonodeid AS departure_geo_node_id, -- integer
        s.arrivalgeonodeid AS arrival_geo_node_id -- integer
    FROM test_search.search_succeeded ss
    , LATERAL unnest(results) r
    , LATERAL unnest(r.segmentsoptions) so
    , LATERAL unnest(so.options) o
    , LATERAL unnest(o.sections) s
),
-- Step 3: For each booked itinerary, count how many distinct searches produced it
itinerary_search_counts AS (
    SELECT
        bi.carrier,
        bi.flight_number,
        bi.departure_date,
        bi.departure_geo_node_id,
        bi.arrival_geo_node_id,
        COUNT(DISTINCT si.search_id) AS search_count
    FROM booked_itineraries bi
    JOIN search_itineraries si
      ON bi.carrier = si.carrier
     AND bi.flight_number::varchar = si.flight_number
     AND bi.departure_date = si.departure_date
     AND bi.departure_geo_node_id::integer = si.departure_geo_node_id
     AND bi.arrival_geo_node_id::integer = si.arrival_geo_node_id
    GROUP BY
        bi.carrier,
        bi.flight_number,
        bi.departure_date,
        bi.departure_geo_node_id,
        bi.arrival_geo_node_id
)
-- Step 4: Count how many booked itineraries were found in exactly one search
SELECT COUNT(*) AS itineraries_with_one_search
FROM itinerary_search_counts
WHERE search_count = 1;






-- 4. Count itineraries searched but never booked
WITH searched_itineraries AS (
    SELECT DISTINCT
        s.marketingcarriercode AS carrier,
        s.flightnumber AS flight_number,
        DATE(s.departuredatetime::timestamp) AS departure_date,
        s.departuregeonodeid AS departure_geo_node_id,
        s.arrivalgeonodeid AS arrival_geo_node_id
    FROM test_search.search_succeeded ss
    , LATERAL unnest(results) r
    , LATERAL unnest(r.segmentsoptions) so
    , LATERAL unnest(so.options) o
    , LATERAL unnest(o.sections) s
),
booked_itineraries AS (
    SELECT DISTINCT
        carrier,
        flight_number,
        DATE(departure_timestamp) AS departure_date,
        departure_geo_node_id,
        arrival_geo_node_id
    FROM test_search.booking_sections
)
SELECT COUNT(*) AS searched_but_not_booked
FROM searched_itineraries si
LEFT JOIN booked_itineraries bi 
    ON si.carrier = bi.carrier
    AND si.flight_number = bi.flight_number
    AND si.departure_date = bi.departure_date
    AND si.departure_geo_node_id = bi.departure_geo_node_id
    AND si.arrival_geo_node_id = bi.arrival_geo_node_id
WHERE bi.carrier IS NULL;
